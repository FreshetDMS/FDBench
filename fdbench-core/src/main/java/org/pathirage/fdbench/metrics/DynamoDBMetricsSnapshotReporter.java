/**
 * Copyright 2016 Milinda Pathirage
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.pathirage.fdbench.metrics;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.*;
import com.google.gson.Gson;
import org.apache.commons.lang3.tuple.Pair;
import org.pathirage.fdbench.FDBenchException;
import org.pathirage.fdbench.metrics.api.MetricsRegistry;
import org.pathirage.fdbench.metrics.api.MetricsReporter;
import org.pathirage.fdbench.utils.DynamoDBUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class DynamoDBMetricsSnapshotReporter extends AbstractMetricsSnapshotReporter implements MetricsReporter, Runnable {
  private static final Logger log = LoggerFactory.getLogger(DynamoDBMetricsSnapshotReporter.class);

  private final String tableName;
  private final Table table;


  public DynamoDBMetricsSnapshotReporter(String name, String jobName, String containerName, int interval,
                                         String tableName, String accessKeyId, String accessKeySecret, String awsRegion) {
    super(name, jobName, containerName, interval, Executors.newScheduledThreadPool(1, new ThreadFactory() {
      @Override
      public Thread newThread(Runnable r) {
        Thread thread = new Thread(r);
        thread.setDaemon(true);
        thread.setName("FDBenchMessaging-DynamoDBMetricsSnapshotReporter");
        return thread;
      }
    }));
    AmazonDynamoDBClient dynamoDBClient = new AmazonDynamoDBClient(new BasicAWSCredentials(accessKeyId, accessKeySecret));
    dynamoDBClient.withRegion(Regions.fromName(awsRegion));
    this.tableName = tableName;

    ArrayList<AttributeDefinition> attributeDefinitions = new ArrayList<AttributeDefinition>();
    attributeDefinitions.add(new AttributeDefinition().withAttributeName("Id").withAttributeType(ScalarAttributeType.N));
    attributeDefinitions.add(new AttributeDefinition().withAttributeName("BenchName").withAttributeType(ScalarAttributeType.S));

    ArrayList<KeySchemaElement> keySchemas = new ArrayList<KeySchemaElement>();
    keySchemas.add(new KeySchemaElement().withAttributeName("BenchName").withKeyType(KeyType.HASH));
    keySchemas.add(new KeySchemaElement().withAttributeName("Id").withKeyType(KeyType.RANGE));

    this.table = DynamoDBUtils.createTable(new DynamoDB(dynamoDBClient), tableName, attributeDefinitions, keySchemas, 5L, 5L);
  }

  @Override
  public void run() {
    log.info("Starting to publish metrics.");
    try {
      for (Pair<String, MetricsRegistry> registry : registries) {
        log.info("Flushing metrics for " + registry.getValue());

        Map<String, Map<String, Object>> metricsEvent = metricRegistryToMap(registry.getValue());

        long recordingTime = System.currentTimeMillis();
        Item metricsSnapshot = new Item()
            .withPrimaryKey("BenchName", jobName, "Id", recordingTime)
            .withString("Container", containerName)
            .withLong("Timestamp", recordingTime)
            .withDouble("TotlaMem", getTotalMemory())
            .withDouble("FreeMem", getFreeMemory())
            .withDouble("UsedMem", getUsedMemory())
            .withDouble("MaxMem", getMaxMemory())
            .withLong("JVMCPUTime", getJVMCPUTime())
            .withString("Snapshot", new Gson().toJson(metricsEvent));


        if (log.isDebugEnabled()) {
          log.debug("Putting an item for registry " + registry.getKey() + " with id " + recordingTime);
        }

        PutItemOutcome outcome = table.putItem(metricsSnapshot);

        if (log.isDebugEnabled()) {
          log.debug("Done putting the item for registry " + registry.getKey() + " with the id " + recordingTime);
        }

        if (log.isDebugEnabled()) {
          log.debug("Published metrics snapshot to DynamoDB table " + tableName + " with outcome " + outcome.toString());
        }
      }
    } catch (Exception e) {
      String errMessage = "Error occurred while publishing metrics to DynamoDB table " + tableName;
      log.error(errMessage, e);
      throw new RuntimeException(errMessage, e);
    }
  }
}
