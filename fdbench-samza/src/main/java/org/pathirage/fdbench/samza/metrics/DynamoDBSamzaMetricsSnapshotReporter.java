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

package org.pathirage.fdbench.samza.metrics;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.google.gson.Gson;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.samza.metrics.*;
import org.pathirage.fdbench.utils.DynamoDBUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class DynamoDBSamzaMetricsSnapshotReporter implements MetricsReporter, Runnable {
  private static final Logger log = LoggerFactory.getLogger(DynamoDBSamzaMetricsSnapshotReporter.class);

  private final String name;
  private final String containerName;
  private final String jobName;
  private final String jobId;
  private final String version;
  private final String awsKeyId;
  private final String awsKeySecret;
  private final Table table;
  private final int pollingInterval;

  private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1, new ThreadFactory() {
    @Override
    public Thread newThread(Runnable r) {
      Thread thread = new Thread(r);
      thread.setDaemon(true);
      thread.setName("FDBenchMessaging-DynamoDBSamzaMetricsSnapshotReporter");
      return thread;
    }
  });

  final List<Pair<String, ReadableMetricsRegistry>> registries = new ArrayList<>();


  public DynamoDBSamzaMetricsSnapshotReporter(String name, String containerName, String jobName, String jobId,
                                              String version, String tableName, String awsKeyId, String awsKeySecret,
                                              int pollingInterval) {
    this.name = name;
    this.containerName = containerName;
    this.jobName = jobName;
    this.jobId = jobId;
    this.version = version;
    this.awsKeyId = awsKeyId;
    this.awsKeySecret = awsKeySecret;
    this.pollingInterval = pollingInterval;
    AmazonDynamoDBClient dynamoDBClient = new AmazonDynamoDBClient(new BasicAWSCredentials(awsKeyId, awsKeySecret));
    dynamoDBClient.withRegion(Regions.US_WEST_2);

    ArrayList<AttributeDefinition> attributeDefinitions = new ArrayList<AttributeDefinition>();
    attributeDefinitions.add(new AttributeDefinition().withAttributeName("Id").withAttributeType(ScalarAttributeType.N));
    attributeDefinitions.add(new AttributeDefinition().withAttributeName("JobName").withAttributeType(ScalarAttributeType.S));

    ArrayList<KeySchemaElement> keySchemas = new ArrayList<KeySchemaElement>();
    keySchemas.add(new KeySchemaElement().withAttributeName("JobName").withKeyType(KeyType.HASH));
    keySchemas.add(new KeySchemaElement().withAttributeName("Id").withKeyType(KeyType.RANGE));

    this.table = DynamoDBUtils.createTable(new DynamoDB(dynamoDBClient), tableName, attributeDefinitions, keySchemas, 5L, 5L);
  }

  @Override
  public void start() {
    log.info("Starting " + getClass().getName() + " instance with name " + name);
    executor.scheduleWithFixedDelay(this, 0, pollingInterval, TimeUnit.SECONDS);
  }

  @Override
  public void register(String source, ReadableMetricsRegistry registry) {
    registries.add(Pair.of(source, registry));
  }

  @Override
  public void stop() {
    log.info("Stopping reporter timer");
    executor.shutdown();
    try {
      executor.awaitTermination(60, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      log.error("Waiting for timer to stop interrupted.", e);
    }

    if(!executor.isTerminated()) {
      log.warn("Unable to shutdown report timer.");
    }
  }

  @Override
  public void run() {
    if(log.isDebugEnabled()) {
      log.debug("Begin flushing metrics.");
    }

    for(Pair<String, ReadableMetricsRegistry> registryPair : registries) {
      if(log.isDebugEnabled()){
        log.debug("Flushing metrics for " + registryPair.getKey());
      }

      Map<String, Map<String, Object>> metricsMsg = new HashMap<>();

      for(String group : registryPair.getValue().getGroups()) {
        Map<String, Object> groupMsg = new HashMap<>();
        for(Map.Entry<String, Metric> metricEntry : registryPair.getValue().getGroup(group).entrySet()) {
          String name = metricEntry.getKey();

          metricEntry.getValue().visit(new MetricsVisitor() {
            @Override
            public void counter(Counter counter) {
              groupMsg.put(name, counter.getCount());
            }

            @Override
            public <T> void gauge(Gauge<T> gauge) {
              groupMsg.put(name, gauge.getValue());
            }

            @Override
            public void timer(Timer timer) {
              groupMsg.put(name, timer.getSnapshot().getAverage());
            }
          });
        }

        metricsMsg.put(group, groupMsg);
      }

      long recordingTime  = System.currentTimeMillis();
      Item metricSnapshot = new Item()
          .withPrimaryKey("JobName", jobName, "Id", recordingTime)
          .withString("ContainerName", containerName)
          .withString("JobId", jobId)
          .withString("Version", version)
          .withJSON("Snapshot", new Gson().toJson(metricsMsg));

      if (log.isDebugEnabled()) {
        log.debug("Putting an item for registry " + registryPair.getKey() + " with id " + recordingTime);
      }

      PutItemOutcome outcome = table.putItem(metricSnapshot);
    }
  }
}
