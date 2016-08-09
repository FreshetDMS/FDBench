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
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.*;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsReporter;
import org.apache.samza.metrics.ReadableMetricsRegistry;
import org.pathirage.fdbench.FDBenchException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class DynamoDBSamzaMetricsSnapshotReporter implements MetricsReporter {
  private static final Logger log = LoggerFactory.getLogger(DynamoDBSamzaMetricsSnapshotReporter.class);

  private final String name;
  private final String containerName;
  private final String jobName;
  private final String jobId;
  private final String version;
  private final String awsKeyId;
  private final String awsKeySecret;
  private final Table table;

  private final ExecutorService executor = Executors.newScheduledThreadPool(1, new ThreadFactory() {
    @Override
    public Thread newThread(Runnable r) {
      Thread thread = new Thread(r);
      thread.setDaemon(true);
      thread.setName("FDBenchMessaging-DynamoDBSamzaMetricsSnapshotReporter");
      return thread;
    }
  });

  public DynamoDBSamzaMetricsSnapshotReporter(String name, String containerName, String jobName, String jobId,
                                              String version, String tableName, String awsKeyId, String awsKeySecret) {
    this.name = name;
    this.containerName = containerName;
    this.jobName = jobName;
    this.jobId = jobId;
    this.version = version;
    this.awsKeyId = awsKeyId;
    this.awsKeySecret = awsKeySecret;
    AmazonDynamoDBClient dynamoDBClient = new AmazonDynamoDBClient(new BasicAWSCredentials(awsKeyId, awsKeySecret));
    dynamoDBClient.withRegion(Regions.US_WEST_2);
    this.table = createDynamoDBTable(new DynamoDB(dynamoDBClient), tableName);
  }

  private Table createDynamoDBTable(DynamoDB dynamoDB, String tableName) {
    Table table;
    try {
      log.info("Checking the availability of table " + tableName);
      table = dynamoDB.getTable(tableName);
      table.describe();

      return table;
    } catch (ResourceNotFoundException e) {
      log.info("No table with name " + tableName + " exists. So creating a new table.");
      ArrayList<AttributeDefinition> attributeDefinitions = new ArrayList<AttributeDefinition>();
      attributeDefinitions.add(new AttributeDefinition().withAttributeName("Id").withAttributeType(ScalarAttributeType.N));
      attributeDefinitions.add(new AttributeDefinition().withAttributeName("BenchName").withAttributeType(ScalarAttributeType.S));

      ArrayList<KeySchemaElement> keySchema = new ArrayList<KeySchemaElement>();
      keySchema.add(new KeySchemaElement().withAttributeName("BenchName").withKeyType(KeyType.HASH));
      keySchema.add(new KeySchemaElement().withAttributeName("Id").withKeyType(KeyType.RANGE));

      CreateTableRequest request = new CreateTableRequest()
          .withTableName(tableName)
          .withKeySchema(keySchema)
          .withAttributeDefinitions(attributeDefinitions)
          .withProvisionedThroughput(new ProvisionedThroughput()
              .withReadCapacityUnits(5L)
              .withWriteCapacityUnits(5L));

      table = dynamoDB.createTable(request);

      try {
        table.waitForActive();
        return table;
      } catch (InterruptedException ie) {
        String errMsg = "Waiting for DynamoDB table to become active interrupted.";
        log.error(errMsg, ie);
        throw new FDBenchException(errMsg, ie);
      }
    }
  }

  @Override
  public void start() {

  }

  @Override
  public void register(String source, ReadableMetricsRegistry registry) {

  }

  @Override
  public void stop() {

  }
}
