/**
 * Copyright 2017 Milinda Pathirage
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

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.connect.json.JsonDeserializer;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class KafkaMetricsSnapshotConsumerCLI {
  @Parameter(names = {"-b", "--brokers"})
  private String brokers = "localhost:9092";

  @Parameter(names = {"-zk", "--zkconstr"})
  private String zkConnectionString = "localhost:2181";

  @Parameter(names = {"-t", "--topic"}, required = true)
  private String topic;

  @Parameter(names = {"-m", "--metrics"}, required = true)
  private String metrics;

  private KafkaConsumer<byte[], JsonNode> metricsSnapshotConsumer;

  public static void main(String[] args) {
    KafkaMetricsSnapshotConsumerCLI cli = new KafkaMetricsSnapshotConsumerCLI();
    JCommander.newBuilder()
        .addObject(cli)
        .build()
        .parse(args);

    cli.init();
    cli.run();

    System.exit(0);
  }

  public void init() {
    metricsSnapshotConsumer = new KafkaConsumer<byte[], JsonNode>(getKafkaConsumerProperties());
  }

  public void run() {
    List<String> metricsToExtract = Arrays.asList(metrics.split("\\s*,\\s*"));

    while (true) {
      ConsumerRecords<byte[], JsonNode> records = metricsSnapshotConsumer.poll(30000);

      for (ConsumerRecord<byte[], JsonNode> record : records) {
        JsonNode root = record.value();
        System.out.println("Received a metrics snapshot..");
        for (String metricPath : metricsToExtract) {
          List<String> pathComponents = Arrays.asList(metricPath.split("\\s*.\\s*"));
          JsonNode field = root;

          for (String pathComponent : pathComponents) {
            if (!field.isMissingNode()) {
              field = field.path(pathComponent);
            }
          }

          if (!field.isMissingNode()) {
            if (field.isDouble()) {
              System.out.println(String.format("%s: %s", metricPath, field.doubleValue()));
            } else if (field.isFloat()) {
              System.out.println(String.format("%s: %s", metricPath, field.floatValue()));
            } else if (field.isInt()) {
              System.out.println(String.format("%s: %s", metricPath, field.intValue()));
            }
          }
        }
      }
    }
  }

  private Properties getKafkaConsumerProperties() {
    Properties consumerProps = new Properties();

    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "metrics-snapshot-consumer-cli");
    consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    consumerProps.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
    consumerProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());

    return consumerProps;
  }
}
