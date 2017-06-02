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
package org.pathirage.fdbench.tools;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.databind.JsonNode;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import net.minidev.json.JSONArray;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class MetricsSnapshotConsumerCLI {
  private static final Logger log = LoggerFactory.getLogger(MetricsSnapshotConsumerCLI.class);

  @Parameter(description = "Snapshot type")
  private String type;

  @Parameter(names = {"-b", "--brokers"})
  private String brokers = "localhost:9092";

  @Parameter(names = {"-ft", "--fdbench-topic"}, required = true)
  private String fdbenchMetricsTopic;

  @Parameter(names = {"-st", "--samza-topic"}, required = true)
  private String samzaMetricsTopic;

  private KafkaConsumer<byte[], JsonNode> metricsSnapshotConsumer;

  public static void main(String[] args) throws IOException {
    exec(args);
  }

  public static void exec(String[] args) {
    MetricsSnapshotConsumerCLI cli = new MetricsSnapshotConsumerCLI();
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
    List<String> topics = new ArrayList<>();
    topics.add(fdbenchMetricsTopic);
    topics.add(samzaMetricsTopic);

    metricsSnapshotConsumer.subscribe(topics);

    while (true) {
      ConsumerRecords<byte[], JsonNode> records = metricsSnapshotConsumer.poll(30000);

      for (ConsumerRecord<byte[], JsonNode> record : records) {
        if (record.topic().equals(fdbenchMetricsTopic)) {
          Double e2eLatency = (Double) JsonPath.<JSONArray>read(record.value().toString(), "$.body..e2e-latency").get(0);
          Object messageRate = JsonPath.<JSONArray>read(record.value().toString(), "$.body..current-message-rate").get(0);
          Object messagesConsumed = JsonPath.<JSONArray>read(record.value().toString(), "$.body..messages-consumed").get(0);
          Object messagesSent = JsonPath.<JSONArray>read(record.value().toString(), "$.body..messages-sent").get(0);
          log.info("[FDBench] mean e2e latency: " + e2eLatency + " milliseconds, message rate: " + messageRate + ", messages consumed: " + messagesConsumed + " messages sent: " + messagesSent);
        } else {
          try {
            double processNs = JsonPath.read(record.value().toString(), "$.metrics.['org.apache.samza.container.SamzaContainerMetrics'].process-ns");
            Object util = JsonPath.read(record.value().toString(), "$.metrics.['org.apache.samza.container.SamzaContainerMetrics'].event-loop-utilization");
            log.info("[Samza] process-ns: " + processNs + " nanoseconds, event-loop-utilization: " + util + " %");
          } catch (PathNotFoundException e) {
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
