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
package org.pathirage.fdbench.samza;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.typesafe.config.Config;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.tools.ThroughputThrottler;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.Gauge;
import org.pathirage.fdbench.api.BenchmarkTask;
import org.pathirage.fdbench.kafka.KafkaBenchmarkConstants;
import org.pathirage.fdbench.metrics.api.MetricsRegistry;
import org.pathirage.fdbench.metrics.api.MetricsReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Properties;

public class SamzaE2ELatencyProduceTask implements BenchmarkTask {
  private static final Logger log = LoggerFactory.getLogger(SamzaE2ELatencyProduceTask.class);

  private final String benchmarkName;
  private final String taskId;
  private final String containerId;
  private final MetricsRegistry metricsRegistry;
  private final RandomString randomString;
  private final RandomString randomKey;
  private final KafkaProducer<String, JsonNode> producer;
  private final Integer benchmarkDuration;
  private final Gauge<Long> elapsedTime;
  private final Gauge<Long> messageRateGuage;
  private final Counter messagesSent;

  public SamzaE2ELatencyProduceTask(String benchmarkName, String taskId, String containerID, Config config, MetricsRegistry metricsRegistry) {
    this.benchmarkName = benchmarkName;
    this.taskId = taskId;
    this.containerId = containerID;
    this.metricsRegistry = metricsRegistry;
    this.randomString = new RandomString(Integer.valueOf(System.getenv(SamzaE2ELatencyBenchmarkConstants.MESSAGE_SIZE)));
    this.randomKey = new RandomString(16);
    this.producer = new KafkaProducer<String, JsonNode>(getProducerProperties());
    this.benchmarkDuration = new SamzaE2ELatencyBenchmarkConfig(config).getDurationSeconds();
    this.elapsedTime = metricsRegistry.<Long>newGauge(getGroup(), "elapsed-time", 0L);
    this.messagesSent = metricsRegistry.newCounter(getGroup(), "messages-sent");
    this.messageRateGuage = metricsRegistry.newGauge(getGroup(), "current-message-rate", 0L);
  }

  private String getGroup() {
    return benchmarkName + "-" + getTaskId();
  }

  @Override
  public String getTaskId() {
    return taskId;
  }

  @Override
  public String getBenchmarkName() {
    return benchmarkName;
  }

  @Override
  public String getContainerId() {
    return containerId;
  }

  @Override
  public void registerMetrics(Collection<MetricsReporter> reporters) {
    for (MetricsReporter reporter : reporters) {
      reporter.register(String.format("%s-%s-%s", benchmarkName, containerId, taskId), metricsRegistry);
    }
  }

  @Override
  public void stop() {
    if (producer != null) {
      producer.close();
    }
  }

  @Override
  public void setup() {

  }

  @Override
  public void run() {
    long currentTime = System.currentTimeMillis();
    long stopAfter = System.currentTimeMillis() + benchmarkDuration * 1000;
    long messageRate = Long.valueOf(System.getenv(SamzaE2ELatencyBenchmarkConstants.MESSAGE_BASE_RATE));
    messageRateGuage.set(messageRate);
    ThroughputThrottler throughputThrottler = new ThroughputThrottler(messageRate, System.currentTimeMillis());

    int i = 0;
    while (true) {
      if (System.currentTimeMillis() >= stopAfter) {
        break;
      }

      try {
        producer.send(new ProducerRecord<>(System.getenv(SamzaE2ELatencyBenchmarkConstants.SOURCE_TOPIC), randomKey.nextString(), generateMessage())).get();
        messagesSent.inc();
        elapsedTime.set(System.currentTimeMillis() - currentTime);
      } catch (Exception e) {
        log.error("Error occurred.", e);
      }

      i++;

      if (throughputThrottler.shouldThrottle(i, System.currentTimeMillis())) {
        throughputThrottler.throttle();
      }
    }
  }

  private JsonNode generateMessage() {
    ObjectNode root = JsonNodeFactory.instance.objectNode();

    root.put("payload", randomString.nextString());
    root.put("created-at", System.currentTimeMillis());

    return root;
  }

  private Properties getProducerProperties() {
    Properties producerProps = new Properties();

    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv(KafkaBenchmarkConstants.ENV_KAFKA_BENCH_BROKERS));
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonSerializer");
    producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, benchmarkName + "-producer-" + getTaskId());

    return producerProps;
  }
}
