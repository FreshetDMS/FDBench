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

package org.pathirage.fdbench.samza;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.typesafe.config.Config;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.Gauge;
import org.pathirage.fdbench.api.BenchmarkTask;
import org.pathirage.fdbench.config.BenchConfig;
import org.pathirage.fdbench.kafka.KafkaBenchmarkConfig;
import org.pathirage.fdbench.kafka.KafkaBenchmarkConstants;
import org.pathirage.fdbench.kafka.KafkaBenchmarkTask;
import org.pathirage.fdbench.kafka.KafkaConfig;
import org.pathirage.fdbench.metrics.api.Histogram;
import org.pathirage.fdbench.metrics.api.MetricsRegistry;
import org.pathirage.fdbench.metrics.api.MetricsReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Int;

import java.util.*;
import java.util.concurrent.*;

public class SamzaE2ELatencyBenchmarkTask implements BenchmarkTask {
  private static final Logger log = LoggerFactory.getLogger(SamzaE2ELatencyBenchmarkTask.class);

  private final String benchmarkName;
  private final String taskId;
  private final String containerId;
  private final Config config;
  private final MetricsRegistry metricsRegistry;
  private final KafkaConfig kafkaConfig;
  private final RandomString randomString;
  private final RandomString randomKey;
  private final KafkaProducer<String, JsonNode> producer;
  private final KafkaConsumer<String, JsonNode> consumer;
  private final Histogram latency;
  private final Gauge<Long> elapsedTime;
  private final Counter messagesSent;
  private final Counter messagesConsumed;
  private final Counter errorCount;
  private final Integer benchmarkDuration;
  private final ExecutorService executor = Executors.newFixedThreadPool(2);
  private final CountDownLatch latch = new CountDownLatch(2);

  public SamzaE2ELatencyBenchmarkTask(String benchmarkName, String taskId, String containerId, Config config, MetricsRegistry metricsRegistry) {
    this.benchmarkName = benchmarkName;
    this.taskId = taskId;
    this.containerId = containerId;
    this.config = config;
    this.metricsRegistry = metricsRegistry;
    this.kafkaConfig = new KafkaConfig(config);
    this.randomString = new RandomString(Integer.valueOf(System.getenv(SamzaE2ELatencyBenchmarkConstants.MESSAGE_SIZE)));
    this.randomKey = new RandomString(16);
    this.producer = new KafkaProducer<String, JsonNode>(getProducerProperties());
    this.consumer = new KafkaConsumer<String, JsonNode>(getConsumerProperties());
    this.elapsedTime = metricsRegistry.<Long>newGauge(getGroup(), "elapsed-time", 0L);
    this.latency = metricsRegistry.newHistogram(getGroup(), "produce-latency", KafkaBenchmarkConstants.MAX_RECORDABLE_LATENCY, KafkaBenchmarkConstants.SIGNIFICANT_VALUE_DIGITS);
    this.messagesSent = metricsRegistry.newCounter(getGroup(), "messages-sent");
    this.messagesConsumed = metricsRegistry.newCounter(getGroup(), "messages-consumed");
    this.errorCount = metricsRegistry.newCounter(getGroup(), "errors");
    this.benchmarkDuration = new BenchConfig(config).getBenchmarkDuration();
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

    if (consumer != null) {
      consumer.close();
    }
  }

  @Override
  public void setup() {

  }

  @Override
  public void run() {
    executor.execute(() -> {
      long stopAfter = System.currentTimeMillis() + benchmarkDuration * 1000;

      while (true) {
        if (System.currentTimeMillis() >= stopAfter) {
          break;
        }

        try {
          producer.send(new ProducerRecord<>(System.getenv(SamzaE2ELatencyBenchmarkConstants.SOURCE_TOPIC), randomKey.nextString(), generateMessage())).get();
        } catch (Exception e) {
          log.error("Error occurred.", e);
          errorCount.inc();
        }
      }

      latch.countDown();
    });

    executor.execute(() -> {
      List<String> topics = new ArrayList<>();
      topics.add(System.getenv(SamzaE2ELatencyBenchmarkConstants.RESULT_TOPIC));

      consumer.subscribe(topics);

      while (true) {
        ConsumerRecords<String, JsonNode> records = consumer.poll(30000);
        long receivedAt = System.currentTimeMillis();
        if (records.isEmpty()) {
          log.warn("No messages for 30 seconds. Shutting down the consumer thread.");
          break;
        }

        for (ConsumerRecord<String, JsonNode> record : records) {
          JsonNode root = record.value();
          long createdAt = root.get("created-at").asLong();
          latency.recordValue(receivedAt - createdAt);
        }
      }

      latch.countDown();
    });

    try {
      latch.await(benchmarkDuration * 2, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      log.error("Waiting for producer and consumer to finish got interrupted.", e);
    } finally {
      if (executor != null && !executor.isTerminated()) {
        executor.shutdownNow();
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

  private Properties getConsumerProperties() {
    Properties consumerProps = new Properties();

    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv(KafkaBenchmarkConstants.ENV_KAFKA_BENCH_BROKERS));
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, getBenchmarkName() + "-consumer-group-" + getTaskId());
    consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    consumerProps.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
    consumerProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonDeserializer");

    return consumerProps;
  }

}
