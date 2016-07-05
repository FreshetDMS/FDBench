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

package org.pathirage.fdbench.kafka.e2elatency;

import com.typesafe.config.Config;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.samza.metrics.Counter;
import org.pathirage.fdbench.api.BenchmarkTask;
import org.pathirage.fdbench.metrics.api.Histogram;
import org.pathirage.fdbench.metrics.api.MetricsRegistry;
import org.pathirage.fdbench.metrics.api.MetricsReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.Random;

public class E2ELatencyBenchTask implements BenchmarkTask {
  private static final Logger log = LoggerFactory.getLogger(E2ELatencyBenchTask.class);

  private static final String E2EBENCH = "e2e-latency";
  static final String ENV_PARTITIONS = "E2EBENCH_PARTITIONS";
  static final String ENV_TOPIC = "E2EBENCH_TOPIC";
  static final String ENV_BROKERS = "E2EBENCH_BROKERS";
  static final String ENV_ZK = "E2EBENCH_ZK";
  private static final long maxRecordableLatencyNS = 300000000000L;
  private static final int sigFigs = 5;

  private Random random = new Random(System.currentTimeMillis());

  private final String taskId;
  private final String benchmarkName;
  private final String containerId;
  private final E2ELatencyBenchmarkConfig config;
  private final MetricsRegistry metricsRegistry;
  private final Histogram successHistogram;
  private final Histogram uncorrectedSuccessHistogram;
  private final Histogram errorHistorgram;
  private final Histogram uncorrectedErrorHistorgram;
  private final Counter successTotal;
  private final Counter errorTotal;
  private long elapsedTime = 0;
  private final Duration taskDuration;
  private final int requestRate;
  private final Duration expectedInterval;

  private KafkaConsumer<byte[], byte[]> consumer;
  private KafkaProducer<byte[], byte[]> producer;


  public E2ELatencyBenchTask(String taskId, String benchmarkName, String containerId, Config rawConfig,
                             MetricsRegistry metricsRegistry) {
    this.taskId = taskId;
    this.benchmarkName = benchmarkName;
    this.containerId = containerId;
    this.config = new E2ELatencyBenchmarkConfig(rawConfig);
    this.metricsRegistry = metricsRegistry;
    this.successHistogram = metricsRegistry.newHistogram(E2EBENCH, "sucess", maxRecordableLatencyNS, sigFigs);
    this.uncorrectedSuccessHistogram = metricsRegistry.newHistogram(E2EBENCH, "uncorrected-sucess", maxRecordableLatencyNS, sigFigs);
    this.errorHistorgram = metricsRegistry.newHistogram(E2EBENCH, "error", maxRecordableLatencyNS, sigFigs);
    this.uncorrectedErrorHistorgram = metricsRegistry.newHistogram(E2EBENCH, "uncorrected-error", maxRecordableLatencyNS, sigFigs);
    this.successTotal = metricsRegistry.newCounter(E2EBENCH, "success-count");
    this.errorTotal = metricsRegistry.newCounter(E2EBENCH, "error-count");
    this.producer = new KafkaProducer<byte[], byte[]>(getProducerProperties());
    this.consumer = new KafkaConsumer<byte[], byte[]>(getConsumerProperties());
    this.taskDuration = Duration.ofSeconds(config.getDurationSeconds());
    this.requestRate = config.getMessageRate();

    if (this.requestRate > 0) {
      this.expectedInterval = Duration.ofNanos(1000000000 / requestRate);
    } else {
      this.expectedInterval = Duration.ZERO;
    }
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
      reporter.register(String.format("E2ELatencyBenchTask-%s-%s", containerId, taskId), metricsRegistry);
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

  private void runFullThrottle() {
    long stopAfter = System.currentTimeMillis() + taskDuration.toMillis();
    long before, latency;

    while (true) {
      if (System.currentTimeMillis() >= stopAfter) {
        break;
      }

      before = System.nanoTime();

      try {
        producer.send(new ProducerRecord<byte[], byte[]>(System.getenv(ENV_TOPIC), genMessage())).get();
        ConsumerRecords<byte[], byte[]> records = consumer.poll(30000);
        if (records.isEmpty()) {
          throw new Exception("Didn't receive a response.");
        }
        latency = System.nanoTime() - before;
        successHistogram.recordValue(latency);
        successTotal.inc();
      } catch (Exception e) {
        log.error("Error occurred.", e);
        latency = System.nanoTime() - before;
        errorHistorgram.recordValue(latency);
        errorTotal.inc();
      }
    }
  }

  private void runRateLimited() {
    long stopAfter = System.currentTimeMillis() + taskDuration.toMillis();
    long before, latency;

    while (true) {
      if (System.currentTimeMillis() >= stopAfter) {
        break;
      }

      before = System.nanoTime();

      try {
        producer.send(new ProducerRecord<byte[], byte[]>(System.getenv(ENV_TOPIC), genMessage())).get();
        ConsumerRecords<byte[], byte[]> records = consumer.poll(30000);
        if (records.isEmpty()) {
          throw new Exception("Didn't receive a response.");
        }
        latency = System.nanoTime() - before;
        successHistogram.recordValueWithExpectedInterval(latency, expectedInterval.toNanos());
        uncorrectedSuccessHistogram.recordValue(latency);
        successTotal.inc();
      } catch (Exception e) {
        log.error("Error occurred.", e);
        latency = System.nanoTime() - before;
        errorHistorgram.recordValueWithExpectedInterval(latency, expectedInterval.toNanos());
        uncorrectedErrorHistorgram.recordValue(latency);
        errorTotal.inc();
      }

      while (expectedInterval.toNanos() > (System.nanoTime() - before)) {
        // busy loop
      }
    }
  }

  private byte[] genMessage() {
    byte[] payload = new byte[config.getMessageSize()];

    for (int i = 0; i < payload.length; i++) {
      payload[i] = (byte) (random.nextInt(26) + 65);
    }

    return payload;
  }

  @Override
  public void run() {
    log.info("Subscribing to topic " + System.getenv(ENV_TOPIC));
    consumer.subscribe(Collections.singletonList(System.getenv(ENV_TOPIC)));

    if (requestRate <= 0) {
      runFullThrottle();
    } else {
      runRateLimited();
    }
  }

  private Properties getProducerProperties() {
    Properties producerProps = new Properties();

    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBrokers());
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, config.getKeySerializerClass());
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, config.getValueSerializerClass());
    producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, "e2e-latency-bench-" + taskId);

    return producerProps;
  }

  private Properties getConsumerProperties() {
    Properties consumerProps = new Properties();

    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBrokers());
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "e2e-latency-bench");
    consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    consumerProps.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
    consumerProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, config.getKeyDeserializerClass());
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, config.getValueDeserializerClass());

    return consumerProps;
  }
}
