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
import org.pathirage.fdbench.kafka.Constants;
import org.pathirage.fdbench.kafka.KafkaBenchmarkTask;
import org.pathirage.fdbench.metrics.api.Histogram;
import org.pathirage.fdbench.metrics.api.MetricsRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class E2ELatencyBenchTask extends KafkaBenchmarkTask {
  private static final Logger log = LoggerFactory.getLogger(E2ELatencyBenchTask.class);

  private static final String E2EBENCH = "e2e-latency";
  private static final long maxRecordableLatencyNS = 300000000000L;
  private static final int sigFigs = 5;

  private final Histogram successHistogram;
  private final Histogram uncorrectedSuccessHistogram;
  private final Histogram errorHistogram;
  private final Histogram uncorrectedErrorHistogram;
  private final Counter successTotal;
  private final Counter errorTotal;
  private final Duration taskDuration;
  private final int requestRate;
  private final Duration expectedInterval;

  private KafkaConsumer<byte[], byte[]> consumer;
  private KafkaProducer<byte[], byte[]> producer;


  public E2ELatencyBenchTask(String taskId, String benchmarkName, String containerId, Config rawConfig,
                             MetricsRegistry metricsRegistry) {
    super(taskId, "e2elatencybench", benchmarkName, containerId, metricsRegistry, new E2ELatencyBenchmarkConfig(rawConfig));
    this.successHistogram = metricsRegistry.newHistogram(E2EBENCH, "hist-success", maxRecordableLatencyNS, sigFigs);
    this.uncorrectedSuccessHistogram = metricsRegistry.newHistogram(E2EBENCH, "hist-uncorrected-success", maxRecordableLatencyNS, sigFigs);
    this.errorHistogram = metricsRegistry.newHistogram(E2EBENCH, "hist-error", maxRecordableLatencyNS, sigFigs);
    this.uncorrectedErrorHistogram = metricsRegistry.newHistogram(E2EBENCH, "hist-uncorrected-error", maxRecordableLatencyNS, sigFigs);
    this.successTotal = metricsRegistry.newCounter(E2EBENCH, "success-count");
    this.errorTotal = metricsRegistry.newCounter(E2EBENCH, "error-count");
    this.producer = new KafkaProducer<byte[], byte[]>(getProducerProperties());
    this.consumer = new KafkaConsumer<byte[], byte[]>(getConsumerProperties());
    this.taskDuration = Duration.ofSeconds(getBenchmarkDuration());
    this.requestRate = getMessageRate();

    if (this.requestRate > 0) {
      this.expectedInterval = Duration.ofNanos(1000000000 / requestRate);
    } else {
      this.expectedInterval = Duration.ZERO;
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
        producer.send(new ProducerRecord<byte[], byte[]>(System.getenv(Constants.ENV_TOPIC), generateRandomMessage())).get();
        ConsumerRecords<byte[], byte[]> records = consumer.poll(30000);
        if (records.isEmpty()) {
          String errMsg = "Didn't receive a response";
          log.error(errMsg);
          throw new Exception(errMsg);
        }
        latency = System.nanoTime() - before;
        successHistogram.recordValue(latency);
        successTotal.inc();
      } catch (Exception e) {
        log.error("Error occurred.", e);
        latency = System.nanoTime() - before;
        errorHistogram.recordValue(latency);
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
        producer.send(new ProducerRecord<byte[], byte[]>(System.getenv(Constants.ENV_TOPIC), generateRandomMessage())).get();
        ConsumerRecords<byte[], byte[]> records = consumer.poll(30000);
        if (records.isEmpty()) {
          String errMsg = "Didn't receive a response";
          log.error(errMsg);
          throw new Exception(errMsg);
        }
        latency = System.nanoTime() - before;
        successHistogram.recordValueWithExpectedInterval(latency, expectedInterval.toNanos());
        uncorrectedSuccessHistogram.recordValue(latency);
        successTotal.inc();
      } catch (Exception e) {
        log.error("Error occurred.", e);
        latency = System.nanoTime() - before;
        errorHistogram.recordValueWithExpectedInterval(latency, expectedInterval.toNanos());
        uncorrectedErrorHistogram.recordValue(latency);
        errorTotal.inc();
      }

      while (expectedInterval.toNanos() > (System.nanoTime() - before)) {
        // busy loop
      }
    }
  }


  @Override
  public void run() {
    log.info("Subscribing to topic " + System.getenv(Constants.ENV_TOPIC));
    consumer.subscribe(Collections.singletonList(System.getenv(Constants.ENV_TOPIC)));

    if (requestRate <= 0) {
      runFullThrottle();
    } else {
      runRateLimited();
    }
  }


}
