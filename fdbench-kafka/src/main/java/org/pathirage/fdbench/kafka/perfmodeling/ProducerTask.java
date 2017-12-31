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

package org.pathirage.fdbench.kafka.perfmodeling;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.Gauge;
import org.pathirage.fdbench.api.Constants;
import org.pathirage.fdbench.kafka.KafkaBenchmarkConfig;
import org.pathirage.fdbench.kafka.KafkaBenchmarkConstants;
import org.pathirage.fdbench.kafka.KafkaBenchmarkTask;
import org.pathirage.fdbench.kafka.NSThroughputThrottler;
import org.pathirage.fdbench.metrics.api.Histogram;
import org.pathirage.fdbench.metrics.api.MetricsRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

/**
 * Generates message of a given size for a given message rate.
 */
public class ProducerTask extends KafkaBenchmarkTask {
  private static final Logger log = LoggerFactory.getLogger(ProducerTask.class);

  private static final String PRODUCER_LOAD_GENERATOR = "kafka-producer-load-generator";

  private final Histogram latency;
  private final Gauge<Long> elapsedTime;
  private final Counter messagesSent;
  private final Counter bytesSent;
  private final Counter errors;
  private final int delay;

  protected KafkaProducer<byte[], byte[]> producer;
  protected List<Integer> partitionAssignment;

  public ProducerTask(String taskId, String benchmarkName, String containerId, MetricsRegistry metricsRegistry, KafkaBenchmarkConfig config) {
    super(taskId, PRODUCER_LOAD_GENERATOR, benchmarkName, containerId, metricsRegistry, config);
    this.elapsedTime = metricsRegistry.<Long>newGauge(getGroup(), "elapsed-time", 0L);
    this.latency = metricsRegistry.newHistogram(getGroup(), "produce-latency", KafkaBenchmarkConstants.MAX_RECORDABLE_LATENCY, KafkaBenchmarkConstants.SIGNIFICANT_VALUE_DIGITS);
    this.messagesSent = metricsRegistry.newCounter(getGroup(), "messages-sent");
    this.bytesSent = metricsRegistry.newCounter(getGroup(), "bytes-sent");
    this.errors = metricsRegistry.newCounter(getGroup(), "error-count");
    this.producer = new KafkaProducer<byte[], byte[]>(getProducerProperties());
    this.partitionAssignment = Arrays.stream(
        System.getenv(KafkaBenchmarkConstants.ENV_KAFKA_BENCH_PARTITIONS).split(","))
        .map((s) -> Integer.valueOf(s.trim())).collect(Collectors.toList());
    this.delay = Integer.valueOf(System.getenv(SyntheticWorkloadGeneratorConstants.ENV_KAFKA_WORKLOAD_GENERATOR_TASK_DELAY));
  }

  @Override
  public void stop() {
    if (producer != null) {
      producer.close();
    }
  }

  @Override
  public String getTopic() {
    return System.getenv(KafkaBenchmarkConstants.ENV_KAFKA_BENCH_TOPIC).trim();
  }

  @Override
  protected int getMessageRate() {
    return Integer.valueOf(System.getenv(SyntheticWorkloadGeneratorConstants.ENV_KAFKA_WORKLOAD_GENERATOR_MESSAGE_RATE));
  }

  @Override
  protected int getMessageSize() {
    return Integer.valueOf(System.getenv(SyntheticWorkloadGeneratorConstants.ENV_KAFKA_WORKLOAD_GENERATOR_MSG_SIZE_MEAN));
  }

  @Override
  public void run() {
    log.info("Starting producer throughput benchmark task " + getTaskId() + " in container: " + getContainerId() +
        " with partition assignment: " + System.getenv(KafkaBenchmarkConstants.ENV_KAFKA_BENCH_PARTITIONS) + " of topic: " + getTopic());
    log.info("The record rate: " + getMessageRate() + ", average message size: " + getMessageSize() +
        ", benchmark duration: " + getBenchmarkDuration());

    if (delay > 0) {
      log.info("Waiting for " + this.delay + " seconds before producing messages.");
      try {
        Thread.sleep(delay * 1000);
      } catch (InterruptedException e) {
        log.info("Task delaying got interrupted.", e);
      }
    }

    int i = 0;
    long startTime = System.currentTimeMillis();
    long startNS = System.nanoTime();
    NSThroughputThrottler throttler = new NSThroughputThrottler(getMessageRate(), startNS);
    while ((System.currentTimeMillis() - startTime) < getBenchmarkDuration() * 1000) {
      long sendStartNanos = System.nanoTime();
      byte[] msg = generateRandomMessage();
      producer.send(new ProducerRecord<byte[], byte[]>(getTopic(),
              partitionAssignment.get(ThreadLocalRandom.current().nextInt(partitionAssignment.size())),
              msgToKey(msg),
              msg),
          new ProduceCompletionCallback(startTime, sendStartNanos, msg.length));

      i++;

      if (throttler.shouldThrottle(i, sendStartNanos)) {
        throttler.throttle();
      }

      if (i % 100000 == 0) {
        log.info(String.format("Sent %s messages.", i));
      }
    }
  }

  protected byte[] msgToKey(byte[] msg) {
    try {
      MessageDigest messageDigest = MessageDigest.getInstance("MD5");
      return messageDigest.digest(msg);
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }

  public void setup() {

  }

  protected String getGroup() {
    return PRODUCER_LOAD_GENERATOR + "-" + System.getenv(SyntheticWorkloadGeneratorConstants.ENV_KAFKA_WORKLOAD_GENERATOR_TASK_GROUP);
  }

  public class ProduceCompletionCallback implements Callback {
    private final long startNs;
    private final long sendStartNanos;
    private final int messageSize;

    public ProduceCompletionCallback(long startNs, long sendStartNanos, int messageSize) {
      this.startNs = startNs;
      this.sendStartNanos = sendStartNanos;
      this.messageSize = messageSize;
    }

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
      long now = System.nanoTime();
      elapsedTime.set(now - startNs);
      long l = now - sendStartNanos;
      bytesSent.inc(messageSize);
      messagesSent.inc();
      latency.recordValue(l); // Since this is workload generation, corrected histogram is not possible.
      if (exception != null) {
        errors.inc();
      }
    }
  }

}
