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
import org.pathirage.fdbench.kafka.KafkaBenchmarkTask;
import org.pathirage.fdbench.kafka.Utils;
import org.pathirage.fdbench.metrics.api.Histogram;
import org.pathirage.fdbench.metrics.api.MetricsRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;

/**
 * Generates message of a given size distribution according to the poisson inter-event arrival time distribution for a
 * given message rate.
 */
public class ProducerTask extends KafkaBenchmarkTask {
  private static final Logger log = LoggerFactory.getLogger(ProducerTask.class);

  private static final String LOAD_GENERATOR = "kafka-load-generator";

  private final Histogram latency;
  private final Gauge<Long> elapsedTime;
  private final Counter messagesSent;

  private KafkaProducer<byte[], byte[]> producer;
  private List<Integer> partitionAssignment;

  public ProducerTask(String taskId, String benchmarkName, String containerId, MetricsRegistry metricsRegistry, KafkaBenchmarkConfig config) {
    super(taskId, "load-generator-task", benchmarkName, containerId, metricsRegistry, config);
    this.elapsedTime = metricsRegistry.<Long>newGauge(LOAD_GENERATOR, "elapsed-time", 0L);
    this.latency = metricsRegistry.newHistogram(LOAD_GENERATOR, "produce-latency", org.pathirage.fdbench.kafka.Constants.MAX_RECORDABLE_LATENCY, org.pathirage.fdbench.kafka.Constants.SIGNIFICANT_VALUE_DIGITS);
    this.messagesSent = metricsRegistry.newCounter(LOAD_GENERATOR, "messages-sent-or-consumed");
    this.producer = new KafkaProducer<byte[], byte[]>(getProducerProperties());
    this.partitionAssignment = Arrays.asList(
        System.getenv(Constants.FDBENCH_PARTITION_ASSIGNMENT).split(","))
        .stream()
        .map((s) -> Integer.valueOf(s)).collect(Collectors.toList());
  }

  @Override
  public void stop() {
    if (producer != null) {
      producer.close();
    }
  }

  @Override
  public String getTopic() {
    return System.getenv(Constants.FDBENCH_TOPIC).trim();
  }

  @Override
  public void run() {
    log.info("Starting producer throughput benchmark task " + getTaskId() + " in container: " + getContainerId() +
        " with partition assignment: " + System.getenv(Constants.FDBENCH_PARTITION_ASSIGNMENT) + " of topic: " + getTopic());
    log.info("The record rate: " + getMessageRate() + ", average message size: " + getMessageSize() +
        ", benchmark duration: " + getBenchmarkDuration());

    int i = 0;
    long startTime = System.currentTimeMillis();
    while (true && (System.currentTimeMillis() - startTime) < getBenchmarkDuration() * 1000) {
      long interval = (long) Utils.poissonRandomInterArrivalDelay((1 / getMessageRate()) * 1000000000);
      // This is not a high accuracy sleep. But will work for microsecond sleep times
      // http://www.rationaljava.com/2015/10/measuring-microsecond-in-java.html
      LockSupport.parkNanos(interval);
      long sendStartNanos = System.nanoTime();
      byte[] msg = generateRandomMessage();
      producer.send(new ProducerRecord<byte[], byte[]>(getTopic(),
              partitionAssignment.get(ThreadLocalRandom.current().nextInt(partitionAssignment.size())),
              msgToKey(msg),
              msg),
          new ProduceCompletionCallback(startTime, sendStartNanos));

      i++;

      if (i % 1000 == 0) {
        log.info(String.format("Sent %s messages.", i));
      }
    }
  }

  private byte[] msgToKey(byte[] msg) {
    try {
      MessageDigest messageDigest = MessageDigest.getInstance("MD5");
      return messageDigest.digest(msg);
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }

  public void setup() {

  }

  public class ProduceCompletionCallback implements Callback {
    private final long startNs;
    private final long sendStartNanos;

    public ProduceCompletionCallback(long startNs, long sendStartNanos) {
      this.startNs = startNs;
      this.sendStartNanos = sendStartNanos;
    }

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
      if (log.isDebugEnabled()) {
        log.debug("Produce to Kafka completed.");
      }
      long now = System.nanoTime();
      elapsedTime.set(now - startNs);
      long l = now - sendStartNanos;
      if (exception == null) {
        messagesSent.inc();
        latency.recordValue(l); // Since this is workload generation, corrected histogram is not possible.
      }
    }
  }

}
