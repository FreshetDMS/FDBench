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

package org.pathirage.fdbench.kafka.throughput;

import com.typesafe.config.Config;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.Gauge;
import org.pathirage.fdbench.FDBenchException;
import org.pathirage.fdbench.kafka.Constants;
import org.pathirage.fdbench.kafka.KafkaBenchmarkTask;
import org.pathirage.fdbench.metrics.api.Histogram;
import org.pathirage.fdbench.metrics.api.MetricsRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ConsumerThroughputTask extends KafkaBenchmarkTask {
  private static final Logger log = LoggerFactory.getLogger(ConsumerThroughputTask.class);

  private static final String CONSUMER_THROUGHPUT_BENCH = "kafka-producer-throughput";
  private final Gauge<Long> elapsedNs;
  private final Counter messagesConsumed;
  private final Counter bytesRead;
  private final Histogram consumeLatency;
  private final KafkaConsumer<byte[], byte[]> consumer;
  private final List<String> partitionAssignment;

  public ConsumerThroughputTask(String taskId, String benchmarkName, String containerId, MetricsRegistry metricsRegistry, Config rawConfig) {
    super(taskId, CONSUMER_THROUGHPUT_BENCH, benchmarkName, containerId, metricsRegistry, new ThroughputBenchmarkConfig(rawConfig));
    this.consumer = new KafkaConsumer<byte[], byte[]>(getConsumerProperties());
    this.elapsedNs = metricsRegistry.newGauge(CONSUMER_THROUGHPUT_BENCH, "elapsed-time", 0L);
    this.messagesConsumed = metricsRegistry.newCounter(CONSUMER_THROUGHPUT_BENCH, "messages-consumed");
    this.bytesRead = metricsRegistry.newCounter(CONSUMER_THROUGHPUT_BENCH, "bytes-read");
    this.consumeLatency = metricsRegistry.newHistogram(CONSUMER_THROUGHPUT_BENCH, "consume-latency",
        Constants.MAX_RECORDABLE_LATENCY,
        Constants.SIGNIFICANT_VALUE_DIGITS);
    this.partitionAssignment = getPartitionAssignment();
  }

  private List<String> getPartitionAssignment() {
    String assignedPartitions = System.getenv(Constants.ENV_PARTITIONS);
    if (assignedPartitions == null || assignedPartitions.isEmpty()) {
      throw new FDBenchException("Cannot find partition assignment.");
    }
    return Arrays.asList(assignedPartitions.split(","));
  }

  @Override
  public void stop() {
    if (consumer != null) {
      consumer.close();
    }
  }

  @Override
  public void run() {
    log.info("Starting consumer throughput benchmark task " + getTaskId() + " in container: " + getContainerId() + " with partition assignment: " + System.getenv(Constants.ENV_PARTITIONS));
    // Assign topic partitions
    List<TopicPartition> assignedPartitions = new ArrayList<>();

    for (String partition : partitionAssignment) {
      assignedPartitions.add(new TopicPartition(getTopic(), Integer.valueOf(partition)));
    }

    consumer.assign(assignedPartitions);

    // Seek to begining
    consumer.seekToBeginning(assignedPartitions.toArray(new TopicPartition[assignedPartitions.size()]));

    long startNs = System.nanoTime();
    long lastConsumerNs = System.nanoTime();
    while (messagesConsumed.getCount() < getRecordLimit() && System.nanoTime() - lastConsumerNs <= getConsumerTimeoutNanos()) {
      ConsumerRecords<byte[], byte[]> records = consumer.poll(100);
      if (records.count() > 0) {
        lastConsumerNs = System.nanoTime();
      }
      for (ConsumerRecord<byte[], byte[]> record : records) {
        messagesConsumed.inc();
        if (record.key() != null) {
          bytesRead.inc(record.key().length);
        }
        if (record.value() != null) {
          bytesRead.inc(record.value().length);
        }
        elapsedNs.set(System.nanoTime() - startNs);
      }
    }
  }

  private long getConsumerTimeoutNanos() {
    return ((ThroughputBenchmarkConfig) getConfig()).getConsumerTimeoutInSeconds() * 1000000000L;
  }
}
