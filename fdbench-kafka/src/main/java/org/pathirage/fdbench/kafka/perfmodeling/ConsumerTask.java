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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.Gauge;
import org.pathirage.fdbench.kafka.KafkaBenchmarkConfig;
import org.pathirage.fdbench.kafka.KafkaBenchmarkConstants;
import org.pathirage.fdbench.kafka.KafkaBenchmarkTask;
import org.pathirage.fdbench.metrics.api.MetricsRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Consume messages from a Kafka topic partition(s) in real-time or from the beginning.
 */
public class ConsumerTask extends KafkaBenchmarkTask {
  private static final Logger log = LoggerFactory.getLogger(ConsumerTask.class);

  private static final String CONSUMER_LOAD_GENERATOR = "kafka-consumer-load-generator";

  //  private final Histogram latency;
  private final Gauge<Long> elapsedTime;
  private final Counter messagesConsumed;
  private final Counter bytesConsumed;
  private final int delay;

  private KafkaConsumer<byte[], byte[]> consumer;
  private List<Integer> partitionAssignment;

  public ConsumerTask(String taskId, String benchmarkName, String containerId, MetricsRegistry metricsRegistry, KafkaBenchmarkConfig config) {
    super(taskId, CONSUMER_LOAD_GENERATOR, benchmarkName, containerId, metricsRegistry, config);
    this.elapsedTime = metricsRegistry.<Long>newGauge(getGroup(), "elapsed-time", 0L);
    this.messagesConsumed = metricsRegistry.newCounter(getGroup(), "messages-consumed");
    this.bytesConsumed = metricsRegistry.newCounter(getGroup(), "bytes-consumed");
    this.partitionAssignment = Arrays.stream(
        System.getenv(KafkaBenchmarkConstants.ENV_KAFKA_BENCH_PARTITIONS).split(","))
        .map((s) -> Integer.valueOf(s)).collect(Collectors.toList());
    this.delay = Integer.valueOf(System.getenv(SyntheticWorkloadGeneratorConstants.ENV_KAFKA_WORKLOAD_GENERATOR_TASK_DELAY));
    this.consumer = new KafkaConsumer<byte[], byte[]>(getConsumerProperties());
  }

  @Override
  protected Properties getConsumerProperties() {
    Properties props = super.getConsumerProperties();
    if (isReplay()) {
      props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    }
    return props;
  }

  private boolean isReplay() {
    SyntheticWorkloadGeneratorConfig.TopicConfig.Type taskType = SyntheticWorkloadGeneratorConfig.TopicConfig.Type.valueOf(
        System.getenv(SyntheticWorkloadGeneratorConstants.ENV_KAFKA_WORKLOAD_GENERATOR_TASK_TYPE));
    return taskType == SyntheticWorkloadGeneratorConfig.TopicConfig.Type.REPLAY;
  }

  private String getGroup() {
    return CONSUMER_LOAD_GENERATOR + "-" + System.getenv(SyntheticWorkloadGeneratorConstants.ENV_KAFKA_WORKLOAD_GENERATOR_TASK_GROUP);
  }

  @Override
  public void stop() {
    if (consumer != null) {
      consumer.close();
    }
  }

  @Override
  public void setup() {
    List<TopicPartition> partitions = new ArrayList<>();

    for (Integer p : partitionAssignment) {
      partitions.add(new TopicPartition(getTopic(), p));
    }

    this.consumer.assign(partitions);
  }

  @Override
  protected int getMessageRate() {
    return Integer.valueOf(System.getenv(SyntheticWorkloadGeneratorConstants.ENV_KAFKA_WORKLOAD_GENERATOR_MESSAGE_RATE));
  }

  @Override
  public void run() {
    // TODO: Simulate processing time
    if (delay > 0) {
      try {
        Thread.sleep(delay * 1000);
      } catch (InterruptedException e) {
        log.info("Delaying got interrupted.", e);
      }
    }

    int i = 0;
    long startTime = System.currentTimeMillis();
    long lastConsumed = System.currentTimeMillis();
    while ((System.currentTimeMillis() - startTime) < getBenchmarkDuration() * 1000 &&
        System.currentTimeMillis() - lastConsumed <= getConsumerTimeoutMilliseconds()) {
      ConsumerRecords<byte[], byte[]> records = consumer.poll(getConsumerBatchSize());
      if (records.count() > 0) {
        lastConsumed = System.currentTimeMillis();
      }

      for (ConsumerRecord<byte[], byte[]> r : records) {
        messagesConsumed.inc();
        if (r.key() != null) {
          bytesConsumed.inc(r.key().length);
        }

        if (r.value() != null) {
          bytesConsumed.inc(r.value().length);
        }

        elapsedTime.set(System.currentTimeMillis() - startTime);
      }

      if (messagesConsumed.getCount() % 100000 == 0){
        log.info(String.format("Consumed %s messages.", messagesConsumed.getCount()));
      }
    }

  }

  @Override
  public String getTopic() {
    return System.getenv(KafkaBenchmarkConstants.ENV_KAFKA_BENCH_TOPIC).trim();
  }

  // TODO: Read below from environment variables
  public long getConsumerTimeoutMilliseconds() {
    return 60 * 1000L;
  }

  public long getConsumerBatchSize() {
    return 100;
  }
}
