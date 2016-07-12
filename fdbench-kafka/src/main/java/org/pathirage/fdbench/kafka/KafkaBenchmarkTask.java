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

package org.pathirage.fdbench.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.pathirage.fdbench.api.BenchmarkTask;
import org.pathirage.fdbench.metrics.api.MetricsRegistry;
import org.pathirage.fdbench.metrics.api.MetricsReporter;

import java.util.Collection;
import java.util.Properties;
import java.util.Random;

public abstract class KafkaBenchmarkTask implements BenchmarkTask {
  private Random random = new Random(System.currentTimeMillis());

  final String taskId;
  final String benchmarkName;
  final String containerId;
  final MetricsRegistry metricsRegistry;
  final KafkaBenchmarkConfig config;
  final String taskType;

  public KafkaBenchmarkTask(String taskId, String taskType, String benchmarkName, String containerId, MetricsRegistry metricsRegistry, KafkaBenchmarkConfig config) {
    this.taskId = taskId;
    this.benchmarkName = benchmarkName;
    this.containerId = containerId;
    this.metricsRegistry = metricsRegistry;
    this.config = config;
    this.taskType = taskType;
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
      reporter.register(String.format("%s-%s-%s", taskType, containerId, taskId), metricsRegistry);
    }
  }

  protected byte[] generateRandomMessage() {
    byte[] payload = new byte[getMessageSize()];

    for (int i = 0; i < payload.length; i++) {
      payload[i] = (byte) (random.nextInt(26) + 65);
    }

    return payload;
  }

  protected Properties getProducerProperties() {
    Properties producerProps = new Properties();

    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getBrokers());
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, getKeySerializerClass());
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, getValueSerializerClass());
    producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, getTaskType() + "-producer-" + getTaskId());

    return producerProps;
  }

  protected Properties getConsumerProperties() {
    Properties consumerProps = new Properties();

    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getBrokers());
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, getTaskType() + "-" + getBenchmarkName() + "-consumer-group");
    consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    consumerProps.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
    consumerProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, getKeyDeserializerClass());
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, getValueDeserializerClass());

    return consumerProps;
  }

  public String getTopic() {
    return config.getTopic();
  }

  public String getTaskType() {
    return taskType;
  }

  protected int getMessageRate() {
    return config.getMessageRate();
  }

  protected int getBenchmarkDuration() {
    return config.getDurationSeconds();
  }

  protected int getMessageSize() {
    return config.getMessageSize();
  }

  protected String getBrokers() {
    return config.getBrokers();
  }

  protected String getZKConnectionString() {
    return config.getZKConnectionString();
  }

  protected String getKeyDeserializerClass() {
    return config.getKeyDeserializerClass();
  }

  protected String getValueDeserializerClass() {
    return config.getValueDeserializerClass();
  }

  protected String getKeySerializerClass() {
    return config.getKeySerializerClass();
  }

  protected String getValueSerializerClass() {
    return config.getValueSerializerClass();
  }

  protected KafkaBenchmarkConfig getConfig() {
    return config;
  }

  protected int getRecordLimit() {
    return config.getRecordLimit();
  }
}
