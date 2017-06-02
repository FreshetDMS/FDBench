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

import org.pathirage.fdbench.api.Benchmark;
import org.pathirage.fdbench.api.BenchmarkDeploymentState;
import org.pathirage.fdbench.api.BenchmarkTaskFactory;
import org.pathirage.fdbench.config.BenchConfig;
import org.pathirage.fdbench.utils.kafka.KafkaAdmin;
import org.pathirage.fdbench.kafka.KafkaBenchmarkConstants;

import java.util.HashMap;
import java.util.Map;

public class SamzaE2ELatencyBenchmark implements Benchmark {
  private final Integer parallelism;

  private final SamzaE2ELatencyBenchmarkConfig config;
  private final KafkaAdmin kafkaAdmin;
  private final String benchName;

  public SamzaE2ELatencyBenchmark(Integer parallelism, SamzaE2ELatencyBenchmarkConfig config) {
    this.parallelism = parallelism;
    this.config = config;
    this.benchName = new BenchConfig(config.getRawConfig()).getName();
    this.kafkaAdmin = new KafkaAdmin(benchName, config.getBrokers(), config.getZKConnectionString());
  }

  @Override
  public void setup() {
    validateTaskAllocation();
    createTopic(config.getSource());
    createTopic(config.getResult());
  }

  @Override
  public void teardown() {
    // Delete topics
    deleteTopic(config.getSource());
    deleteTopic(config.getResult());
  }

  @Override
  public Class<? extends BenchmarkTaskFactory> getTaskFactoryClass() {
    return SamzaE2ELatencyBenchmarkTaskFactory.class;
  }

  @Override
  public Map<String, String> configureTask(int taskId, BenchmarkDeploymentState deploymentState) {
    Map<String, String> env = new HashMap<>();

    env.put(KafkaBenchmarkConstants.ENV_KAFKA_BENCH_BROKERS, config.getBrokers());
    env.put(KafkaBenchmarkConstants.ENV_KAFKA_BENCH_ZK, config.getZKConnectionString());
    env.put(SamzaE2ELatencyBenchmarkConstants.SOURCE_TOPIC, config.getSource().getName());
    env.put(SamzaE2ELatencyBenchmarkConstants.RESULT_TOPIC, config.getResult().getName());
    env.put(SamzaE2ELatencyBenchmarkConstants.MESSAGE_BASE_RATE, Integer.toString(config.getMessageRate()));
    env.put(SamzaE2ELatencyBenchmarkConstants.MESSAGE_SIZE, Integer.toString(config.getPayloadSize()));

    return env;
  }

  private void deleteTopic(SamzaE2ELatencyBenchmarkConfig.TopicConfig topicConfig) {
    if (!kafkaAdmin.isTopicExists(topicConfig.getName())) {
      kafkaAdmin.deleteTopic(topicConfig.getName());
    }
  }

  private void createTopic(SamzaE2ELatencyBenchmarkConfig.TopicConfig topicConfig) {
    if (!kafkaAdmin.isTopicExists(topicConfig.getName())) {
      kafkaAdmin.createTopic(topicConfig.getName(), topicConfig.getPartitions(), topicConfig.getReplicationFactor());
    }
  }

  private void validateTaskAllocation() {
    // In the initial prototype we only use a single task.
    if (parallelism != 1) {
      String errMsg = String.format("Configured parallelism %s does not match the required parallelism of 1",
          parallelism);
      throw new RuntimeException(errMsg);
    }
  }
}
