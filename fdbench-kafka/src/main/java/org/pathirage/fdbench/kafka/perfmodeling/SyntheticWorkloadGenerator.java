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

import com.google.common.base.Joiner;
import com.google.common.collect.Sets;
import org.pathirage.fdbench.api.BenchmarkDeploymentState;
import org.pathirage.fdbench.api.BenchmarkTaskFactory;
import org.pathirage.fdbench.api.Constants;
import org.pathirage.fdbench.aws.BenchmarkOnAWS;
import org.pathirage.fdbench.config.BenchConfig;
import org.pathirage.fdbench.kafka.KafkaAdmin;
import org.pathirage.fdbench.kafka.KafkaBenchmark;
import org.pathirage.fdbench.kafka.KafkaBenchmarkConfig;
import org.pathirage.fdbench.kafka.KafkaBenchmarkConstants;

import java.util.*;

public class SyntheticWorkloadGenerator extends BenchmarkOnAWS {
  private final SyntheticWorkloadGeneratorConfig workloadGeneratorConfig;
  private final KafkaAdmin kafkaAdmin;
  private final String benchName;
  public SyntheticWorkloadGenerator(KafkaBenchmarkConfig benchmarkConfig, int parallelism) {
    super(benchmarkConfig.getRawConfig());
    workloadGeneratorConfig = (SyntheticWorkloadGeneratorConfig) benchmarkConfig;
    benchName = new BenchConfig(benchmarkConfig.getRawConfig()).getName();
    kafkaAdmin = new KafkaAdmin(benchName, benchmarkConfig.getBrokers(), benchmarkConfig.getZKConnectionString());
  }

  @Override
  public Class<? extends BenchmarkTaskFactory> getTaskFactoryClass() {
    return ProducerTaskFactory.class;
  }


  @Override
  public void setup() {
    super.setup();
    validateTaskAllocation();

    if (!workloadGeneratorConfig.isReuseTopic() &&  areProduceTopicsExists()) {
      throw new RuntimeException("Some produce topics already exists. Existing topics: " + Joiner.on(", ").join(kafkaAdmin.listTopics()));
    }

    Set<String> consumeTopics = getConsumeAndReplayTopicsNotInProduce();
    if (!consumeTopics.isEmpty()) {
      for (String topic : consumeTopics) {
        if (kafkaAdmin.isTopicExists(topic)) {
          throw new RuntimeException("Consume topic " + topic + " should be there in Kafka.");
        }
      }
    }

    createTopics(getProduceTopicConfigs());
  }

  @Override
  public void teardown() {
    super.teardown();
    // TODO: Delete topics if needed
  }

  @Override
  protected String getBenchName() {
    return benchName;
  }

  @Override
  public Map<String, String> configureTask(int taskId, BenchmarkDeploymentState deploymentState) {
    SyntheticWorkloadGeneratorDeploymentState.TaskDeploymentConfig deploymentConfig =
        ((SyntheticWorkloadGeneratorDeploymentState) deploymentState).nextTaskDeploymentConfig();

    Map<String, String> env = new HashMap<>();
    env.put(KafkaBenchmarkConstants.ENV_KAFKA_BENCH_BROKERS, workloadGeneratorConfig.getBrokers());
    env.put(KafkaBenchmarkConstants.ENV_KAFKA_BENCH_ZK, workloadGeneratorConfig.getZKConnectionString());
    env.putAll(deploymentConfig.getTaskEnvironment());

    return env;
  }

  private boolean areProduceTopicsExists() {
    for (String topic : workloadGeneratorConfig.getProduceTopics()) {
      if (kafkaAdmin.isTopicExists(topic)) {
        return true;
      }
    }

    return false;
  }

  private List<SyntheticWorkloadGeneratorConfig.TopicConfig> getProduceTopicConfigs() {
    List<SyntheticWorkloadGeneratorConfig.TopicConfig> topicConfigs = new ArrayList<>();

    for (String t : workloadGeneratorConfig.getProduceTopics()) {
      topicConfigs.add(workloadGeneratorConfig.getProduceTopicConfig(t));
    }

    return topicConfigs;
  }

  private void createTopics(List<SyntheticWorkloadGeneratorConfig.TopicConfig> topics) {
    for (SyntheticWorkloadGeneratorConfig.TopicConfig tc : topics) {
      if (!kafkaAdmin.isTopicExists(tc.getName())) {
        kafkaAdmin.createTopic(tc.getName(), tc.getPartitions(), tc.getReplicationFactor());
      }
    }
  }

  private Set<String> getConsumeAndReplayTopicsNotInProduce() {
    Set<String> consumeTopics = workloadGeneratorConfig.getConsumeTopics();
    consumeTopics.addAll(workloadGeneratorConfig.getReplayTopics());

    return Sets.difference(consumeTopics, workloadGeneratorConfig.getProduceTopics());
  }

  private void validateTaskAllocation() {
    int requiredTasks = 0;
    Set<String> produceTopics = workloadGeneratorConfig.getProduceTopics();
    Set<String> consumerTopics = workloadGeneratorConfig.getConsumeTopics();
    Set<String> replayTopics = workloadGeneratorConfig.getReplayTopics();

    for (String topic : produceTopics) {
      List<SyntheticWorkloadGeneratorConfig.ProducerGroupConfig> producerGroups =
          workloadGeneratorConfig.getProduceTopicConfig(topic).getProducerGroups();
      for (SyntheticWorkloadGeneratorConfig.ProducerGroupConfig p : producerGroups) {
        requiredTasks += p.getTaskCount();
      }
    }

    for (String topic : consumerTopics) {
      List<SyntheticWorkloadGeneratorConfig.ConsumerGroupConfig> consumerGroups =
          workloadGeneratorConfig.getConsumerTopicConfig(topic).getConsumerGroups();
      for (SyntheticWorkloadGeneratorConfig.ConsumerGroupConfig c : consumerGroups) {
        requiredTasks += c.getTaskCount();
      }
    }

    for (String topic : replayTopics) {
      List<SyntheticWorkloadGeneratorConfig.ConsumerGroupConfig> consumerGroups =
          workloadGeneratorConfig.getReplayTopicConfig(topic).getConsumerGroups();
      for (SyntheticWorkloadGeneratorConfig.ConsumerGroupConfig c : consumerGroups) {
        requiredTasks += c.getTaskCount();
      }
    }

    if (requiredTasks != new BenchConfig(workloadGeneratorConfig.getRawConfig()).getParallelism()) {
      throw new RuntimeException(String.format("Number of required tasks [%s] and allocated tasks [%s] does not match.", requiredTasks, new BenchConfig(workloadGeneratorConfig.getRawConfig()).getParallelism()));
    }
  }
}
