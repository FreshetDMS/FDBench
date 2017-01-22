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
import org.pathirage.fdbench.config.BenchConfig;
import org.pathirage.fdbench.kafka.KafkaBenchmark;
import org.pathirage.fdbench.kafka.KafkaBenchmarkConfig;

import java.util.*;

public class SyntheticWorkloadGenerator extends KafkaBenchmark {
  private final SyntheticWorkloadGeneratorConfig workloadGeneratorConfig;

  public SyntheticWorkloadGenerator(KafkaBenchmarkConfig benchmarkConfig, int parallelism) {
    super(benchmarkConfig, parallelism);
    workloadGeneratorConfig = (SyntheticWorkloadGeneratorConfig) benchmarkConfig;
  }

  @Override
  public Class<? extends BenchmarkTaskFactory> getTaskFactoryClass() {
    return ProducerTaskFactory.class;
  }

  @Override
  public boolean isValidPartitionCountAndParallelism(int partitionCount, int parallelism) {
    return true;
  }

  @Override
  public void setup() {
    validateTaskAllocation();

    if (areProduceTopicsExists()) {
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
  }

  @Override
  public Map<String, String> configureTask(int taskId, BenchmarkDeploymentState deploymentState) {
    // Tasks can read other configurations from config file. They just need to know the topic.
    Map<String, String> taskConfig = new HashMap<>();

    SyntheticWorkloadGeneratorDeploymentState.TaskDeploymentConfig deploymentConfig =
        ((SyntheticWorkloadGeneratorDeploymentState) deploymentState).nextTaskDeploymentConfig();

    taskConfig.put(Constants.FDBENCH_PARTITION_ASSIGNMENT, Joiner.on(",").join(deploymentConfig.getPartitions()));
    taskConfig.put(Constants.FDBENCH_TOPIC, deploymentConfig.getConfig().getName());
    taskConfig.put(Constants.FDBENCH_TASK_FACTORY_CLASS, deploymentConfig.getTaskFactoryClass());

    return taskConfig;
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
      kafkaAdmin.createTopic(tc.getName(), tc.getPartitions(), tc.getReplicationFactor());
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
      requiredTasks += workloadGeneratorConfig.getProduceTopicConfig(topic).getPublishers();
    }

    for (String topic : consumerTopics) {
      requiredTasks += workloadGeneratorConfig.getConsumerTopicConfig(topic).getConsumers();
    }

    for (String topic : replayTopics) {
      requiredTasks += workloadGeneratorConfig.getReplayTopicConfig(topic).getConsumers();
    }

    if (requiredTasks != new BenchConfig(workloadGeneratorConfig.getRawConfig()).getParallelism()) {
      throw new RuntimeException("Number of required tasks and allocated tasks does not match.");
    }
  }
}
