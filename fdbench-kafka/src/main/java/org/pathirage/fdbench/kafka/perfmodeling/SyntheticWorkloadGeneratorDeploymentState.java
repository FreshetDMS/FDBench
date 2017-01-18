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

import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import org.pathirage.fdbench.api.BenchmarkDeploymentState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SyntheticWorkloadGeneratorDeploymentState implements BenchmarkDeploymentState {

  private static final Logger log = LoggerFactory.getLogger(SyntheticWorkloadGeneratorDeploymentState.class);

  private final Config rawConfig;
  private final SyntheticWorkloadGeneratorConfig workloadGeneratorConfig;
  private Deque<SyntheticWorkloadGeneratorConfig.TopicConfig> topics = new ArrayDeque<>();
  private int totalTasks = 0;
  private int configuredTaskCount = 0;
  private int currentTopicsTasks = 0;
  private int configuredTasksForCurrentTopics = 0;
  private int taskOfCurrentTopic;
  private SyntheticWorkloadGeneratorConfig.TopicConfig current = null;
  private List<List<Integer>> currentTopicPartitionAssignment;

  public SyntheticWorkloadGeneratorDeploymentState(Config benchmarkConfig) {
    this.rawConfig = benchmarkConfig;
    this.workloadGeneratorConfig = new SyntheticWorkloadGeneratorConfig(rawConfig);
    init();
  }

  private void init() {
    for (String topic : workloadGeneratorConfig.getProduceTopics()) {
      SyntheticWorkloadGeneratorConfig.TopicConfig tc = workloadGeneratorConfig.getProduceTopicConfig(topic);
      totalTasks += tc.getPublishers();
      topics.add(tc);
    }

    for (String topic : workloadGeneratorConfig.getConsumeTopics()) {
      SyntheticWorkloadGeneratorConfig.TopicConfig tc = workloadGeneratorConfig.getConsumerTopicConfig(topic);
      totalTasks += tc.getConsumers();
      topics.add(tc);
    }

    for (String topic : workloadGeneratorConfig.getReplayTopics()) {
      SyntheticWorkloadGeneratorConfig.TopicConfig tc = workloadGeneratorConfig.getReplayTopicConfig(topic);
      totalTasks += tc.getConsumers();
      topics.add(tc);
    }
  }

  public synchronized TaskDeploymentConfig nextTaskDeploymentConfig() {

    if (totalTasks == configuredTaskCount) {
      throw new RuntimeException("No more tasks to deploy.");
    }

    if (current == null || currentTopicsTasks == configuredTasksForCurrentTopics) {
      current = topics.pop();
      currentTopicsTasks = current.getTasks();
      configuredTasksForCurrentTopics = 0;
      taskOfCurrentTopic = 0;
      if (current.getPartitions() % currentTopicsTasks != 0) {
        log.warn("Topic " + current.getName() + " partitions (" + current.getPartitions() + " ) will not be " +
            "evenly divided across tasks (" + currentTopicsTasks + ")");
      }

      currentTopicPartitionAssignment = Lists.partition(
          IntStream.rangeClosed(1, current.getPartitions()).boxed().collect(Collectors.toList()),
          current.getPartitions() / currentTopicsTasks);
    }
    List<Integer> partitionAssignment = currentTopicPartitionAssignment.get(taskOfCurrentTopic);
    TaskDeploymentConfig deploymentConfig = new TaskDeploymentConfig(
        partitionAssignment.<Integer>toArray(new Integer[partitionAssignment.size()]),
        current,
        getTaskFactory(current.getType()));

    configuredTaskCount++;
    taskOfCurrentTopic++;

    return deploymentConfig;
  }

  private String getTaskFactory(SyntheticWorkloadGeneratorConfig.TopicConfig.Type type) {
    switch (type) {
      case CONSUME:
        return "";
      case PRODUCE:
        return "";
      case REPLAY:
        return "";
      default:
        throw new RuntimeException("Unsupported task type: " + type);
    }
  }

  public static class TaskDeploymentConfig {
    private final Integer[] partitions;
    private final SyntheticWorkloadGeneratorConfig.TopicConfig config;
    private final String taskFactoryClass;

    public TaskDeploymentConfig(Integer[] partitions, SyntheticWorkloadGeneratorConfig.TopicConfig config, String taskFactoryClass) {
      this.partitions = partitions;
      this.config = config;
      this.taskFactoryClass = taskFactoryClass;
    }

    public Integer[] getPartitions() {
      return partitions;
    }

    public SyntheticWorkloadGeneratorConfig.TopicConfig getConfig() {
      return config;
    }

    public String getTaskFactoryClass() {
      return taskFactoryClass;
    }
  }
}
