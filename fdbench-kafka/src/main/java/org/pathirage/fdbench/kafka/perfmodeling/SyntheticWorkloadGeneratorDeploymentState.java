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
import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import org.apache.commons.math3.util.Pair;
import org.pathirage.fdbench.api.BenchmarkDeploymentState;
import org.pathirage.fdbench.kafka.KafkaBenchmark;
import org.pathirage.fdbench.kafka.KafkaBenchmarkConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SyntheticWorkloadGeneratorDeploymentState implements BenchmarkDeploymentState {

  private static final Logger log = LoggerFactory.getLogger(SyntheticWorkloadGeneratorDeploymentState.class);

  private final Config rawConfig;
  private final SyntheticWorkloadGeneratorConfig workloadGeneratorConfig;
  private Deque<Pair<SyntheticWorkloadGeneratorConfig.WorkerGroupConfig,
      SyntheticWorkloadGeneratorConfig.TopicConfig>> workerGroups = new ArrayDeque<>();
  private int totalTasks = 0;
  private int configuredTaskCount = 0;
  private int currentWorkerGroupsTasks = 0;
  private int configuredTasksForCurrentWorkerGroup = 0;
  private int taskOfCurrentWorkerGroup;
  private Pair<SyntheticWorkloadGeneratorConfig.WorkerGroupConfig,
      SyntheticWorkloadGeneratorConfig.TopicConfig> current = null;
  private List<List<Integer>> currentTopicPartitionAssignment;

  public SyntheticWorkloadGeneratorDeploymentState(Config benchmarkConfig) {
    this.rawConfig = benchmarkConfig;
    this.workloadGeneratorConfig = new SyntheticWorkloadGeneratorConfig(rawConfig);
    init();
  }

  private void init() {
    Set<String> produceTopics = workloadGeneratorConfig.getProduceTopics();
    Set<String> consumerTopics = workloadGeneratorConfig.getConsumeTopics();
    Set<String> replayTopics = workloadGeneratorConfig.getReplayTopics();

    for (String topic : produceTopics) {
      SyntheticWorkloadGeneratorConfig.TopicConfig topicConfig = workloadGeneratorConfig.getProduceTopicConfig(topic);
      List<SyntheticWorkloadGeneratorConfig.ProducerGroupConfig> producerGroups =
          topicConfig.getProducerGroups();
      for (SyntheticWorkloadGeneratorConfig.ProducerGroupConfig p : producerGroups) {
        totalTasks += p.getTaskCount();
        workerGroups.add(new Pair<>(p, topicConfig));
      }
    }

    for (String topic : consumerTopics) {
      SyntheticWorkloadGeneratorConfig.TopicConfig topicConfig = workloadGeneratorConfig.getConsumerTopicConfig(topic);
      List<SyntheticWorkloadGeneratorConfig.ConsumerGroupConfig> consumerGroups =
          topicConfig.getConsumerGroups();
      for (SyntheticWorkloadGeneratorConfig.ConsumerGroupConfig c : consumerGroups) {
        totalTasks += c.getTaskCount();
        workerGroups.add(new Pair<>(c, topicConfig));
      }
    }

    for (String topic : replayTopics) {
      SyntheticWorkloadGeneratorConfig.TopicConfig topicConfig = workloadGeneratorConfig.getReplayTopicConfig(topic);
      List<SyntheticWorkloadGeneratorConfig.ConsumerGroupConfig> consumerGroups =
          topicConfig.getConsumerGroups();
      for (SyntheticWorkloadGeneratorConfig.ConsumerGroupConfig c : consumerGroups) {
        totalTasks += c.getTaskCount();
        workerGroups.add(new Pair<>(c, topicConfig));
      }
    }
  }

  public synchronized TaskDeploymentConfig nextTaskDeploymentConfig() {

    if (totalTasks == configuredTaskCount) {
      throw new RuntimeException("No more tasks to deploy.");
    }

    if (current == null || currentWorkerGroupsTasks == configuredTasksForCurrentWorkerGroup) {
      current = workerGroups.pop();
      currentWorkerGroupsTasks = current.getFirst().getTaskCount();
      configuredTasksForCurrentWorkerGroup = 0;
      taskOfCurrentWorkerGroup = 0;
      int topicPartitions = current.getSecond().getPartitions();
      if (topicPartitions % currentWorkerGroupsTasks != 0) {
        log.warn("Topic " + current.getSecond().getName() + " partitions (" + topicPartitions + " ) will not be " +
            "evenly divided across tasks (" + currentWorkerGroupsTasks + ")");
      }

      currentTopicPartitionAssignment = Lists.partition(
          IntStream.range(0, topicPartitions).boxed().collect(Collectors.toList()),
          topicPartitions / currentWorkerGroupsTasks);
    }
    List<Integer> partitionAssignment = currentTopicPartitionAssignment.get(taskOfCurrentWorkerGroup);
    TaskDeploymentConfig deploymentConfig = new TaskDeploymentConfig(
        partitionAssignment.<Integer>toArray(new Integer[partitionAssignment.size()]),
        current.getSecond(),
        getTaskFactory(current.getSecond().getType()),
        current.getFirst());

    configuredTaskCount++;
    taskOfCurrentWorkerGroup++;

    return deploymentConfig;
  }

  private String getTaskFactory(SyntheticWorkloadGeneratorConfig.TopicConfig.Type type) {
    switch (type) {
      case CONSUME:
        return "org.pathirage.fdbench.kafka.perfmodeling.ConsumerTaskFactory";
      case PRODUCE:
        return "org.pathirage.fdbench.kafka.perfmodeling.ProducerTaskFactory";
      case REPLAY:
        return "org.pathirage.fdbench.kafka.perfmodeling.ConsumerTaskFactory";
      default:
        throw new RuntimeException("Unsupported task type: " + type);
    }
  }

  public static class TaskDeploymentConfig {
    private final Integer[] partitions;
    private final SyntheticWorkloadGeneratorConfig.TopicConfig config;
    private final String taskFactoryClass;
    private final SyntheticWorkloadGeneratorConfig.WorkerGroupConfig workerGroupConfig;

    public TaskDeploymentConfig(Integer[] partitions, SyntheticWorkloadGeneratorConfig.TopicConfig config,
                                String taskFactoryClass,
                                SyntheticWorkloadGeneratorConfig.WorkerGroupConfig workerGroupConfig) {
      this.partitions = partitions;
      this.config = config;
      this.taskFactoryClass = taskFactoryClass;
      this.workerGroupConfig = workerGroupConfig;
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

    public Map<String, String> getTaskEnvironment() {
      Map<String, String> env = new HashMap<>();

      env.put(KafkaBenchmarkConstants.ENV_KAFKA_BENCH_TOPIC, config.getName());
      env.put(KafkaBenchmarkConstants.ENV_KAFKA_BENCH_PARTITIONS, Joiner.on(", ").join(partitions));
      env.put(SyntheticWorkloadGeneratorConstants.ENV_KAFKA_WORKLOAD_GENERATOR_TASK_TYPE, workerGroupConfig.getGroupType().name());
      env.put(SyntheticWorkloadGeneratorConstants.ENV_KAFKA_WORKLOAD_GENERATOR_TASK_GROUP, workerGroupConfig.getName());
      env.put(SyntheticWorkloadGeneratorConstants.ENV_KAFKA_WORKLOAD_GENERATOR_TASK_DELAY, Integer.toString(workerGroupConfig.getDelaySecs()));
      env.put(SyntheticWorkloadGeneratorConstants.ENV_KAFKA_WORKLOAD_GENERATOR_MESSAGE_RATE, Integer.toString(workerGroupConfig.getMessageRate()));


      switch (workerGroupConfig.getGroupType()) {
        case PRODUCE:
          env.put(SyntheticWorkloadGeneratorConstants.ENV_KAFKA_WORKLOAD_GENERATOR_MSG_SIZE_MEAN, Integer.toString(config.getMessageSizeConfig().mean()));
          break;
        case CONSUME:
        case REPLAY:
          SyntheticWorkloadGeneratorConfig.ConsumerGroupConfig consumerGroupConfig = (SyntheticWorkloadGeneratorConfig.ConsumerGroupConfig)workerGroupConfig;
          env.put(SyntheticWorkloadGeneratorConstants.ENV_KAFKA_WORKLOAD_GENERATOR_MSG_PROC_MEAN, Integer.toString(consumerGroupConfig.getMessageProcessingConfig().mean()));
          env.put(SyntheticWorkloadGeneratorConstants.ENV_KAFKA_WORKLOAD_GENERATOR_MSG_PROC_STDEV, Integer.toString(consumerGroupConfig.getMessageProcessingConfig().std()));
          break;
        default:
          break;
      }

      return env;
    }
  }
}
