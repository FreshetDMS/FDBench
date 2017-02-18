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

import com.google.common.base.Joiner;
import org.apache.commons.lang3.ArrayUtils;
import org.pathirage.fdbench.FDBenchException;
import org.pathirage.fdbench.api.Benchmark;
import org.pathirage.fdbench.api.BenchmarkDeploymentState;
import org.pathirage.fdbench.config.BenchConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

public abstract class KafkaBenchmark implements Benchmark {
  private static final Logger log = LoggerFactory.getLogger(KafkaBenchmark.class);

  private final KafkaBenchmarkConfig benchmarkConfig;
  protected final KafkaAdmin kafkaAdmin;
  private final int parallelism;

  public KafkaBenchmark(KafkaBenchmarkConfig benchmarkConfig, int parallelism) {
    this.benchmarkConfig = benchmarkConfig;
    this.kafkaAdmin = new KafkaAdmin(new BenchConfig(benchmarkConfig.getRawConfig()).getName(),
        benchmarkConfig.getBrokers(), benchmarkConfig.getZKConnectionString());
    this.parallelism = parallelism;
  }

  @Override
  public void setup() {
    String topic = benchmarkConfig.getTopic();
    int partitions = benchmarkConfig.getPartitionCount();
    int replicationFactor = benchmarkConfig.getReplicationFactor();
    if (kafkaAdmin.isTopicExists(topic) && !benchmarkConfig.isReuseTopic()) {
      throw new RuntimeException("Topic reuse is disabled and topic " + topic + " already exists.");
    }

    if (benchmarkConfig.isReuseTopic() && !kafkaAdmin.isTopicExists(topic)) {
      throw new RuntimeException("Topic reuse is enabled. But the topic " + topic + " does not exist.");
    }

    if (benchmarkConfig.isReuseTopic() && !kafkaAdmin.isPartitionCountAndReplicationFactorMatch(topic, partitions, replicationFactor)) {
      throw new RuntimeException("Topic reuse is enabled. But existing topic's partition count " +
          " and replication factor does not match.");
    }

    if (!benchmarkConfig.isReuseTopic()) {
      log.info("Creating topic " + topic + " with " + partitions +
          " partitions and replication factor " + replicationFactor);
      kafkaAdmin.createTopic(topic, partitions, replicationFactor);
    }
  }

  @Override
  public void teardown() {
    String topic = benchmarkConfig.getTopic();
    if (kafkaAdmin.isTopicExists(topic) && benchmarkConfig.isDeleteTopic()) {
      log.info("Deleting topic: " + topic);
      kafkaAdmin.deleteTopic(topic);
    }
  }

  public abstract boolean isValidPartitionCountAndParallelism(int partitionCount, int parallelism);

  @Override
  public Map<String, String> configureTask(int taskId, BenchmarkDeploymentState deploymentState) {
    Map<String, String> taskConfig = new HashMap<>();
    int partitionCount = benchmarkConfig.getPartitionCount();
    if (!isValidPartitionCountAndParallelism(partitionCount, parallelism)) {
      String errMsg = String.format("Partition count (%d) and parallelism (%d) don't go together for this benchmark.", partitionCount, parallelism);
      log.error(errMsg);
      throw new FDBenchException(errMsg);
    }

    int partitionsPerTask = partitionCount / parallelism;
    int start = taskId * partitionsPerTask;
    int end = start + partitionsPerTask;

    Integer[] partitionAssignment = ArrayUtils.toObject(IntStream.range(start, end).toArray());

    taskConfig.put(KafkaBenchmarkConstants.ENV_KAFKA_BENCH_PARTITIONS, Joiner.on(",").join(partitionAssignment));
    taskConfig.put(KafkaBenchmarkConstants.ENV_KAFKA_BENCH_TOPIC, benchmarkConfig.getTopic());
    taskConfig.put(KafkaBenchmarkConstants.ENV_KAFKA_BENCH_BROKERS, benchmarkConfig.getBrokers());
    taskConfig.put(KafkaBenchmarkConstants.ENV_KAFKA_BENCH_ZK, benchmarkConfig.getZKConnectionString());

    return taskConfig;
  }
}
