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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.IntStream;

public abstract class KafkaBenchmark implements Benchmark {
  private static final Logger log = LoggerFactory.getLogger(KafkaBenchmark.class);

  private final KafkaBenchmarkConfig benchmarkConfig;
  private final KafkaAdmin kafkaAdmin;
  private final int parallelism;

  public KafkaBenchmark(KafkaBenchmarkConfig benchmarkConfig, int parallelism) {
    this.benchmarkConfig = benchmarkConfig;
    this.kafkaAdmin = new KafkaAdmin(benchmarkConfig.getBrokers(), benchmarkConfig.getZKConnectionString());
    this.parallelism = parallelism;
  }

  @Override
  public void setup() {
    String topic = benchmarkConfig.getTopic();
    int partitions = benchmarkConfig.getPartitionCount();
    int replicationFactor = benchmarkConfig.getReplicationFactor();
    // It looks like isTopicExists doesn't work as expected
//    if (kafkaAdmin.isTopicExists(topic)) {
//      log.warn("Topic " + topic +
//          " already exists. So deleting the existing topic (This only works with Kafka versions >= 0.9.0).");
//      kafkaAdmin.deleteTopic(topic);
//    }

    log.info("Creating topic " + topic + " with " + partitions +
        " partitions and replication factor " + replicationFactor);
    // TODO: Why we need to pass properties to createTopic
    kafkaAdmin.createTopic(topic, partitions, replicationFactor, new Properties());
  }

  @Override
  public void teardown() {
    String topic = benchmarkConfig.getTopic();
    if (kafkaAdmin.isTopicExists(topic)) {
      log.info("Deleting topic: " + topic);
      kafkaAdmin.deleteTopic(topic);
    } else {
      log.warn("Cannot find topic " + topic + ". May be something went wrong during benchmark setup.");
    }
  }

  @Override
  public Map<String, String> configureTask(int taskId) {
    Map<String, String> taskConfig = new HashMap<>();
    int partitionCount = benchmarkConfig.getPartitionCount();
    if (partitionCount % parallelism != 0) {
      String errMsg = String.format("Partition count (%s) be evenly divisible by task count (%s) for end-to-end " +
          "latency benchmark.", partitionCount, parallelism);
      log.error(errMsg);
      throw new FDBenchException(errMsg);
    }

    int partitionsPerTask = partitionCount / parallelism;
    int start = taskId * partitionsPerTask;
    int end = start + partitionsPerTask;

    Integer[] partitionAssignment = ArrayUtils.toObject(IntStream.range(start, end).toArray());

    taskConfig.put(Constants.ENV_PARTITIONS, Joiner.on(",").join(partitionAssignment));
    taskConfig.put(Constants.ENV_TOPIC, benchmarkConfig.getTopic());
    taskConfig.put(Constants.ENV_BROKERS, benchmarkConfig.getBrokers());
    taskConfig.put(Constants.ENV_ZK, benchmarkConfig.getZKConnectionString());

    return taskConfig;
  }
}
