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

import com.typesafe.config.Config;
import org.apache.commons.lang.StringUtils;
import org.pathirage.fdbench.messaging.FDMessagingBenchException;
import org.pathirage.fdbench.messaging.api.BenchmarkConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class KafkaBenchmarkConfigurator implements BenchmarkConfigurator {
  private static final Logger log = LoggerFactory.getLogger(KafkaBenchmarkConfigurator.class);
  protected static final String ENV_PARTITION_ASSIGNMENT = "partition.assignment";
  private final int parallelism;
  private final KafkaBenchmarkConfig config;
  private final KafkaUtils kafkaUtils;
  private int partitionCount;
  private final Map<Integer, List<Integer>> partitionMap = new HashMap<>();

  public KafkaBenchmarkConfigurator(int parallelism, Config rawConfig) {
    this.parallelism = parallelism;
    this.config = new KafkaBenchmarkConfig(rawConfig);
    this.kafkaUtils = new KafkaUtils(config.getBrokers(), config.getZKConnectionString());
  }

  @Override
  public void configureBenchmark() {
    KafkaBenchmarkConfig.BenchType type = config.getBenchType();

    if (type == KafkaBenchmarkConfig.BenchType.PRODUCER) {
      if (kafkaUtils.isTopicExists(config.getProducerTopic())) {
        log.info("Deleting the kafka existing kafka topic " + config.getProducerTopic());
        kafkaUtils.deleteTopic(config.getProducerTopic());
      }

      log.info("Creating a new kafka topic " + config.getProducerTopic());
      kafkaUtils.createTopic(config.getProducerTopic(), config.getPartitionCount(), config.getReplication(), null);
      this.partitionCount = kafkaUtils.getPartitionCount(config.getProducerTopic());
      mapPartitionsToContainer();

    } else {
      throw new FDMessagingBenchException("Doesn't support Kafka benchmark type " + type + " yet.");
    }
  }

  private void mapPartitionsToContainer() {
    for (int i = 0; i < partitionCount; i++) {
      int container = partitionCount % parallelism;
      if (!partitionMap.containsKey(container)) {
        partitionMap.put(container, new ArrayList<>());
      }

      partitionMap.get(container).add(i);
    }
  }

  @Override
  public Map<String, String> configureTask(int taskId) {
    return Collections.singletonMap(ENV_PARTITION_ASSIGNMENT, StringUtils.join(partitionMap.get(taskId), ","));

  }
}
