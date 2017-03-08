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

import com.typesafe.config.Config;
import com.typesafe.config.ConfigObject;
import com.typesafe.config.ConfigValue;
import org.pathirage.fdbench.config.AbstractConfig;
import org.pathirage.fdbench.kafka.KafkaBenchmarkConfig;

import java.util.*;

public class SyntheticWorkloadGeneratorConfig extends KafkaBenchmarkConfig {
  private static final String PRODUCE_TOPICS = "workload.produce";
  private static final String CONSUME_TOPICS = "workload.consume";
  private static final String REPLAY_TOPICS = "workload.replay";

  public SyntheticWorkloadGeneratorConfig(Config rawConfig) {
    super(rawConfig);
  }

  public Set<String> getProduceTopics() {
    return getTopics(PRODUCE_TOPICS);
  }

  public Set<String> getConsumeTopics() {
    return getTopics(CONSUME_TOPICS);
  }

  public Set<String> getReplayTopics() {
    return getTopics(REPLAY_TOPICS);
  }

  private Set<String> getTopics(String path) {
    if (hasPath(path)) {
      Config topicConfig = getConfig(path);
      Set<Map.Entry<String, ConfigValue>> topicConfigEntries = topicConfig.root().entrySet();
      Set<String> topics = new HashSet<>();
      for (Map.Entry<String, ConfigValue> e : topicConfigEntries) {
        // This can return incorrect topic list if we decided to add extra configurations under workload.produce
        topics.add(e.getKey());
      }

      return topics;
    }

    return Collections.emptySet();
  }

  public TopicConfig getProduceTopicConfig(String topic) {
    return new TopicConfig(topic, TopicConfig.Type.PRODUCE, getConfig(String.format("workload.produce.%s", topic)));
  }

  public TopicConfig getConsumerTopicConfig(String topic) {
    return new TopicConfig(topic, TopicConfig.Type.CONSUME, getConfig(String.format("workload.consume.%s", topic)));
  }

  public TopicConfig getReplayTopicConfig(String topic) {
    return new TopicConfig(topic, TopicConfig.Type.REPLAY, getConfig(String.format("workload.replay.%s", topic)));
  }

  public static class TopicConfig extends AbstractConfig {

    public enum Type {
      PRODUCE,
      CONSUME,
      REPLAY
    }

    private final String name;
    private final Type type;

    public TopicConfig(String name, Type type, Config config) {
      super(config);
      this.name = name;
      this.type = type;
    }

    public String getName() {
      return name;
    }

    public Type getType() {
      return type;
    }

    public int getPartitions() {
      return getInt("partitions", 1);
    }

    public int getReplicationFactor() {
      return getInt("replication-factor", 1);
    }

    public MessageSizeConfig getMessageSizeConfig() {
      return new MessageSizeConfig(getConfig("msg-size"));
    }

    public List<ConsumerGroupConfig> getConsumerGroups() {
      if (hasPath("consumers")) {
        List<ConsumerGroupConfig> consumerGroups = new ArrayList<>();
        Config consumers = getConfig("consumers");
        Set<Map.Entry<String, ConfigValue>> consumerEntries = consumers.root().entrySet();

        for (Map.Entry<String, ConfigValue> c : consumerEntries) {
          consumerGroups.add(new ConsumerGroupConfig(c.getKey(), ((ConfigObject) c.getValue()).toConfig(), false));
        }

        return consumerGroups;
      }
      return Collections.emptyList();
    }

    public List<ProducerGroupConfig> getProducerGroups() {
      if (hasPath("producers")) {
        List<ProducerGroupConfig> producerGroups = new ArrayList<>();
        Config producers = getConfig("producers");
        Set<Map.Entry<String, ConfigValue>> producerEntries = producers.root().entrySet();

        for (Map.Entry<String, ConfigValue> c : producerEntries) {
          producerGroups.add(new ProducerGroupConfig(c.getKey(), ((ConfigObject) c.getValue()).toConfig()));
        }

        return producerGroups;
      }
      return Collections.emptyList();
    }

    public List<ConsumerGroupConfig> getReplayGroups() {
      if (hasPath("replays")) {
        List<ConsumerGroupConfig> replayGroups = new ArrayList<>();
        Config replays = getConfig("replays");
        Set<Map.Entry<String, ConfigValue>> replayEntries = replays.root().entrySet();

        for (Map.Entry<String, ConfigValue> c : replayEntries) {
          replayGroups.add(new ConsumerGroupConfig(c.getKey(), ((ConfigObject) c.getValue()).toConfig(), true));
        }

        return replayGroups;
      }
      return Collections.emptyList();
    }
  }

  public abstract static class WorkerGroupConfig extends AbstractConfig {
    private static final String CONFIG_TASKS = "tasks";
    private static final String CONFIG_DELAY = "delay";
    private static final String CONFIG_RATE = "rate";
    private static final String CONFIG_USE_ALL_PARTITIONS = "use-all-partitions";

    private final Config config;
    private final String name;

    public WorkerGroupConfig(String name, Config config) {
      super(config);
      this.name = name;
      this.config = config;
    }

    public int getTaskCount() {
      return getInt(CONFIG_TASKS, 1);
    }

    public int getDelaySecs() {
      return getInt(CONFIG_DELAY, 0);
    }

    public String getName() {
      return name;
    }

    public int getMessageRate() {
      return getInt(CONFIG_RATE, 1000);
    }

    public boolean isUseAllPartitions(){
      return getBool(CONFIG_USE_ALL_PARTITIONS, false);
    }

    public abstract TopicConfig.Type getGroupType();
  }

  public static class ProducerGroupConfig extends WorkerGroupConfig {
    public ProducerGroupConfig(String name, Config config) {
      super(name, config);
    }

    @Override
    public TopicConfig.Type getGroupType() {
      return TopicConfig.Type.PRODUCE;
    }
  }

  public static class ConsumerGroupConfig extends WorkerGroupConfig {
    private final boolean replay;

    public ConsumerGroupConfig(String name, Config config, boolean replay) {
      super(name, config);
      this.replay = replay;
    }

    public MessageProcessingConfig getMessageProcessingConfig() {
      return new MessageProcessingConfig(getConfig("msg-processing"));
    }

    @Override
    public TopicConfig.Type getGroupType() {
      return replay ? TopicConfig.Type.REPLAY : TopicConfig.Type.CONSUME;
    }
  }

  public enum ProbabilityDistribution {
    LOGNORMAL,
    NORMAL,
    CHISQUARED,
    CONSTANT,
    EXPONENTIAL
  }

  public static class MessageSizeConfig extends AbstractConfig {

    public MessageSizeConfig(Config config) {
      super(config);
    }

    public ProbabilityDistribution getMessageSizeDistribution() {
      return ProbabilityDistribution.valueOf(getString("dist", "LOGNORMAL"));
    }

    public int mean() {
      return getInt("mean", 123);
    }

    public int std() {
      return getInt("std", 7);
    }
  }

  public static class MessageProcessingConfig extends AbstractConfig {
    public MessageProcessingConfig(Config config) {
      super(config);
    }

    public ProbabilityDistribution getMessageProcessingTimeDistribution() {
      return ProbabilityDistribution.valueOf(getString("dist", "CONSTANT"));
    }

    public int mean() {
      return getInt("mean", 123);
    }

    public int std() {
      return getInt("std", 7);
    }
  }
}
