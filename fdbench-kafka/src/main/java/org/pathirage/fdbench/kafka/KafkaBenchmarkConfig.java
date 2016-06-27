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
import org.pathirage.fdbench.messaging.config.AbstractConfig;

public class KafkaBenchmarkConfig extends AbstractConfig {

  public enum BenchType {
    PRODUCER,
    CONSUMER,
    LAG,
    LATENCY
  }

  private static final String PRODUCER_TOPIC = "kafka.producer.topic";
  private static final String TOPIC_PARTITIONS = "kafka.topic.partitions";
  private static final String TOPIC_REPLICATION = "kafka.topic.replication";
  private static final String CONSUMER_TOPIC = "kafka.consumer.topic";
  private static final String KAFKA_ZK_CONNECT = "kafka.zookeeper.connect";
  private static final String KAFKA_BROKERS = "kafka.brokers";
  private static final String BENCH_TYPE = "kafka.bench.type";

  public KafkaBenchmarkConfig(Config rawConfig) {
    super(rawConfig);
  }

  public String getZKConnectionString() {
    return getString(KAFKA_ZK_CONNECT);
  }

  public String getBrokers() {
    return getString(KAFKA_BROKERS);
  }

  public String getProducerTopic() {
    return getString(PRODUCER_TOPIC);
  }

  public String getConsumerTopic() {
    return getString(CONSUMER_TOPIC);
  }

  public int getPartitionCount() {
    return getInt(TOPIC_PARTITIONS, 1);
  }

  public int getReplication() {
    return getInt(TOPIC_REPLICATION, 1);
  }

  public BenchType getBenchType() {
    return BenchType.valueOf(getString(BENCH_TYPE, "PRODUCER"));
  }
}
