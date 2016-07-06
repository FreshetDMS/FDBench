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

package org.pathirage.fdbench.kafka.e2elatency;

import com.typesafe.config.Config;
import org.pathirage.fdbench.kafka.KafkaConfig;

public class E2ELatencyBenchmarkConfig extends KafkaConfig {
  private static final String KAFKA_E2EBENCH_TOPIC = "kafka.e2ebench.topic.name";
  private static final String KAFKA_E2EBENCH_TOPIC_PARTITIONS = "kafka.e2ebench.topic.partitions";
  private static final String KAFKA_E2EBENCH_TOPIC_REPLICATION_FACTOR = "kafka.e2ebench.topic.replication.factor";
  private static final String KAFKA_E2EBENCH_MESSAGE_SIZE = "kafka.e2ebench.message.size";
  private static final String KAFKA_E2EBENCH_MESSAGE_RATE = "kafka.e2ebench.message.rate";
  private static final String KAFKA_E2EBENCH_DURATION = "kafka.e2ebench.duration";

  public E2ELatencyBenchmarkConfig(Config rawConfig) {
    super(rawConfig);
  }

  public String getTopic() {
    return getString(KAFKA_E2EBENCH_TOPIC, "e2ebenchtopic");
  }

  public int getPartitionCount() {
    return getInt(KAFKA_E2EBENCH_TOPIC_PARTITIONS, 1);
  }

  public int getReplicationFactor() {
    return getInt(KAFKA_E2EBENCH_TOPIC_REPLICATION_FACTOR, 1);
  }

  public int getMessageSize() {
    return getInt(KAFKA_E2EBENCH_MESSAGE_SIZE, 100);
  }

  public int getMessageRate() {
    return getInt(KAFKA_E2EBENCH_MESSAGE_RATE, 1000);
  }

  public int getDurationSeconds() {
    return getInt(KAFKA_E2EBENCH_DURATION, 120);
  }
}
