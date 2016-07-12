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

import java.util.Random;

public abstract class KafkaBenchmarkConfig extends KafkaConfig {

  private static final String TOPIC = "kafka.bench.topic";
  private static final String TOPIC_PARTITIONS = "kafka.bench.partitions";
  private static final String TOPIC_REPLICATION = "kafka.bench.replication.factor";
  private static final String MESSAGE_SIZE = "kafka.bench.message.size";
  private static final String MESSAGE_RATE = "kafka.bench.message.rate";
  private static final String DURATION = "kafka.bench.duration";
  private static final String RECORD_LIMIT = "kafka.bench.records";
  private final Random random = new Random(System.currentTimeMillis());

  public KafkaBenchmarkConfig(Config rawConfig) {
    super(rawConfig);
  }

  public String getTopic() {
    return getString(TOPIC, "kafkabenchtopic" + random.nextInt(1000));
  }

  public int getPartitionCount() {
    return getInt(TOPIC_PARTITIONS, 1);
  }

  public int getReplicationFactor() {
    return getInt(TOPIC_REPLICATION, 1);
  }

  public int getMessageSize() {
    return getInt(MESSAGE_SIZE, 100);
  }

  public int getMessageRate() {
    return getInt(MESSAGE_RATE, -1);
  }

  public int getDurationSeconds() {
    return getInt(DURATION, 120);
  }

  public int getRecordLimit() {
    return getInt(RECORD_LIMIT, -1);
  }
}
