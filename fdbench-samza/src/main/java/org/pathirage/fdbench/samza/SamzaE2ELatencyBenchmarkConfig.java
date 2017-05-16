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

package org.pathirage.fdbench.samza;

import com.typesafe.config.Config;
import org.pathirage.fdbench.config.AbstractConfig;
import org.pathirage.fdbench.kafka.KafkaConfig;

public class SamzaE2ELatencyBenchmarkConfig extends KafkaConfig {

  private static final String SOURCE_TOPIC = "samza.e2elatency.source.topic";
  private static final String RESULT_TOPIC = "samza.e2elatency.result.topic";
  private static final String MESSAGE_RATE = "samza.e2elatency.message.rate";
  private static final String PAYLOAD_SIZE = "samza.e2elatency.payload.size";

  public SamzaE2ELatencyBenchmarkConfig(Config rawConfig) {
    super(rawConfig);
  }

  public TopicConfig getSource() {
    return new TopicConfig(getConfig(SOURCE_TOPIC));
  }

  public TopicConfig getResult() {
    return new TopicConfig(getConfig(RESULT_TOPIC));
  }

  public int getMessageRate() {
    return getInt(MESSAGE_RATE, 1000);
  }

  public int getPayloadSize() {
    return getInt(PAYLOAD_SIZE, 100);
  }

  public static class TopicConfig extends AbstractConfig {

    private static final String NAME = "name";
    private static final String PARTITIONS = "partitions";
    private static final String REPLICATION = "replication";


    public TopicConfig(Config config) {
      super(config);
    }

    public String getName() {
      return getString(NAME);
    }

    public Integer getPartitions() {
      return getInt(PARTITIONS, 1);
    }

    public Integer getReplicationFactor() {
      return getInt(REPLICATION, 1);
    }
  }
}
