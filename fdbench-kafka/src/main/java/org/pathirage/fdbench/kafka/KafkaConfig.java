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
import org.pathirage.fdbench.config.AbstractConfig;

public abstract class KafkaConfig extends AbstractConfig {
  private static final String KAFKA_ZK_CONNECT = "kafka.zookeeper.connect";
  private static final String KAFKA_BROKERS = "kafka.brokers";
  private static final String KAFKA_KEY_SERIALIZER_CLASS = "kafka.key.serializer.class";
  private static final String KAFKA_KEY_DESERIALIZER_CLASS = "kafka.key.deserializer.class";
  private static final String KAFKA_VALUE_SERIALIZER_CLASS = "kafka.value.serializer.class";
  private static final String KAFKA_VALUE_DESERIALIZER_CLASS = "kafka.value.deserializer.class";
  private static final String KAFKA_REQUEST_REQUIRED_ACKS = "kafka.request.require.acks";

  public KafkaConfig(Config rawConfig) {
    super(rawConfig);
  }

  public String getZKConnectionString() {
    return getString(KAFKA_ZK_CONNECT);
  }

  public String getBrokers() {
    return getString(KAFKA_BROKERS);
  }

  public String getKeySerializerClass() {
    return getString(KAFKA_KEY_SERIALIZER_CLASS, "org.apache.kafka.common.serialization.ByteArraySerializer");
  }

  public String getValueSerializerClass() {
    return getString(KAFKA_VALUE_SERIALIZER_CLASS, "org.apache.kafka.common.serialization.ByteArraySerializer");
  }

  public String getKeyDeserializerClass() {
    return getString(KAFKA_KEY_DESERIALIZER_CLASS, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
  }

  public String getValueDeserializerClass() {
    return getString(KAFKA_VALUE_DESERIALIZER_CLASS, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
  }

  public String getRequestRequiredAcks() {
    return getString(KAFKA_REQUEST_REQUIRED_ACKS, "1");
  }

}
