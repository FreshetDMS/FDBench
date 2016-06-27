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

public class KafkaProducerConfig extends KafkaBenchmarkConfig {
  private static final String KEY_SERIALIZER_CLASS = "key.serializer.class";
  private static final String KEY_DESERIALIZER_CLASS = "key.deserializer.class";
  private static final String VALUE_SERIALIZER_CLASS = "value.serializer.class";
  private static final String VALUE_DESERIALIZER_CLASS = "value.deserializer.class";
  private static final String REQUEST_REQUIRED_ACKS = "request.require.acks";
  private static final String MESSAGE_SIZE = "message.size";
  private static final String ALLOCATED_PARTITIONS = "allocated.partitions";

  public KafkaProducerConfig(Config config) {
    super(config);
  }

  public String getKeySerializerClass() {
    return getString(KEY_SERIALIZER_CLASS, "org.apache.kafka.common.serialization.ByteArraySerializer");
  }

  public String getValueSerializerClass() {
    return getString(VALUE_SERIALIZER_CLASS, "org.apache.kafka.common.serialization.ByteArraySerializer");
  }

  public String getKeyDeserializerClass() {
    return getString(KEY_DESERIALIZER_CLASS, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
  }

  public String getValueDeserializerClass() {
    return getString(VALUE_DESERIALIZER_CLASS, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
  }

  public String getRequestRequiredAcks() {
    return getString(REQUEST_REQUIRED_ACKS, "1");
  }

  public int getMessageSize() {
    return getInt(MESSAGE_SIZE, 100);
  }

  public String getAllocatedPartitions() {
    return getString(ALLOCATED_PARTITIONS, "0");
  }
}
