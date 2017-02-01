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

package org.pathirage.fdbench.tools;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

public class SimpleKafkaProducer {

  private static Random random = new Random(System.currentTimeMillis());

  public static void main(String[] args) {
    KafkaProducer<byte[], byte[]> kafkaProducer = new KafkaProducer<byte[], byte[]>(getProducerProperties());

    for (int i = 0; i < 100000; i++) {
      kafkaProducer.send(new ProducerRecord<byte[], byte[]>("test1", generateRandomMessage(200)));
    }
  }

  private static Properties getProducerProperties() {
    Properties producerProps = new Properties();

    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "Milindas-iMac:9092");
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
    producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, "simple-producer");

    return producerProps;
  }

  private static byte[] generateRandomMessage(int size) {
    byte[] payload = new byte[size];

    for (int i = 0; i < payload.length; i++) {
      payload[i] = (byte) (random.nextInt(26) + 65);
    }

    return payload;
  }
}
