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
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.pathirage.fdbench.core.LatencyBenchmark;
import org.pathirage.fdbench.FDBenchException;

import java.util.Collections;
import java.util.Properties;
import java.util.Random;

/**
 *
 */
public class KafkaRequestGenerator implements LatencyBenchmark.RequestGenerator {
  private final KafkaProducerConfig config;
  private final int taskId;
  private KafkaProducer<byte[], byte[]> producer;
  private KafkaConsumer<byte[], byte[]> consumer;


  public KafkaRequestGenerator(Config config, int taskId) {
    this.config = new KafkaProducerConfig(config);
    this.taskId = taskId;
  }

  @Override
  public void setup() throws Exception {
    this.producer = new KafkaProducer<>(getProducerProperties());
    this.consumer = new KafkaConsumer<>(getConsumerProperties());
    this.consumer.subscribe(Collections.singletonList(config.getProducerTopic()));
  }

  @Override
  public void request() throws Exception {
    byte[] payload = new byte[config.getMessageSize()];
    Random random = new Random(0);
    for (int i = 0; i < payload.length; ++i)
      payload[i] = (byte) (random.nextInt(26) + 65);

    producer.send(new ProducerRecord<>(config.getProducerTopic(), payload)).get();

    ConsumerRecords<byte[], byte[]> records = consumer.poll(30000);
    if(records.isEmpty()) {
      throw new FDBenchException("Didn't receive any messages");
    }
  }

  @Override
  public void shutdown() throws Exception {
    if (producer != null) {
      producer.close();
    }

    if(consumer != null) {
      consumer.close();
    }
  }

  private Properties getConsumerProperties() {
    Properties consumerProps = new Properties();

    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBrokers());
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
    consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    consumerProps.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
    consumerProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, config.getKeyDeserializerClass());
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, config.getValueDeserializerClass());

    return consumerProps;
  }

  private Properties getProducerProperties() {
    Properties producerProps = new Properties();

    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBrokers());
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, config.getKeySerializerClass());
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, config.getValueSerializerClass());
    producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, "kafka-request-gen-" + taskId);

    return producerProps;
  }
}
