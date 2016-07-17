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

import kafka.admin.AdminUtils;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Properties;

public class KafkaClusterVerifier {
  private static final Logger log = LoggerFactory.getLogger(KafkaClusterVerifier.class);

  private final KafkaProducer<String, String> producer;
  private final KafkaConsumer<String, String> consumer;
  private final ZkUtils zkUtils;

  private static final String TOPIC = "test-topic-creation7";

  public KafkaClusterVerifier(String kafkaBrokers, String zkConnectionStr) {
    this.producer = new KafkaProducer<String, String>(getProducerProperties(kafkaBrokers));
    this.consumer = new KafkaConsumer<String, String>(Utils.getConsumerProperties(kafkaBrokers, "kafka-cluster-validator"));
    this.zkUtils = createZkUtils(zkConnectionStr);
  }

  private ZkUtils createZkUtils(String zkConnectionStr) {
    ZkConnection zkConnection = new ZkConnection(zkConnectionStr);
    ZkClient zkClient = new ZkClient(zkConnection, 30000, new ZKStringSerializer());
    return new ZkUtils(zkClient, zkConnection, false);
  }

  public void verify() {
    verifyTopicCreation();
    verifyProducing();
  }

  public void verifyProducing() {
    producer.send(new ProducerRecord<String, String>(TOPIC, "rkey", "rvalue"),
        new Callback() {
          @Override
          public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (exception != null) {
              throw new RuntimeException("Kafka cluster verification failed.");
            }
            log.info("Message sent successfully. Topic: " + metadata.topic() + " Offset: " + metadata.offset() +
                " Partition: " + metadata.partition());
          }
        });
    producer.close();
  }

  public void verifyTopicCreation() {
    AdminUtils.createTopic(zkUtils, TOPIC, 2, 1, new Properties());
    Map<String, List<PartitionInfo>> topics = consumer.listTopics();

    for (String topc : topics.keySet()) {
      System.out.println("Topic: " + topc);
    }

    if (!topics.containsKey("test-topic-creation")) {
      throw new RuntimeException("Cannot find the topic created.");
    }
  }

  private Properties getProducerProperties(String kafkaBrokers) {
    Properties producerProps = new Properties();

    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers);
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, "cluster-validator");

    return producerProps;
  }

  public static void main(String[] args) {
    new KafkaClusterVerifier("localhost:9092", "localhost:2181").verify();
  }
}
