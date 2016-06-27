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

import kafka.utils.ZkUtils;
import kafka.admin.AdminUtils;
import org.I0Itec.zkclient.ZkClient;
import org.pathirage.fdbench.FDBenchException;
import scala.collection.Map;
import scala.collection.Seq;

import java.util.Collections;
import java.util.Properties;

import static scala.collection.JavaConversions.*;

public class KafkaUtils {
  private ZkUtils kafkaZKUtils;

  public KafkaUtils(String brokers, String zkConnect) {
    this.kafkaZKUtils = ZkUtils.apply(new ZkClient(zkConnect), false);
  }

  public boolean isTopicExists(String topic) {
    Map<String, Seq<Object>> topicToPartitionMap = kafkaZKUtils.getPartitionsForTopics(asScalaBuffer(Collections.singletonList(topic)));
    return topicToPartitionMap.get(topic).isDefined();
  }

  public void createTopic(String topic, int partitions, int replicationFactor, Properties topicProps) {
    AdminUtils.createTopic(kafkaZKUtils, topic, partitions, replicationFactor, topicProps);
  }

  public void deleteTopic(String topic) {
    AdminUtils.deleteTopic(kafkaZKUtils, topic);
  }

  public int getPartitionCount(String topic) {
    Map<String, Seq<Object>> topicToPartitionMap = kafkaZKUtils.getPartitionsForTopics(asScalaBuffer(Collections.singletonList(topic)));
    if (topicToPartitionMap.get(topic).isDefined()){
      return topicToPartitionMap.get(topic).get().size();
    }

    throw new FDBenchException("Cannot get partition count for topic " + topic);
  }
}
