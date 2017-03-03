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

package org.pathirage.fdbench.kafka.perfmodeling;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import javax.management.*;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.List;

public class KafkaJMXTest {
  public static void main(String[] args) throws IOException, MalformedObjectNameException, AttributeNotFoundException, MBeanException, ReflectionException, InstanceNotFoundException, KeeperException, InterruptedException {
    ZooKeeper zk = new ZooKeeper("ec2-54-186-101-195.us-west-2.compute.amazonaws.com:2181", 10000, null);
    List<String> ids = zk.getChildren("/brokers/ids", false);
    for (String id : ids) {
      String brokerInfo = new String(zk.getData("/brokers/ids/" + id, false, null));
      System.out.println(id + ": " + brokerInfo);
    }
    JMXServiceURL u = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://ec2-54-202-67-6.us-west-2.compute.amazonaws.com:9999/jmxrmi");
    JMXConnector c = JMXConnectorFactory.connect(u);
    System.out.println(c.getMBeanServerConnection().getAttribute(ObjectName.getInstance("kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec"),"Count"));
  }
}
