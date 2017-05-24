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

import com.beust.jcommander.internal.Lists;
import com.google.common.base.Joiner;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import org.apache.zookeeper.ZooKeeper;
import org.pathirage.fdbench.api.BenchmarkDeploymentState;
import org.pathirage.fdbench.api.BenchmarkTaskFactory;
import org.pathirage.fdbench.aws.BenchmarkOnAWS;
import org.pathirage.fdbench.config.BenchConfig;
import org.pathirage.fdbench.utils.kafka.KafkaAdmin;
import org.pathirage.fdbench.kafka.KafkaBenchmarkConfig;
import org.pathirage.fdbench.kafka.KafkaBenchmarkConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.*;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class SyntheticWorkloadGenerator extends BenchmarkOnAWS {
  private static final Logger logger = LoggerFactory.getLogger(SyntheticWorkloadGenerator.class);
  private static final String METRIC_BYTES_IN_PER_SEC = "kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec";
  private static final String METRIC_BYTES_OUT_PER_SEC = "kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec";
  private static final String MSGS_IN_PER_SEC = "kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec";
  private static final String TOTAL_FETCH_REQS_IN_PER_SEC = "kafka.server:type=BrokerTopicMetrics,name=TotalFetchRequestsPerSec";
  private static final String TOTAL_PRODUCE_REQS_IN_PER_SEC = "kafka.server:type=BrokerTopicMetrics,name=TotalProduceRequestsPerSec";

  private final SyntheticWorkloadGeneratorConfig workloadGeneratorConfig;
  private final KafkaAdmin kafkaAdmin;
  private final String benchName;
  private final Gson gson = new Gson();
  private Map<String, String> brokerJMXEndpoints;
  private Map<String, BrokerMetricsSummary> brokerMetricsSummary = new HashMap<>();

  public SyntheticWorkloadGenerator(KafkaBenchmarkConfig benchmarkConfig, int parallelism) {
    super(benchmarkConfig.getRawConfig());
    workloadGeneratorConfig = (SyntheticWorkloadGeneratorConfig) benchmarkConfig;
    benchName = new BenchConfig(benchmarkConfig.getRawConfig()).getName();
    kafkaAdmin = new KafkaAdmin(benchName, benchmarkConfig.getBrokers(), benchmarkConfig.getZKConnectionString());
    brokerJMXEndpoints = findBrokerJMXEndpoints();
  }

  private Map<String, String> findBrokerJMXEndpoints() {
    try {
      Map<String, String> jmxEndpoints = new HashMap<>();
      ZooKeeper zk = new ZooKeeper(workloadGeneratorConfig.getZKConnectionString(), 10000, null);
      List<String> ids = zk.getChildren("/brokers/ids", false);
      for (String id : ids) {
        String brokerInfo = new String(zk.getData("/brokers/ids/" + id, false, null));
        Map brokerInfoMap = gson.fromJson(brokerInfo, Map.class);
        jmxEndpoints.put(id, String.format("%s:%s", brokerInfoMap.get("host").toString(), ((Double)brokerInfoMap.get("jmx_port")).intValue()));
      }

      return jmxEndpoints;
    } catch (Exception e) {
      throw new RuntimeException("Couldn't get list broker info from zookeeper.", e);
    }
  }

  @Override
  public Class<? extends BenchmarkTaskFactory> getTaskFactoryClass() {
    return ProducerTaskFactory.class;
  }


  @Override
  public void setup() {
    super.setup();
    validateTaskAllocation();

    if (!workloadGeneratorConfig.isReuseTopic() && areProduceTopicsExists()) {
      throw new RuntimeException("Some produce topics already exists. Existing topics: " + Joiner.on(", ").join(kafkaAdmin.listTopics()));
    }

    createTopics(getProduceTopicConfigs());

    Set<String> consumeTopics = getConsumeAndReplayTopicsNotInProduce();
    if (!consumeTopics.isEmpty()) {
      for (String topic : consumeTopics) {
        if (!kafkaAdmin.isTopicExists(topic)) {
          throw new RuntimeException("Consume topic " + topic + " should be there in Kafka.");
        }
      }
    }
  }

  @Override
  public void teardown() {
    super.teardown();
    // TODO: Delete topics if needed
  }

  @Override
  protected String getBenchName() {
    return benchName;
  }

  @Override
  protected void setupHook() {
    for (Map.Entry<String, String> e : brokerJMXEndpoints.entrySet()) {
      BrokerMetricsSummary summary = new BrokerMetricsSummary(Integer.valueOf(e.getKey()));

      try {
        summary.setStart(getBrokerMetricsSnapshot(e.getKey(), e.getValue()));
      } catch (Exception ex) {
        logger.error("Could not retrieve broker metrics.", ex);
        throw new RuntimeException("Could not retrieve broker metrics.", ex);
      }

      brokerMetricsSummary.put(e.getKey(), summary);
    }
  }

  @Override
  protected File teardownHook() {
    for (Map.Entry<String, String> e : brokerJMXEndpoints.entrySet()) {
      BrokerMetricsSummary summary = brokerMetricsSummary.get(e.getKey());

      try {
        summary.setEnd(getBrokerMetricsSnapshot(e.getKey(), e.getValue()));
      } catch (Exception ex) {
        logger.error("Could not retrieve broker metrics.", ex);
        throw new RuntimeException("Could not retrieve broker metrics.", ex);
      }
    }


    Brokers brokers = new Brokers();
    brokers.setBrokers(Lists.newArrayList(brokerMetricsSummary.values()));
    FileWriter fw = null;
    try {
      Path tmpDir = Files.createTempDirectory("broker-metrics");
      Path brokerMetricsPath = Paths.get(tmpDir.toString(), "broker-metrics.json");
      fw = new FileWriter(brokerMetricsPath.toFile());
      Gson gson = new Gson();
      gson.toJson(brokers, fw);
      return brokerMetricsPath.toFile();
    } catch (Exception e) {
      throw new RuntimeException("Could not save broker metrics to file.", e);
    } finally {
      if (fw != null) {
        try {
          fw.close();
        } catch (IOException e) {
          logger.warn("File write close failed.", e);
        }
      }
    }
  }

  private BrokerMetricsSummary.MetricsSnapshot getBrokerMetricsSnapshot(String brokerId, String brokerJMXEndpoint) throws IOException, MalformedObjectNameException, AttributeNotFoundException, MBeanException, ReflectionException, InstanceNotFoundException {
    BrokerMetricsSummary.MetricsSnapshot metricsSnapshot = new BrokerMetricsSummary.MetricsSnapshot(System.currentTimeMillis());
    metricsSnapshot.setBytesInPerSec(getRates(brokerJMXEndpoint, METRIC_BYTES_IN_PER_SEC));
    metricsSnapshot.setBytesOutPerSec(getRates(brokerJMXEndpoint, METRIC_BYTES_OUT_PER_SEC));
    metricsSnapshot.setMsgsInPerSec(getRates(brokerJMXEndpoint, MSGS_IN_PER_SEC));
    metricsSnapshot.setTotalFetchRequestsPerSec(getRates(brokerJMXEndpoint, TOTAL_FETCH_REQS_IN_PER_SEC));
    metricsSnapshot.setTotalProduceRequestsPerSec(getRates(brokerJMXEndpoint, TOTAL_PRODUCE_REQS_IN_PER_SEC));

    return metricsSnapshot;
  }

  private BrokerMetricsSummary.RateMetric getRates(String brokerJMXEndpoint, String objectName) throws IOException, MalformedObjectNameException, AttributeNotFoundException, MBeanException, ReflectionException, InstanceNotFoundException {
    BrokerMetricsSummary.RateMetric rateMetric = new BrokerMetricsSummary.RateMetric();

    JMXServiceURL u = new JMXServiceURL(String.format("service:jmx:rmi:///jndi/rmi://%s/jmxrmi", brokerJMXEndpoint));
    try (JMXConnector c = JMXConnectorFactory.connect(u)) {
      MBeanServerConnection mBeanServerConnection = c.getMBeanServerConnection();
      rateMetric.setCount((Long) mBeanServerConnection.getAttribute(ObjectName.getInstance(objectName), "Count"));
      rateMetric.setOneMinRate((double) mBeanServerConnection.getAttribute(ObjectName.getInstance(objectName), "OneMinuteRate"));
      rateMetric.setOneMinRate((double) mBeanServerConnection.getAttribute(ObjectName.getInstance(objectName), "FifteenMinuteRate"));
      rateMetric.setOneMinRate((double) mBeanServerConnection.getAttribute(ObjectName.getInstance(objectName), "FiveMinuteRate"));
      rateMetric.setOneMinRate((double) mBeanServerConnection.getAttribute(ObjectName.getInstance(objectName), "MeanRate"));
    }

    return rateMetric;
  }

  @Override
  public Map<String, String> configureTask(int taskId, BenchmarkDeploymentState deploymentState) {
    SyntheticWorkloadGeneratorDeploymentState.TaskDeploymentConfig deploymentConfig =
        ((SyntheticWorkloadGeneratorDeploymentState) deploymentState).nextTaskDeploymentConfig();

    Map<String, String> env = new HashMap<>();
    env.put(KafkaBenchmarkConstants.ENV_KAFKA_BENCH_BROKERS, workloadGeneratorConfig.getBrokers());
    env.put(KafkaBenchmarkConstants.ENV_KAFKA_BENCH_ZK, workloadGeneratorConfig.getZKConnectionString());
    env.putAll(deploymentConfig.getTaskEnvironment());

    return env;
  }

  private boolean areProduceTopicsExists() {
    for (String topic : workloadGeneratorConfig.getProduceTopics()) {
      if (kafkaAdmin.isTopicExists(topic)) {
        return true;
      }
    }

    return false;
  }

  private List<SyntheticWorkloadGeneratorConfig.TopicConfig> getProduceTopicConfigs() {
    List<SyntheticWorkloadGeneratorConfig.TopicConfig> topicConfigs = new ArrayList<>();

    for (String t : workloadGeneratorConfig.getProduceTopics()) {
      topicConfigs.add(workloadGeneratorConfig.getProduceTopicConfig(t));
    }

    return topicConfigs;
  }

  private void createTopics(List<SyntheticWorkloadGeneratorConfig.TopicConfig> topics) {
    for (SyntheticWorkloadGeneratorConfig.TopicConfig tc : topics) {
      if (!kafkaAdmin.isTopicExists(tc.getName())) {
        kafkaAdmin.createTopic(tc.getName(), tc.getPartitions(), tc.getReplicationFactor());
      }
    }
  }

  private Set<String> getConsumeAndReplayTopicsNotInProduce() {
    Set<String> consumeTopics = workloadGeneratorConfig.getConsumeTopics();
    consumeTopics.addAll(workloadGeneratorConfig.getReplayTopics());

    return Sets.difference(consumeTopics, workloadGeneratorConfig.getProduceTopics());
  }

  private void validateTaskAllocation() {
    int requiredTasks = 0;
    Set<String> produceTopics = workloadGeneratorConfig.getProduceTopics();
    Set<String> consumerTopics = workloadGeneratorConfig.getConsumeTopics();
    Set<String> replayTopics = workloadGeneratorConfig.getReplayTopics();

    for (String topic : produceTopics) {
      List<SyntheticWorkloadGeneratorConfig.ProducerGroupConfig> producerGroups =
          workloadGeneratorConfig.getProduceTopicConfig(topic).getProducerGroups();
      for (SyntheticWorkloadGeneratorConfig.ProducerGroupConfig p : producerGroups) {
        requiredTasks += p.getTaskCount();
      }
    }

    for (String topic : consumerTopics) {
      List<SyntheticWorkloadGeneratorConfig.ConsumerGroupConfig> consumerGroups =
          workloadGeneratorConfig.getConsumerTopicConfig(topic).getConsumerGroups();
      for (SyntheticWorkloadGeneratorConfig.ConsumerGroupConfig c : consumerGroups) {
        requiredTasks += c.getTaskCount();
      }
    }

    for (String topic : replayTopics) {
      List<SyntheticWorkloadGeneratorConfig.ConsumerGroupConfig> consumerGroups =
          workloadGeneratorConfig.getReplayTopicConfig(topic).getConsumerGroups();
      for (SyntheticWorkloadGeneratorConfig.ConsumerGroupConfig c : consumerGroups) {
        requiredTasks += c.getTaskCount();
      }
    }

    if (requiredTasks != new BenchConfig(workloadGeneratorConfig.getRawConfig()).getParallelism()) {
      throw new RuntimeException(String.format("Number of required tasks [%s] and allocated tasks [%s] does not match.", requiredTasks, new BenchConfig(workloadGeneratorConfig.getRawConfig()).getParallelism()));
    }
  }

  public static class Brokers {
    private List<BrokerMetricsSummary> brokers;

    public List<BrokerMetricsSummary> getBrokers() {
      return brokers;
    }

    public void setBrokers(List<BrokerMetricsSummary> brokers) {
      this.brokers = brokers;
    }
  }
}
