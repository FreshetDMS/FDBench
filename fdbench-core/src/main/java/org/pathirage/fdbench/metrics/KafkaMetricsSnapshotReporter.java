/**
 * Copyright 2017 Milinda Pathirage
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
package org.pathirage.fdbench.metrics;

import com.google.gson.Gson;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.samza.util.Util;
import org.pathirage.fdbench.metrics.api.MetricsRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class KafkaMetricsSnapshotReporter extends AbstractMetricsSnapshotReporter {
  private static final Logger log = LoggerFactory.getLogger(KafkaMetricsSnapshotReporter.class);

  private final KafkaMetricsSnapshotReporterFactory.KafkaMetricsSnapshotReporterConfig config;
  private final String metricsSnapshotTopic;
  private final String containerName;
  private final KafkaProducer<byte[], String> producer;

  public KafkaMetricsSnapshotReporter(String name, String jobName, String containerName, KafkaMetricsSnapshotReporterFactory.KafkaMetricsSnapshotReporterConfig config) {
    super(name, jobName, containerName, config.getReportingInterval(),
        Executors.newScheduledThreadPool(1, new ThreadFactory() {
          @Override
          public Thread newThread(Runnable r) {
            Thread thread = new Thread(r);
            thread.setDaemon(true);
            thread.setName("FDBench-KafkaMetricsSnapshotReporter");
            return thread;
          }
        }));

    this.config = config;
    this.metricsSnapshotTopic = String.format("%s-metrics", jobName);
    this.containerName = containerName;
    this.producer = new KafkaProducer<byte[], String>(getProducerProperties());
  }

  @Override
  public void run() {
    log.info("Starting to publish metrics.");

    for (Pair<String, MetricsRegistry> registry : registries) {
      log.info("Flushing metrics for " + registry.getValue());

      Map<String, Map<String, Object>> metricsEvent = metricRegistryToMap(registry.getValue());

      long recordingTime = System.currentTimeMillis();
      HashMap<String, Object> header = new HashMap<>();
      header.put("bench-name", jobName);
      header.put("container", containerName);
      header.put("host", Util.getLocalHost().getHostName());
      header.put("time", recordingTime);

      HashMap<String, Object> metricsSnapshot = new HashMap<>();
      metricsSnapshot.put("header", header);
      metricsSnapshot.put("body", metricsEvent);

      Gson gson = new Gson();

      producer.send(new ProducerRecord<byte[], String>(metricsSnapshotTopic, gson.toJson(metricsSnapshot)));
    }
  }

  @Override
  public void flush() {
    run();
    if (producer != null) {
      producer.close();
    }
  }

  private Properties getProducerProperties() {
    Properties producerProps = new Properties();

    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getKafkaBrokers());
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, containerName + "-producer");

    return producerProps;
  }
}
