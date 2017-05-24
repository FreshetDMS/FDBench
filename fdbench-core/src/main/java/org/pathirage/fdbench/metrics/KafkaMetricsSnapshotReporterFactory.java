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

import com.typesafe.config.Config;
import org.pathirage.fdbench.config.BenchConfig;
import org.pathirage.fdbench.config.MetricsReporterConfig;
import org.pathirage.fdbench.metrics.api.MetricsReporter;
import org.pathirage.fdbench.metrics.api.MetricsReporterFactory;

public class KafkaMetricsSnapshotReporterFactory implements MetricsReporterFactory {
  @Override
  public MetricsReporter getMetricsReporter(String name, String containerName, Config config) {
    return new KafkaMetricsSnapshotReporter(name, new BenchConfig(config).getName(), containerName,
        new KafkaMetricsSnapshotReporterConfig(new MetricsReporterConfig(config).getReporterConfig(name)));
  }

  public static class KafkaMetricsSnapshotReporterConfig extends AbstractMetricsReporterConfig {
    private static final String KAFKA_ZK_CONNECT = "kafka.zookeeper";
    private static final String KAFKA_BROKERS = "kafka.brokers";

    public KafkaMetricsSnapshotReporterConfig(Config config) {
      super(config);
    }

    public String getKafkaBrokers() {
      return getString(KAFKA_BROKERS, "localhost:9092");
    }

    public String getKafkaZkConnect() {
      return getString(KAFKA_ZK_CONNECT, "localhost:2181");
    }
  }
}
