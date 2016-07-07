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

package org.pathirage.fdbench.metrics;

import com.typesafe.config.Config;
import org.apache.samza.util.Util;
import org.pathirage.fdbench.config.AbstractConfig;
import org.pathirage.fdbench.config.BenchConfig;
import org.pathirage.fdbench.config.MetricsReporterConfig;
import org.pathirage.fdbench.metrics.api.MetricsReporter;
import org.pathirage.fdbench.metrics.api.MetricsReporterFactory;

import java.nio.file.Paths;

public class FSMetricsSnapshotReporterFactory implements MetricsReporterFactory {

  @Override
  public MetricsReporter getMetricsReporter(String name, String containerName, Config config) {
    FSMetricsSnapshotReporterConfig reporterConfig = new FSMetricsSnapshotReporterConfig(new MetricsReporterConfig(config).getReporterConfig(name));

    return new FSMetricsSnapshotReporter(name, new BenchConfig(config).getName(), containerName,
        Util.getLocalHost().getHostName(),
        Paths.get(reporterConfig.getSnapshotsDirectory()),
        reporterConfig.getReportingInterval());
  }

  private static class FSMetricsSnapshotReporterConfig extends AbstractConfig {
    private static final String SNAPSHOTS_DIR = "snapshots.directory";
    private static final String REPORTING_INTERVAL = "reporting.interval";

    private FSMetricsSnapshotReporterConfig(Config config) {
      super(config);
    }

    private String getSnapshotsDirectory() {
      return getString(SNAPSHOTS_DIR);
    }

    private int getReportingInterval() {
      return getInt(REPORTING_INTERVAL, 60);
    }
  }
}
