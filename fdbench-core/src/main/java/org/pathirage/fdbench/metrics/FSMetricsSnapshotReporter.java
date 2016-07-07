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

import com.google.gson.Gson;
import org.apache.commons.lang3.tuple.Pair;
import org.pathirage.fdbench.metrics.api.MetricsRegistry;
import org.pathirage.fdbench.metrics.api.MetricsReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class FSMetricsSnapshotReporter extends AbstractMetricsSnapshotReporter implements MetricsReporter, Runnable {
  private static final Logger log = LoggerFactory.getLogger(FSMetricsSnapshotReporter.class);

  private final String hostName;
  private final Path snapshotsDirectory;

  public FSMetricsSnapshotReporter(String name, String jobName, String containerName,
                                   String hostName, Path snapshotsDirectory, int interval) {
    super(name, jobName, containerName, interval, Executors.newScheduledThreadPool(1, new ThreadFactory() {
      @Override
      public Thread newThread(Runnable r) {
        Thread thread = new Thread(r);
        thread.setDaemon(true);
        thread.setName("FDBenchMessaging-FSMetricsSnapshotReporter");
        return thread;
      }
    }));
    this.hostName = hostName;
    this.snapshotsDirectory = snapshotsDirectory;
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
      header.put("host", hostName);
      header.put("time", recordingTime);

      HashMap<String, Object> metricsSnapshot = new HashMap<>();
      metricsSnapshot.put("header", header);
      metricsSnapshot.put("body", metricsEvent);

      Gson gson = new Gson();
      try (FileWriter fw = new FileWriter(Paths.get(snapshotsDirectory.toString(), registry.getKey() + "-" + recordingTime + ".json").toFile())) {
        String snapshot = gson.toJson(metricsSnapshot);
        if (log.isDebugEnabled()) {
          log.debug("Metrics snapshot: \n" + snapshot);
        }
        fw.write(snapshot);
      } catch (Exception e) {
        log.error("Couldn't publish metrics.", e);
      }
    }
  }
}
