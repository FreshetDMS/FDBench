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
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.Gauge;
import org.apache.samza.metrics.Metric;
import org.apache.samza.metrics.Timer;
import org.pathirage.fdbench.metrics.api.ExtendedMetricsVisitor;
import org.pathirage.fdbench.metrics.api.Histogram;
import org.pathirage.fdbench.metrics.api.MetricsRegistry;
import org.pathirage.fdbench.metrics.api.MetricsReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class FSMetricsSnapshotReporter implements MetricsReporter, Runnable {
  private static final Logger log = LoggerFactory.getLogger(FSMetricsSnapshotReporter.class);
  private final String name;
  private final String jobName;
  private final String containerName;
  private final String hostName;
  private final String benchFactory;
  private final Path snapshotsDirectory;
  private final int interval;
  private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1, new ThreadFactory() {
    @Override
    public Thread newThread(Runnable r) {
      Thread thread = new Thread(r);
      thread.setDaemon(true);
      thread.setName("FDBenchMessaging-FSMetricsSnapshotReporter");
      return thread;
    }
  });
  private final List<Pair<String, MetricsRegistry>> registeries = new ArrayList<>();

  public FSMetricsSnapshotReporter(String name, String jobName, String containerName, String benchFactory,
                                   String hostName, Path snapshotsDirectory, int interval) {
    this.name = name;
    this.jobName = jobName;
    this.containerName = containerName;
    this.hostName = hostName;
    this.benchFactory = benchFactory;
    this.snapshotsDirectory = snapshotsDirectory;
    this.interval = interval;
  }

  @Override
  public void start() {
    log.info("Starting FSMetricsSnapshotReporter");
    executor.scheduleWithFixedDelay(this, 0, interval, TimeUnit.SECONDS);
  }

  @Override
  public void register(String source, MetricsRegistry registry) {
    registeries.add(Pair.<String, MetricsRegistry>of(source, registry));
  }

  @Override
  public void stop() {
    executor.shutdown();
    try {
      executor.awaitTermination(interval, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      log.error("Executor awaitTermination interrupted.");
    } finally {
      if (!executor.isTerminated()) {
        log.warn("Unable to shutdown executor service.");
      }
    }
  }

  @Override
  public void run() {
    log.info("Starting to publish metrics.");

    for (Pair<String, MetricsRegistry> registry : registeries) {
      log.info("Flushing metrics for " + registry.getValue());

      HashMap<String, Map<String, Object>> metricsEvent = new HashMap<>();
      for (String group : registry.getValue().getGroups()) {
        if(log.isDebugEnabled()) {
          log.debug("Retrieving metrics for group: " + group);
        }

        HashMap<String, Object> metricsGroupEvent = new HashMap<>();
        for (Map.Entry<String, Metric> metricEntry : registry.getValue().getGroup(group).entrySet()) {
          String name = metricEntry.getKey();

          metricEntry.getValue().visit(new ExtendedMetricsVisitor() {
            @Override
            public void histogram(Histogram histogram) {

              if(log.isDebugEnabled()) {
                log.debug("Processing histogram " + name);
              }

              HashMap<String, Object> histogramEvent = new HashMap<String, Object>();
              for (double percentile : Histogram.LOGARITHMIC_PERCENTILES) {
                long value = histogram.getValueAtPercentile(percentile);
                double valueToMilliseconds = (double) value / 1000000;
                histogramEvent.put(Double.toString(percentile), valueToMilliseconds);
              }

              if(log.isDebugEnabled()) {
                log.debug("Histogram " + name + " content \n" + histogramEvent);
              }
              metricsGroupEvent.put(name, histogramEvent);
            }

            @Override
            public void counter(Counter counter) {
              if(log.isDebugEnabled()) {
                log.debug("Processing counter " + name + " with value " + counter.getCount());
              }

              metricsGroupEvent.put(name, counter.getCount());
            }

            @Override
            public <T> void gauge(Gauge<T> gauge) {
              throw new UnsupportedOperationException("Guages not supported yet.");
            }

            @Override
            public void timer(Timer timer) {
              metricsGroupEvent.put(name, timer.getSnapshot().getAverage());
            }
          });
        }

        metricsEvent.put(group, metricsGroupEvent);
      }

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
        if(log.isDebugEnabled()) {
          log.debug("Metrics snapshot: \n" + snapshot);
        }
        fw.write(snapshot);
      } catch (Exception e) {
        log.error("Couldn't publish metrics.", e);
      }
    }
  }
}
