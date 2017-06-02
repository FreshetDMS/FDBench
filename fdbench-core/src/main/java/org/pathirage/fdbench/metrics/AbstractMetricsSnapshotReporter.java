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

import org.apache.commons.lang3.tuple.Pair;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.Gauge;
import org.apache.samza.metrics.Metric;
import org.apache.samza.metrics.Timer;
import org.pathirage.fdbench.api.Constants;
import org.pathirage.fdbench.metrics.api.ExtendedMetricsVisitor;
import org.pathirage.fdbench.metrics.api.Histogram;
import org.pathirage.fdbench.metrics.api.MetricsRegistry;
import org.pathirage.fdbench.metrics.api.MetricsReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public abstract class AbstractMetricsSnapshotReporter implements MetricsReporter, Runnable {
  private static final Logger log = LoggerFactory.getLogger(AbstractMetricsSnapshotReporter.class);
  private static final int BYTES_PER_MB = 1024 * 1024;
  final ScheduledExecutorService executor;
  final String name;
  final String jobName;
  final String containerName;
  final int interval;
  final List<Pair<String, MetricsRegistry>> registries = new ArrayList<>();


  public AbstractMetricsSnapshotReporter(String name, String jobName, String containerName, int interval, ScheduledExecutorService executor) {
    this.executor = executor;
    this.name = name;
    this.jobName = jobName;
    this.containerName = containerName;
    this.interval = interval;
  }

  @Override
  public void start() {
    log.info("Starting " + getClass().getName() + " instance with name " + name + " and interval " + interval);
    executor.scheduleWithFixedDelay(this, 0, interval, TimeUnit.SECONDS);
  }

  @Override
  public void register(String source, MetricsRegistry registry) {
    registries.add(Pair.<String, MetricsRegistry>of(source, registry));
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

  protected Map<String, Map<String, Object>> metricRegistryToMap(MetricsRegistry registry) {
    HashMap<String, Map<String, Object>> metricsEvent = new HashMap<>();
    for (String group : registry.getGroups()) {
      if (log.isDebugEnabled()) {
        log.debug("Retrieving metrics for group: " + group);
      }

      HashMap<String, Object> metricsGroupEvent = new HashMap<>();
      for (Map.Entry<String, Metric> metricEntry : registry.getGroup(group).entrySet()) {
        String name = metricEntry.getKey();

        metricEntry.getValue().visit(new ExtendedMetricsVisitor() {
          @Override
          public void histogram(Histogram histogram) {

            if (log.isDebugEnabled()) {
              log.debug("Processing histogram " + name);
            }

            HashMap<String, Object> histogramEvent = new HashMap<String, Object>();
            for (double percentile : Histogram.LOGARITHMIC_PERCENTILES) {
              long value = histogram.getValueAtPercentile(percentile);
              double valueToMilliseconds = (double) value;
              histogramEvent.put(Double.toString(percentile), valueToMilliseconds);
            }

            // Commenting out because this causes concurrent modification exception
//            HashMap<String, Object> summary = new HashMap<>();
//            summary.put("mean", histogram.getMean());
//            summary.put("max", histogram.getMaxValue());
//            summary.put("min", histogram.getMinValue());
//            summary.put("std", histogram.getStdDeviation());
//
//            histogramEvent.put("summary", summary);

            if (log.isDebugEnabled()) {
              log.debug("Histogram " + name + " content \n" + histogramEvent);
            }
            metricsGroupEvent.put(name, histogramEvent);
          }

          @Override
          public void counter(Counter counter) {
            if (log.isDebugEnabled()) {
              log.debug("Processing counter " + name + " with value " + counter.getCount());
            }

            metricsGroupEvent.put(name, counter.getCount());
          }

          @Override
          public <T> void gauge(Gauge<T> gauge) {
            metricsGroupEvent.put(name, gauge.getValue());
          }

          @Override
          public void timer(Timer timer) {
            metricsGroupEvent.put(name, timer.getSnapshot().getAverage());
          }
        });
      }

      metricsEvent.put(group, metricsGroupEvent);
    }
    return metricsEvent;
  }

  protected double getTotalMemory() {
    return Runtime.getRuntime().totalMemory() / (double) BYTES_PER_MB;
  }

  protected double getFreeMemory() {
    return Runtime.getRuntime().freeMemory() / (double) BYTES_PER_MB;
  }

  protected double getUsedMemory() {
    return getTotalMemory() - getFreeMemory();
  }

  protected double getMaxMemory() {
    return Runtime.getRuntime().maxMemory() / (double) BYTES_PER_MB;
  }

  protected long getJVMCPUTime() {
    OperatingSystemMXBean bean =
        ManagementFactory.getOperatingSystemMXBean( );
    if ( ! (bean instanceof com.sun.management.OperatingSystemMXBean) )
      return 0L;
    return ((com.sun.management.OperatingSystemMXBean)bean)
        .getProcessCpuTime( );
  }

}
