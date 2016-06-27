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

package org.pathirage.fdbench.yarn;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.pathirage.fdbench.messaging.FDMessagingBenchException;
import org.pathirage.fdbench.messaging.Utils;
import org.pathirage.fdbench.messaging.api.BenchmarkTask;
import org.pathirage.fdbench.metrics.api.MetricsReporter;
import org.pathirage.fdbench.metrics.api.MetricsReporterFactory;
import org.pathirage.fdbench.messaging.config.BenchConfig;
import org.pathirage.fdbench.messaging.api.BenchmarkTaskFactory;
import org.pathirage.fdbench.messaging.config.MetricsReporterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class FDMessagingBenchContainer {
  private static final Logger log = LoggerFactory.getLogger(FDMessagingBenchContainer.class);

  private final Config rawConfig;
  private final BenchConfig benchConfig;
  private BenchmarkTask benchmark;
  private final String containerId;
  private final String taskId;
  private final String benchName;
  private Map<String, MetricsReporter> metricsReporters = new HashMap<>();

  public FDMessagingBenchContainer(String benchName, String taskId, String containerId, Config rawConfig) {
    this.benchName = benchName;
    this.taskId = taskId;
    this.containerId = containerId;
    this.rawConfig = rawConfig;
    this.benchConfig = new BenchConfig(rawConfig);
    init();
  }

  private void init() {
    setupBenchmark();
    setupMetricsReporters();
  }

  private void setupMetricsReporters() {
    MetricsReporterConfig reporterConfig = new MetricsReporterConfig(rawConfig);

    for (String reporter : reporterConfig.getMetricsReporters()) {
      try{
        MetricsReporterFactory factory =
            Utils.instantiate(reporterConfig.getMetricsReporterFactoryClass(reporter), MetricsReporterFactory.class);
        metricsReporters.put(reporter, factory.getMetricsReporter(reporter, containerId, rawConfig));
      } catch (Exception e) {
        throw new FDMessagingBenchException(String.format("[%s] Couldn't setup metrics reporter %s", containerId, reporter));
      }
    }
  }

  private void startMetricsReporters() {
    this.benchmark.registerMetrics(metricsReporters.values());
    for(MetricsReporter reporter : metricsReporters.values()) {
      reporter.start();
    }
  }

  private void setupBenchmark() {
    try {
      log.info(String.format("[%s] Loading benchmark factory %s.", containerId, benchConfig.getBenchmarkFactoryClass()));
      BenchmarkTaskFactory benchmarkFactory = Utils.instantiate(benchConfig.getBenchmarkFactoryClass(), BenchmarkTaskFactory.class);
      log.info(String.format("[%s] Creating benchmark instance.", containerId));
      this.benchmark = benchmarkFactory.getBenchmark(benchName, taskId, containerId, rawConfig);
    } catch (Exception e) {
      throw new FDMessagingBenchException(String.format("[%s] Couldn't load benchmark factory.", containerId), e);
    }
  }

  private void shutdownBenchmark() {
    benchmark.stop();
  }

  private void shutdownMetrics() {
    for(MetricsReporter reporter : metricsReporters.values()) {
      reporter.stop();
    }
  }

  public void mainLoop() {
    log.info(String.format("[%s] Executing benchmark ", containerId));
    try {
      benchmark.run();
    } catch (Exception e) {
      throw new FDMessagingBenchException("Error occurred in benchmark loop.", e);
    } finally {
      log.info("Shutting down...");
      shutdownBenchmark();
      shutdownMetrics();
    }
  }

  public static void main(String[] args) throws IOException {
    String containerId = System.getenv(ApplicationConstants.Environment.CONTAINER_ID.toString());
    String taskId = System.getenv(Constants.KBENCH_TASK_ID_ENV);
    String name = System.getenv(Constants.KBENCH_BENCH_NAME_ENV);
    File workingDir = new File(System.getenv(ApplicationConstants.Environment.PWD.toString()));
    File configuration = new File(workingDir, "__bench.conf");

    FDMessagingBenchContainer container = new FDMessagingBenchContainer(name, taskId, containerId, ConfigFactory.parseFile(configuration));
    container.mainLoop();
  }
}
