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
import org.pathirage.fdbench.FDBenchException;
import org.pathirage.fdbench.utils.Utils;
import org.pathirage.fdbench.api.BenchmarkTask;
import org.pathirage.fdbench.api.BenchmarkTaskFactory;
import org.pathirage.fdbench.api.Constants;
import org.pathirage.fdbench.config.BenchConfig;
import org.pathirage.fdbench.config.MetricsReporterConfig;
import org.pathirage.fdbench.metrics.InMemoryMetricsRegistry;
import org.pathirage.fdbench.metrics.api.MetricsRegistry;
import org.pathirage.fdbench.metrics.api.MetricsReporter;
import org.pathirage.fdbench.metrics.api.MetricsReporterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class FDMessagingBenchContainer {
  private static final Logger log = LoggerFactory.getLogger(FDMessagingBenchContainer.class);

  private final Config rawConfig;
  private final BenchConfig benchConfig;
  private BenchmarkTask benchTask;
  private final String containerId;
  private final String taskId;
  private final String benchName;
  private final String taskFactoryClass;
  private Map<String, MetricsReporter> metricsReporters = new HashMap<>();
  private final MetricsRegistry metricsRegistry = new InMemoryMetricsRegistry();

  public FDMessagingBenchContainer(String benchName, String taskId, String containerId, String taskFactoryClass, Config rawConfig) {
    this.benchName = benchName;
    this.taskId = taskId;
    this.containerId = containerId;
    this.rawConfig = rawConfig;
    this.benchConfig = new BenchConfig(rawConfig);
    this.taskFactoryClass = taskFactoryClass;
    init();
  }

  private void init() {
    setupBenchmark();
    setupMetricsReporters();
  }

  private void setupMetricsReporters() {
    MetricsReporterConfig reporterConfig = new MetricsReporterConfig(rawConfig);

    for (String reporter : reporterConfig.getMetricsReporters()) {
      try {
        MetricsReporterFactory factory =
            Utils.instantiate(reporterConfig.getMetricsReporterFactoryClass(reporter), MetricsReporterFactory.class);
        metricsReporters.put(reporter, factory.getMetricsReporter(reporter, containerId, rawConfig));
      } catch (Exception e) {
        throw new FDBenchException(String.format("[%s] Couldn't setup metrics reporter %s", containerId, reporter), e);
      }
    }
  }

  private void startMetricsReporters() {
    this.benchTask.registerMetrics(metricsReporters.values());
    metricsReporters.values().forEach(MetricsReporter::start);
  }

  private void setupBenchmark() {
    try {
      log.info(String.format("[%s] Loading benchTask factory %s.", containerId, taskFactoryClass));
      BenchmarkTaskFactory benchTaskFactory = Utils.instantiate(taskFactoryClass, BenchmarkTaskFactory.class);
      log.info(String.format("[%s] Creating benchTask instance.", containerId));
      this.benchTask = benchTaskFactory.getTask(benchName, taskId, containerId, rawConfig, metricsRegistry);
      this.benchTask.setup();
    } catch (Exception e) {
      throw new FDBenchException(String.format("[%s] Couldn't load benchTask factory %s.", containerId, taskFactoryClass), e);
    }
  }

  private void shutdownBenchmark() {
    benchTask.stop();
  }

  private void flushMetrics() {
    metricsReporters.values().forEach(MetricsReporter::flush);
  }

  private void shutdownMetrics() {
    metricsReporters.values().forEach(MetricsReporter::stop);
  }

  private void tearDownContainer() {
    flushMetrics();
    shutdownBenchmark();
    shutdownMetrics();
  }

  public void mainLoop() {
    log.info("Starting metrics reporters");
    startMetricsReporters();
    log.info(String.format("[%s] Executing benchTask ", containerId));
    try {
      benchTask.run();
    } catch (Exception e) {
      throw new FDBenchException("Error occurred in benchTask loop.", e);
    } finally {
      log.info("Shutting down...");
      tearDownContainer();
    }
  }

  public static void main(String[] args) throws IOException {
    String containerId = System.getenv(ApplicationConstants.Environment.CONTAINER_ID.toString());
    String taskId = System.getenv(Constants.FDBENCH_TASK_ID_ENV);
    String name = System.getenv(Constants.FDBENCH_BENCH_NAME_ENV);
    String taskFactoryClass = System.getenv(Constants.FDBENCH_TASK_FACTORY_CLASS);

    File workingDir = new File(System.getenv(ApplicationConstants.Environment.PWD.toString()));
    File configuration = new File(workingDir, "__bench.conf");

    FDMessagingBenchContainer container = new FDMessagingBenchContainer(name, taskId, containerId, taskFactoryClass,
        ConfigFactory.parseFile(configuration));
    container.mainLoop();
  }
}
