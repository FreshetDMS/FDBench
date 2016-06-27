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

package org.pathirage.fdbench.kafka.producer;

import com.typesafe.config.Config;
import org.pathirage.fdbench.api.BenchmarkTask;
import org.pathirage.fdbench.metrics.api.MetricsReporter;

import java.util.Collection;

public class ProducerThroughputBenchmark implements BenchmarkTask {
  private final String taskId;
  private final String name;
  private final String containerId;
  private final Config rawConfig;

  public ProducerThroughputBenchmark(String taskId, String name, String containerId, Config rawConfig) {
    this.taskId = taskId;
    this.name = name;
    this.containerId = containerId;
    this.rawConfig = rawConfig;
  }

  @Override
  public String getTaskId() {
    return null;
  }

  @Override
  public String getBenchmarkName() {
    return null;
  }

  @Override
  public String getContainerId() {
    return null;
  }

  @Override
  public void registerMetrics(Collection<MetricsReporter> reporters) {

  }

  @Override
  public void stop() {

  }

  @Override
  public void run() {

  }
}
