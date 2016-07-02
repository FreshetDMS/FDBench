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

package org.pathirage.fdbench.kafka.e2elatency;

import com.typesafe.config.Config;
import org.pathirage.fdbench.api.Benchmark;
import org.pathirage.fdbench.api.BenchmarkTaskFactory;

import java.util.Map;

/**
 * This benchmark measure the end-to-end latency of Kafka.
 */
public class E2ELatencyBenchmark implements Benchmark {
  private final E2ELatencyBenchmarkConfig benchmarkConfig;

  public E2ELatencyBenchmark(Config rawConfig) {
    this.benchmarkConfig = new E2ELatencyBenchmarkConfig(rawConfig);
  }

  @Override
  public void setup() {
    // Create the topic according to configuration
  }

  @Override
  public void teardown() {
    // Delete the topic created during setup stage.
  }

  @Override
  public Class<? extends BenchmarkTaskFactory> getTaskFactoryClass() {
    return E2ELatencyBenchTaskFactory.class;
  }


  @Override
  public Map<String, String> configureTask(int taskId) {
    // Calculate partition assignments.
    return null;
  }
}
