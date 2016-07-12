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

package org.pathirage.fdbench.kafka.throughput;

import com.typesafe.config.Config;
import org.pathirage.fdbench.api.BenchmarkTask;
import org.pathirage.fdbench.api.BenchmarkTaskFactory;
import org.pathirage.fdbench.metrics.api.MetricsRegistry;

public class ConsumerThroughputTaskFactory implements BenchmarkTaskFactory{
  @Override
  public BenchmarkTask getTask(String benchmarkName, String taskId, String containerID, Config config, MetricsRegistry metricsRegistry) {
    return null;
  }
}
