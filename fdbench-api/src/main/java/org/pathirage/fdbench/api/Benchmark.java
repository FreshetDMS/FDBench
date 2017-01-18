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

package org.pathirage.fdbench.api;

import java.util.Map;

/**
 * Defines the benchmark setup process, teardown process and how individual tasks in the benchmark is configured.
 */
public interface Benchmark {
  /**
   * Setup the environment required for benchmark related to this bootstrapper. For example, boostrapper for a Kafka
   * benchmark may create
   */
  void setup();

  /**
   * Perform cleanup tasks upon completion of the benchmark.
   */
  void teardown();

  /**
   * Class implementing the individual tasks of the benchmark.
   * @return BenchmarkTask implementation
   */
  Class<? extends BenchmarkTaskFactory> getTaskFactoryClass();

  /**
   * Generate set of properties that defines how a task should behave. For example, what Kafka partitions are assigned
   * to a particular task.
   * @param taskId Identifier of the task to configure
   * @return Task configuration as a set of key/value pairs
   */
  Map<String, String> configureTask(int taskId, BenchmarkDeploymentState deploymentState);
}
