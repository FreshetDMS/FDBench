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
import org.pathirage.fdbench.api.BenchmarkTaskFactory;
import org.pathirage.fdbench.kafka.KafkaBenchmark;

public class ConsumerThroughputBenchmark extends KafkaBenchmark {
  public ConsumerThroughputBenchmark(int parallelism, Config rawConfig) {
    super(new ThroughputBenchmarkConfig(rawConfig), parallelism);
  }

  @Override
  public Class<? extends BenchmarkTaskFactory> getTaskFactoryClass() {
    return ConsumerThroughputTaskFactory.class;
  }

  @Override
  public boolean isValidPartitionCountAndParallelism(int partitionCount, int parallelism) {
    return partitionCount % parallelism == 0;
  }
}
