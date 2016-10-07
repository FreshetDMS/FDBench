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

import com.google.common.base.Joiner;
import com.typesafe.config.Config;
import org.apache.commons.lang3.ArrayUtils;
import org.pathirage.fdbench.FDBenchException;
import org.pathirage.fdbench.api.Benchmark;
import org.pathirage.fdbench.api.BenchmarkTaskFactory;
import org.pathirage.fdbench.kafka.KafkaAdmin;
import org.pathirage.fdbench.kafka.KafkaBenchmark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.IntStream;

/**
 * This benchmark measure the end-to-end latency of Kafka.
 */
public class E2ELatencyBenchmark extends KafkaBenchmark {
  private static final Logger log = LoggerFactory.getLogger(E2ELatencyBenchmark.class);

  public E2ELatencyBenchmark(int parallelism, Config rawConfig) {
    super(new E2ELatencyBenchmarkConfig(rawConfig), parallelism);
  }


  @Override
  public Class<? extends BenchmarkTaskFactory> getTaskFactoryClass() {
    return E2ELatencyBenchTaskFactory.class;
  }

  @Override
  public boolean isValidPartitionCountAndParallelism(int partitionCount, int parallelism) {
    return true;
  }
}
