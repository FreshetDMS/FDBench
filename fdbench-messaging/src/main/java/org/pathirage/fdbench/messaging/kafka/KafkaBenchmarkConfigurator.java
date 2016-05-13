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

package org.pathirage.fdbench.messaging.kafka;

import com.typesafe.config.Config;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.pathirage.fdbench.messaging.FDMessagingBenchException;
import org.pathirage.fdbench.messaging.api.BenchmarkConfigurator;

import java.util.Map;

public class KafkaBenchmarkConfigurator implements BenchmarkConfigurator {
  private final int parallelism;
  private final KafkaBenchmarkConfig config;

  public KafkaBenchmarkConfigurator(int parallelism, Config rawConfig) {
    this.parallelism = parallelism;
    this.config = new KafkaBenchmarkConfig(rawConfig);
  }

  @Override
  public void configureBenchmark() {
    KafkaBenchmarkConfig.BenchType type = config.getBenchType();

    if(type == KafkaBenchmarkConfig.BenchType.PRODUCER) {
    } else {
      throw new FDMessagingBenchException("Doesn't support Kafka benchmark type " + type + " yet.");
    }
  }

  @Override
  public Map<String, String> configureTask(int taskId) {
    return null;
  }
}
