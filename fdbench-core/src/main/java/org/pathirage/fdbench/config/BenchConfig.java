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

package org.pathirage.fdbench.config;

import com.typesafe.config.Config;

public class BenchConfig extends AbstractConfig {

  private static final String BENCHMARK_NAME = "benchmark.name";

  private static final String BENCHMARK_DESC = "benchmark.description";

  private static final String BENCHMARK_JOB_FACTORY_CLASS = "benchmark.job.factory.class";

  private static final String BENCHMARK_FACTORY_CLASS = "benchmark.factory.class";

  private static final String BENCHMARK_PARALLELISM = "benchmark.parallelism";

  private static final String BENCHMARK_DEPLOYMENT_STATE_FACTORY = "benchmark.deployment.state.factory";

  private static final String BENCHMARK_DURATION = "benchmark.duration";

  public BenchConfig(Config config) {
    super(config);
  }

  public String getName() {
    return getString(BENCHMARK_NAME, null);
  }

  public String getDescription() {
    return getString(BENCHMARK_DESC, "KBench job");
  }

  public String getJobFactoryClass() {
    return getString(BENCHMARK_JOB_FACTORY_CLASS);
  }

  public String getBenchmarkFactoryClass() {
    return getString(BENCHMARK_FACTORY_CLASS);
  }

  public int getParallelism() {
    return getInt(BENCHMARK_PARALLELISM, 1);
  }

  public String getBenchmarkDeploymentStateFactory(){
    return getString(BENCHMARK_DEPLOYMENT_STATE_FACTORY);
  }

  public int getBenchmarkDuration() {
    return getInt(BENCHMARK_DURATION, 5 * 60);
  }
}
