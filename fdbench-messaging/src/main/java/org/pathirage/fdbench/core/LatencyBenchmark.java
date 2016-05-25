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

package org.pathirage.fdbench.core;

import com.typesafe.config.Config;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Implemented based on  https://github.com/tylertreat/bench.
 */
public class LatencyBenchmark {

  protected static final long maxRecordableLatencyNS = 300000000000L;
  protected static final int sigFigs = 5;

  public interface RequestGeneratorFactory {
    RequestGenerator getRequestGenerator(Config config, int taskId);
  }

  public interface RequestGenerator {
    void setup() throws Exception;

    void request() throws  Exception;

    void shutdown() throws  Exception;
  }

  private final int requestRate;
  private final int parallelism;
  private final Duration duration;
  private final List<LatencyBenchTask> benchTasks = new ArrayList<>();
  private final ExecutorService executorService;

  public LatencyBenchmark(Config config, RequestGeneratorFactory requestGeneratorFactory, int requestRate, int parallelism,
                          Duration duration) {
    this.requestRate = requestRate;
    this.parallelism = parallelism;
    this.duration = duration;

    for (int i = 0; i < parallelism; i++) {
      benchTasks.add(new LatencyBenchTask(requestGeneratorFactory.getRequestGenerator(config, i),
          requestRate / parallelism, duration));
    }

    this.executorService = Executors.newFixedThreadPool(parallelism);
  }

  public LatencySummary run() {

  }
}
