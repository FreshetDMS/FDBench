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

package org.pathirage.fdbench.kafka.simple;

import com.typesafe.config.Config;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.*;

/**
 * Implemented based on  https://github.com/tylertreat/bench.
 */
public class SimpleLatencyBenchmark {

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
  private final Set<Future<SimpleLatencySummary>> results = new HashSet<Future<SimpleLatencySummary>>();
  private final Set<SimpleLatencyBenchTask> benchTasks = new HashSet<SimpleLatencyBenchTask>();
  private final ExecutorService executorService;
  private final RequestGeneratorFactory requestGeneratorFactory;
  private final Config config;

  public SimpleLatencyBenchmark(Config config, RequestGeneratorFactory requestGeneratorFactory, int requestRate, int parallelism,
                                Duration duration) {
    this.config = config;
    this.requestRate = requestRate;
    this.parallelism = parallelism;
    this.duration = duration;
    this.requestGeneratorFactory = requestGeneratorFactory;
    this.executorService = Executors.newFixedThreadPool(parallelism);
  }

  public SimpleLatencySummary run() throws Exception {
    SimpleLatencySummary summary = null;
    for (int i = 0; i < parallelism; i++) {
      SimpleLatencyBenchTask callable =
          new SimpleLatencyBenchTask(requestGeneratorFactory.getRequestGenerator(config, i), requestRate, duration);
      benchTasks.add(callable);

      callable.setup();

      results.add(executorService.submit(callable));
    }

    for(Future<SimpleLatencySummary> future : results) {
      if (summary == null) {
        summary = future.get();
      } else {
        summary.merge(future.get());
      }
    }

    for(SimpleLatencyBenchTask benchTask : benchTasks) {
      benchTask.shutdown();
    }

    return summary;
  }
}