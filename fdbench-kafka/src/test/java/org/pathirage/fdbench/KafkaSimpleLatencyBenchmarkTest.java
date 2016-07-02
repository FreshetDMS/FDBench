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

package org.pathirage.fdbench;


import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Test;
import org.pathirage.fdbench.kafka.simple.SimpleLatencyBenchmark;
import org.pathirage.fdbench.kafka.simple.SimpleLatencySummary;
import org.pathirage.fdbench.kafka.simple.KafkaRequestGeneratorFactory;

import java.time.Duration;

public class KafkaSimpleLatencyBenchmarkTest {
  @Test
  public void testSimpleLatencyBench() throws Exception {
    SimpleLatencyBenchmark simpleLatencyBenchmark =
        new SimpleLatencyBenchmark(getSimpleLatencyBenchConfig(), new KafkaRequestGeneratorFactory(), 3000, 1, Duration.ofMinutes(1));
    SimpleLatencySummary summary = simpleLatencyBenchmark.run();
    SimpleLatencySummary.LatencyDistribution latencyDistribution = summary.getLatencyDistribution(null);
    SimpleLatencySummary.StandardDeviations success = summary.getStandardDeviation();
    System.out.println(String.format("STD corrected: %f, uncorrected: %f", success.getStd(), success.getStdUncorrected()));
    for(SimpleLatencySummary.LatencyPercentile p : latencyDistribution.getCorrected()) {
      System.out.println(String.format("percentile %f \t value %f \t count %d", p.getPercentile(), p.getValue(), p.getTotalCount()));
    }

  }

  private Config getSimpleLatencyBenchConfig() {
    ClassLoader classLoader = getClass().getClassLoader();

    return ConfigFactory.parseURL(classLoader.getResource("configurations/simple-latency-bench.conf"));
  }
}