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

import org.HdrHistogram.Histogram;

import java.time.Duration;
import java.util.concurrent.Callable;

public class LatencyBenchTask implements Callable<LatencySummary> {
  private final LatencyBenchmark.RequestGenerator requestGenerator;
  private final int requestRate;
  private final Duration duration;
  private final Duration expectedInterval;
  private final Histogram successHistogram;
  private final Histogram uncorrectedSuccessHistogram;
  private final Histogram errorHistogram;
  private final Histogram uncorrectedErrorHistogram;
  private int successTotal = 0;
  private int errorTotal = 0;
  private long elapsedTime = 0;

  public LatencyBenchTask(LatencyBenchmark.RequestGenerator requestGenerator, int requestRate, Duration duration) {
    this.requestGenerator = requestGenerator;
    this.requestRate = requestRate;
    this.duration = duration;

    if (requestRate > 0) {
      this.expectedInterval = Duration.ofNanos(1000000000 / requestRate);
    } else {
      this.expectedInterval = Duration.ZERO;
    }

    this.successHistogram = new Histogram(1, LatencyBenchmark.maxRecordableLatencyNS, LatencyBenchmark.sigFigs);
    this.uncorrectedSuccessHistogram = new Histogram(1, LatencyBenchmark.maxRecordableLatencyNS, LatencyBenchmark.sigFigs);
    this.errorHistogram = new Histogram(1, LatencyBenchmark.maxRecordableLatencyNS, LatencyBenchmark.sigFigs);
    this.uncorrectedErrorHistogram = new Histogram(1, LatencyBenchmark.maxRecordableLatencyNS, LatencyBenchmark.sigFigs);
  }

  public void setup() throws Exception {
    this.successHistogram.reset();
    this.uncorrectedSuccessHistogram.reset();
    this.errorHistogram.reset();
    this.uncorrectedErrorHistogram.reset();
    this.successTotal = 0;
    this.errorTotal = 0;
    this.requestGenerator.setup();
  }

  public void shutdown() throws Exception {
    this.requestGenerator.shutdown();
  }

  private long runRateLimited() {
    long stopAfter = System.currentTimeMillis() + duration.toMillis();
    long startTime = System.nanoTime();
    long before, latency;

    while (true) {
      if (System.currentTimeMillis() >= stopAfter) {
        return System.nanoTime() - startTime;
      }

      before = System.nanoTime();
      try {
        requestGenerator.request();
        latency = System.nanoTime() - before;
        successHistogram.recordValueWithExpectedInterval(latency, expectedInterval.toNanos());
        uncorrectedSuccessHistogram.recordValue(latency);
        successTotal++;
      } catch (Exception e) {
        latency = System.nanoTime() - before;
        errorHistogram.recordValueWithExpectedInterval(latency, expectedInterval.toNanos());
        uncorrectedErrorHistogram.recordValue(latency);
        errorTotal++;
      }


      while (expectedInterval.toNanos() > (System.nanoTime() - before)) {
        // busy loop
      }
    }

  }

  private long runFullThrottle() {
    long stopAfter = System.currentTimeMillis() + duration.toMillis();
    long startTime = System.nanoTime();
    long before, latency;

    while (true) {
      if (System.currentTimeMillis() >= stopAfter) {
        return System.nanoTime() - startTime;
      }

      before = System.nanoTime();

      try {
        requestGenerator.request();
        latency = System.nanoTime() - before;
        successHistogram.recordValue(latency);
        successTotal++;
      } catch (Exception e) {
        latency = System.nanoTime() - before;
        errorHistogram.recordValue(latency);
        errorTotal++;
      }
    }
  }

  @Override
  public LatencySummary call() throws Exception {
    if (requestRate <= 0) {
      elapsedTime = runFullThrottle();
    } else {
      elapsedTime = runRateLimited();
    }

    return new LatencySummary(requestRate, successTotal, errorTotal, Duration.ofNanos(elapsedTime), successHistogram.copy(),
        uncorrectedSuccessHistogram.copy(), errorHistogram.copy(), uncorrectedErrorHistogram.copy());
  }
}
