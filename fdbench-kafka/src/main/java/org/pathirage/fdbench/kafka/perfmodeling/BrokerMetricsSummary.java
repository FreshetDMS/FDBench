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

package org.pathirage.fdbench.kafka.perfmodeling;

public class BrokerMetricsSummary {
  private int id;
  private MetricsSnapshot start;
  private MetricsSnapshot end;

  public BrokerMetricsSummary(int id) {
    this.id = id;
  }

  public MetricsSnapshot getStart() {
    return start;
  }

  public void setStart(MetricsSnapshot start) {
    this.start = start;
  }

  public MetricsSnapshot getEnd() {
    return end;
  }

  public void setEnd(MetricsSnapshot end) {
    this.end = end;
  }

  public static class MetricsSnapshot {
    private final long timestamp;
    private RateMetric bytesInPerSec;
    private RateMetric bytesOutPerSec;
    private RateMetric msgsInPerSec;
    private RateMetric totalFetchRequestsPerSec;
    private RateMetric totalProduceRequestsPerSec;

    public MetricsSnapshot(long timestamp) {
      this.timestamp = timestamp;
    }

    public long getTimestamp() {
      return timestamp;
    }

    public RateMetric getBytesInPerSec() {
      return bytesInPerSec;
    }

    public void setBytesInPerSec(RateMetric bytesInPerSec) {
      this.bytesInPerSec = bytesInPerSec;
    }

    public RateMetric getBytesOutPerSec() {
      return bytesOutPerSec;
    }

    public void setBytesOutPerSec(RateMetric bytesOutPerSec) {
      this.bytesOutPerSec = bytesOutPerSec;
    }

    public RateMetric getMsgsInPerSec() {
      return msgsInPerSec;
    }

    public void setMsgsInPerSec(RateMetric msgsInPerSec) {
      this.msgsInPerSec = msgsInPerSec;
    }

    public RateMetric getTotalFetchRequestsPerSec() {
      return totalFetchRequestsPerSec;
    }

    public void setTotalFetchRequestsPerSec(RateMetric totalFetchRequestsPerSec) {
      this.totalFetchRequestsPerSec = totalFetchRequestsPerSec;
    }

    public RateMetric getTotalProduceRequestsPerSec() {
      return totalProduceRequestsPerSec;
    }

    public void setTotalProduceRequestsPerSec(RateMetric totalProduceRequestsPerSec) {
      this.totalProduceRequestsPerSec = totalProduceRequestsPerSec;
    }
  }

  public static class RateMetric {
    private long count;
    private double meanRate;
    private double oneMinRate;
    private double fiveMinRate;
    private double fifteenMinRate;

    public long getCount() {
      return count;
    }

    public void setCount(long count) {
      this.count = count;
    }

    public double getMeanRate() {
      return meanRate;
    }

    public void setMeanRate(double meanRate) {
      this.meanRate = meanRate;
    }

    public double getOneMinRate() {
      return oneMinRate;
    }

    public void setOneMinRate(double oneMinRate) {
      this.oneMinRate = oneMinRate;
    }

    public double getFiveMinRate() {
      return fiveMinRate;
    }

    public void setFiveMinRate(double fiveMinRate) {
      this.fiveMinRate = fiveMinRate;
    }

    public double getFifteenMinRate() {
      return fifteenMinRate;
    }

    public void setFifteenMinRate(double fifteenMinRate) {
      this.fifteenMinRate = fifteenMinRate;
    }
  }
}
