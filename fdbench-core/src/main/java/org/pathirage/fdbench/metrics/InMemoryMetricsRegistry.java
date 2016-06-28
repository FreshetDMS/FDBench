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

package org.pathirage.fdbench.metrics;

import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.Gauge;
import org.apache.samza.metrics.Metric;
import org.apache.samza.metrics.Timer;
import org.pathirage.fdbench.metrics.api.Histogram;
import org.pathirage.fdbench.metrics.api.MetricsRegistry;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryMetricsRegistry implements MetricsRegistry {
  private final ConcurrentHashMap<String, ConcurrentHashMap<String, Metric>> metrics = new ConcurrentHashMap<>();
  private final String name;

  public InMemoryMetricsRegistry() {
    this("unknown");
  }

  public InMemoryMetricsRegistry(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  private ConcurrentHashMap<String, Metric> putAndGetGroup(String group) {
    metrics.putIfAbsent(group, new ConcurrentHashMap<>());
    return metrics.get(group);
  }

  @Override
  public Counter newCounter(String group, String name) {
    return newCounter(group, new Counter(name));
  }

  @Override
  public Counter newCounter(String group, Counter counter) {
    putAndGetGroup(group).putIfAbsent(counter.getName(), counter);
    return (Counter)metrics.get(group).get(counter.getName());
  }

  @Override
  public <T> Gauge<T> newGauge(String group, String name, T value) {
    return newGauge(group, new Gauge<T>(name, value));
  }

  @Override
  public <T> Gauge<T> newGauge(String group, Gauge<T> guage) {
    putAndGetGroup(group).putIfAbsent(guage.getName(), guage);
    return (Gauge<T>)metrics.get(group).get(guage.getName());
  }

  @Override
  public Timer newTimer(String group, String name) {
    return newTimer(group, new Timer(name));
  }

  @Override
  public Timer newTimer(String group, Timer timer) {
    putAndGetGroup(group).putIfAbsent(timer.getName(), timer);
    return (Timer)metrics.get(group).get(timer.getName());
  }

  @Override
  public Histogram newHistogram(String group, String name,  int numberOfSignificantValueDigits) {
    return newHistogram(group, new Histogram(name, numberOfSignificantValueDigits));
  }

  @Override
  public Histogram newHistogram(String group, String name, long highestTrackableValue, int numberOfSignificantValueDigits) {
    return newHistogram(group, new Histogram(name, highestTrackableValue, numberOfSignificantValueDigits));
  }

  @Override
  public Histogram newHistogram(String group, Histogram histogram) {
    putAndGetGroup(group).putIfAbsent(histogram.getName(), histogram);
    return (Histogram)metrics.get(group).get(histogram.getName());
  }

  @Override
  public Set<String> getGroups() {
    return metrics.keySet();
  }

  @Override
  public Map<String, Metric> getGroup(String group) {
    return metrics.get(group);
  }
}
