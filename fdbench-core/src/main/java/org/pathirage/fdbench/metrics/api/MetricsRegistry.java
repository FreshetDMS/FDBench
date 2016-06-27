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

package org.pathirage.fdbench.metrics.api;

import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.Gauge;
import org.apache.samza.metrics.Metric;
import org.apache.samza.metrics.Timer;

import java.util.Map;
import java.util.Set;

public interface MetricsRegistry {
  /**
   * Create and register a new {@link org.apache.samza.metrics.Counter}
   *
   * @param group Group for this Counter
   * @param name  Name of to-be-created Counter
   * @return New Counter instance
   */
  Counter newCounter(String group, String name);

  /**
   * Register existing {@link org.apache.samza.metrics.Counter} with this registry
   *
   * @param group   Group for this Counter
   * @param counter Existing Counter to register
   * @return Counter that was registered
   */
  Counter newCounter(String group, Counter counter);

  /**
   * Create and register a new {@link org.apache.samza.metrics.Gauge}
   *
   * @param group Group for this Gauge
   * @param name  Name of to-be-created Gauge
   * @param value Initial value for the Gauge
   * @param <T>   Type the Gauge will be wrapping
   * @return Gauge was created and registered
   */
  <T> Gauge<T> newGauge(String group, String name, T value);

  /**
   * Register an existing {@link org.apache.samza.metrics.Gauge}
   *
   * @param group Group for this Gauge
   * @param value Initial value for the Gauge
   * @param <T>   Type the Gauge will be wrapping
   * @return Gauge was registered
   */
  <T> Gauge<T> newGauge(String group, Gauge<T> value);

  /**
   * Create and Register a new {@link org.apache.samza.metrics.Timer}
   *
   * @param group Group for this Timer
   * @param name  Name of to-be-created Timer
   * @return New Timer instance
   */
  Timer newTimer(String group, String name);

  /**
   * Register existing {@link org.apache.samza.metrics.Timer} with this registry
   *
   * @param group Group for this Timer
   * @param timer Existing Timer to register
   * @return Timer that was registered
   */
  Timer newTimer(String group, Timer timer);

  /**
   * Create and register a new {@link Histogram}
   *
   * @param group Group of this histogram
   * @param name  Name of to-be-created histogram
   * @return New Histogram instance
   */
  Histogram newHistogram(String group, String name);

  /**
   * Register existing {@link Histogram} with this registry
   *
   * @param group     Group of this Histogram
   * @param histogram Existing Histogram to register
   * @return Histogram that was registered
   */
  Histogram newHistogram(String group, Histogram histogram);

  Set<String> getGroups();

  Map<String, Metric> getGroup(String group);
}
