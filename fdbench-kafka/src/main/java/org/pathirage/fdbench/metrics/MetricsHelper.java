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
import org.apache.samza.metrics.Timer;
import org.pathirage.fdbench.metrics.api.MetricsRegistry;

public abstract class MetricsHelper {
  private final String group = this.getClass().getName();
  private final MetricsRegistry registry;
  private final MetricsGroup metricsGroup;

  public MetricsHelper(MetricsRegistry registry) {
    this.registry = registry;
    this.metricsGroup = new MetricsGroup(registry, group, getPrefix());
  }

  public Counter newCounter(String name) {
    return metricsGroup.newCounter(name);
  }

  public Histogram newHistogram(String name) {
    return metricsGroup.newHistogram(name);
  }

  public Timer newTimer(String name) {
    return metricsGroup.newTimer(name);
  }

  public String getPrefix() {
    return "";
  }
}
