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

package org.pathirage.kafka.bench.config;

import com.typesafe.config.Config;

public class MetricStoreConfig extends AbstractConfig {
  private static final String METRICS_STORE_CONFIG_PREFIX = "metrics.store";

  private static final String METRICS_STORE_FACTORY_CLASS_SUFFIX = "metrics.store.factory.class";

  public MetricStoreConfig(Config config) {
    super(config);
  }

  public String getMetricsStoreFactoryClass() {
    return getString(METRICS_STORE_FACTORY_CLASS_SUFFIX);
  }

  public Config getMetricStoreProperties() {
    return config.getConfig(METRICS_STORE_CONFIG_PREFIX);
  }
}
