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

import com.google.common.collect.Lists;
import com.typesafe.config.Config;

import java.util.Collections;
import java.util.List;

public class MetricsReporterConfig extends AbstractConfig {

  private static final String METRICS_REPORTERS = "metrics.reporters";
  private static final String METRICS_REPORTER_CONFIG_PREFIX = "metrics.reporter.%s";
  private static final String METRICS_REPORTER_FACTORY_CLASS = METRICS_REPORTER_CONFIG_PREFIX + ".factory.class";

  public MetricsReporterConfig(Config config) {
    super(config);
  }

  public String getMetricsReporterFactoryClass(String name) {
    return getString(String.format(METRICS_REPORTER_FACTORY_CLASS, name));
  }

  public Config getMetricReporterProperties(String name) {
    return config.getConfig(String.format(METRICS_REPORTER_CONFIG_PREFIX, name));
  }

  public List<String> getMetricsReporters() {
    String reporters = getString(METRICS_REPORTERS);

    if (reporters != null && !reporters.isEmpty()) {
      return Lists.newArrayList(reporters.split(","));
    }

    return Collections.emptyList();
  }

  public Config getReporterConfig(String name) {
    return config.getConfig(String.format(METRICS_REPORTER_CONFIG_PREFIX, name));
  }
}
