/**
 * Copyright 2017 Milinda Pathirage
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

import com.typesafe.config.Config;
import org.pathirage.fdbench.config.AbstractConfig;

public abstract class AbstractMetricsReporterConfig extends AbstractConfig {
  private static final String REPORTING_INTERVAL = "reporting.interval";


  public AbstractMetricsReporterConfig(Config config) {
    super(config);
  }

  public int getReportingInterval() {
    return getInt(REPORTING_INTERVAL, 60);
  }
}
