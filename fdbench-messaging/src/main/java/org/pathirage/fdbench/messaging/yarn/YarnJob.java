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

package org.pathirage.fdbench.messaging.yarn;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.pathirage.fdbench.messaging.api.BenchJob;
import org.pathirage.fdbench.messaging.yarn.config.YarnConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.Optional;

public class YarnJob implements BenchJob {
  private static final Logger log = LoggerFactory.getLogger(YarnJob.class);

  private static final YarnConfiguration configuration = new YarnConfiguration();
  private static final YarnClientWrapper clientWrapper = new YarnClientWrapper(configuration);

  private ApplicationId appId;
  private final String name;
  private final YarnConfig yarnConfig;

  public YarnJob(String name, YarnConfig yarnConfig) {
    this.name = name;
    this.yarnConfig = yarnConfig;
  }

  @Override
  public BenchJob submit(Path configFile) {
    this.appId = clientWrapper.submitApp(Optional.of(name), configFile, yarnConfig);
    return this;
  }
}
