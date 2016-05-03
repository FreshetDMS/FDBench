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

package org.pathirage.kafka.bench.yarn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.pathirage.kafka.bench.KafkaBenchException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;

public class YarnClientWrapper {
  private static final Logger log = LoggerFactory.getLogger(YarnClientWrapper.class);

  private final YarnClient yarnClient;

  public YarnClientWrapper(Configuration conf) {
    this.yarnClient = YarnClient.createYarnClient();
    this.yarnClient.init(conf);
    this.yarnClient.start();
  }

  public Optional<ApplicationId> submitApp(Path packagePath, Optional<String> name, int cpuCores, int memoryMb) {
    YarnClientApplication app = null;
    Optional<ApplicationId> appId;
    try {
      app = yarnClient.createApplication();
    } catch (Exception e) {
      throw new KafkaBenchException(e);
    }

    GetNewApplicationResponse newAppResp = app.getNewApplicationResponse();

    if (memoryMb > newAppResp.getMaximumResourceCapability().getMemory()) {
      throw new KafkaBenchException(String.format("You're asking for more memory (%s) than is allowed by YARN: %s",
          memoryMb, newAppResp.getMaximumResourceCapability().getMemory()));
    }

    if (cpuCores > newAppResp.getMaximumResourceCapability().getVirtualCores()) {
      throw new KafkaBenchException(String.format("You're asking for more CPU (%s) than is allowed by YARN: %s",
          cpuCores, newAppResp.getMaximumResourceCapability().getVirtualCores()));
    }

    appId = Optional.of(newAppResp.getApplicationId());

    log.info(String.format("Preparing to request resources for app id %s", appId.get()));

    ApplicationSubmissionContext appCtx = app.getApplicationSubmissionContext();
    appCtx.setKeepContainersAcrossApplicationAttempts(false);
    appCtx.setApplicationName(name.isPresent() ? name.get() : "kafka-bench-" + appId.get());

    return Optional.empty();
  }
}
