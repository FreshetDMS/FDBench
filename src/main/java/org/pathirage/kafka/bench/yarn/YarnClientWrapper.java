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

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.pathirage.kafka.bench.KafkaBenchException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

public class YarnClientWrapper {
  private static final Logger log = LoggerFactory.getLogger(YarnClientWrapper.class);

  private final YarnClient yarnClient;
  private final Configuration conf;

  public YarnClientWrapper(Configuration conf) {
    this.conf = conf;
    this.yarnClient = YarnClient.createYarnClient();
    log.info(String.format("YARN resource manager is at %s", conf.get(YarnConfiguration.RM_ADDRESS)));
    this.yarnClient.init(conf);
    this.yarnClient.start();
  }

  public Optional<ApplicationId> submitApp(Path packagePath, Optional<String> name, int cpuCores, int memoryMb) {
    YarnClientApplication app = null;
    Optional<ApplicationId> appId;
    try {
      log.info("Creating YARN application....");
      app = yarnClient.createApplication();
      log.info("YARN application was created.");

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
      String appName = name.isPresent() ? name.get() : "kafka-bench-" + appId.get();
      appCtx.setApplicationName(appName);


      Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

      log.info("Copy App package from local filesystem and add to local environment");

      FileSystem fs = FileSystem.get(conf);
      addToLocalResources(appName, fs, packagePath.toString(), appId.get().toString() , localResources);

      List<String> cmdList = new ArrayList<>();
      cmdList.add(String.format(
          "export KBENCH_LOG_DIR=%s && ln -sfn %s logs && exec ./__package/bin/run-am.sh 1>logs/%s 2>logs/%s",
          "<LOG_DIR>", "<LOG_DIR>", "stdout", "stderr"));

      ContainerLaunchContext amContainer = ContainerLaunchContext.newInstance(localResources,
          Collections.emptyMap(), cmdList, null, null, null);

      Resource capability = Resource.newInstance(memoryMb, cpuCores);

      appCtx.setResource(capability);
      appCtx.setAMContainerSpec(amContainer);
      appCtx.setApplicationType("KBench");
      appCtx.setQueue("default");

      log.info(String.format("Submitting application request for %s", appId.get()));

      yarnClient.submitApplication(appCtx);

      return appId;
    } catch (Exception e) {
      throw new KafkaBenchException(e);
    }
  }

  private static void addToLocalResources(String appName, FileSystem fs, String fileSrcPath,
                                          String appId, Map<String, LocalResource> localResources) {
    String suffix =
        appName + "/" + appId + "/package.tgz";
    org.apache.hadoop.fs.Path dst =
        new org.apache.hadoop.fs.Path(fs.getHomeDirectory(), suffix);

    log.info("Resource destination path: " + dst.toString());
    log.info("Destination path to YARN URL: " + ConverterUtils.getYarnUrlFromURI(dst.toUri()));
    if (fileSrcPath == null) {
      throw new KafkaBenchException("Local resource source path cannot be null.");
    }

    FileStatus scFileStatus;

    try {
      fs.copyFromLocalFile(new org.apache.hadoop.fs.Path(fileSrcPath), dst);
      scFileStatus = fs.getFileStatus(dst);
    } catch (Exception e) {
      throw new KafkaBenchException("Cannot copy local resource to remote.", e);
    }

    LocalResource scRsrc =
        LocalResource.newInstance(
            ConverterUtils.getYarnUrlFromURI(dst.toUri()),
            LocalResourceType.ARCHIVE, LocalResourceVisibility.APPLICATION,
            scFileStatus.getLen(), scFileStatus.getModificationTime());
    localResources.put("__package", scRsrc);
  }

}
