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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.pathirage.kafka.bench.KBenchException;
import org.pathirage.kafka.bench.yarn.config.YarnConfig;
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

  public ApplicationId submitApp(Optional<String> name, Path appConf, YarnConfig config) {
    YarnClientApplication app;
    ApplicationId appId;
    try {
      log.info("Creating YARN application....");
      app = yarnClient.createApplication();
      log.info("YARN application was created with id " + app.getNewApplicationResponse().getApplicationId());

      GetNewApplicationResponse newAppResp = app.getNewApplicationResponse();

      int amMem = config.getAMContainerMaxMemory();
      int amCPUCores  = config.getAMContainerMaxCPUCores();

      if (amMem > newAppResp.getMaximumResourceCapability().getMemory()) {
        throw new KBenchException(String.format("You're asking for more memory (%s) than is allowed by YARN: %s",
            config.getAMContainerMaxMemory(), newAppResp.getMaximumResourceCapability().getMemory()));
      }

      if (amCPUCores > newAppResp.getMaximumResourceCapability().getVirtualCores()) {
        throw new KBenchException(String.format("You're asking for more CPU (%s) than is allowed by YARN: %s",
            config.getAMContainerMaxCPUCores(), newAppResp.getMaximumResourceCapability().getVirtualCores()));
      }

      appId = newAppResp.getApplicationId();

      if(appId == null) {
        throw new KBenchException("YARN didn't return an applicaiton id.");
      }

      log.info(String.format("Preparing to request resources for app id %s", appId));

      ApplicationSubmissionContext appCtx = app.getApplicationSubmissionContext();
      appCtx.setKeepContainersAcrossApplicationAttempts(false);
      String appName = name.isPresent() ? name.get() : "kafka-bench-" + appId;
      appCtx.setApplicationName(appName);


      Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

      LocalResource jobPkgRsc = localizeAppPackage(conf, getAppPackageSuffix(appName, appId.toString()),
          config.getPackagePath());
      LocalResource appConfRsc = localizeJobConfig(conf, getAppConfSuffix(appName, appId.toString()),
          appConf.toString());

      localResources.put("__package", jobPkgRsc);
      //localResources.put("configuration", appConfRsc);


      List<String> cmdList = new ArrayList<>();
      cmdList.add(String.format(
          "export KBENCH_LOG_DIR=%s && ln -sfn %s logs && exec ./__package/bin/run-am.sh 1>logs/%s 2>logs/%s",
          "<LOG_DIR>", "<LOG_DIR>", "stdout", "stderr"));

      ContainerLaunchContext amContainer = ContainerLaunchContext.newInstance(localResources,
          Collections.emptyMap(), cmdList, null, null, null);

      Resource capability = Resource.newInstance(amMem, amCPUCores);

      appCtx.setResource(capability);
      appCtx.setAMContainerSpec(amContainer);
      appCtx.setApplicationType("KBench");
      appCtx.setQueue("default");

      log.info(String.format("Submitting application request for %s", appId));

      yarnClient.submitApplication(appCtx);

      return appId;
    } catch (Exception e) {
      throw new KBenchException(e);
    }
  }

  private static LocalResource localizeJobConfig(Configuration hadoopConf, String confSuffix, String appConfPath) {
    if (appConfPath == null) {
      throw new KBenchException("App config path is null");
    }

    FileStatus remoteFileStatus = copyFileToRemote(hadoopConf, confSuffix, appConfPath);

    return LocalResource.newInstance(
        ConverterUtils.getYarnUrlFromPath(remoteFileStatus.getPath()),
        LocalResourceType.FILE, LocalResourceVisibility.APPLICATION,
        remoteFileStatus.getLen(), remoteFileStatus.getModificationTime());
  }


  private static LocalResource localizeAppPackage(Configuration hadoopConf, String packageSuffix, String packagePath) {
    if (packagePath == null) {
      throw new KBenchException("App package path is null.");
    }

    FileStatus remoteFileStatus = copyFileToRemote(hadoopConf, packageSuffix, packagePath);

    return LocalResource.newInstance(
        ConverterUtils.getYarnUrlFromPath(remoteFileStatus.getPath()),
        LocalResourceType.ARCHIVE, LocalResourceVisibility.APPLICATION,
        remoteFileStatus.getLen(), remoteFileStatus.getModificationTime());
  }

  private static String getAppPackageSuffix(String appName, String appId) {
    return appName + "/" + appId + "/package.tgz";
  }

  private static String getAppConfSuffix(String appName, String appId) {
    return appName + "/" + appId + "/package.tgz";
  }

  private static FileSystem getHadoopFS(Configuration hadoopConf) {
    try {
      return FileSystem.get(hadoopConf);
    } catch (IOException e) {
      throw new KBenchException("Couldn't create Hadoop file system.", e);
    }
  }

  private static FileStatus copyFileToRemote(Configuration hadoopConf, String filePathSuffix, String srcFilePath) {
    FileSystem fs = getHadoopFS(hadoopConf);

    org.apache.hadoop.fs.Path dst = new org.apache.hadoop.fs.Path(fs.getHomeDirectory(), filePathSuffix);

    try {
      log.info("Copying file " + srcFilePath + " to " + dst.toString());
      fs.copyFromLocalFile(new org.apache.hadoop.fs.Path(srcFilePath), dst);
      return fs.getFileStatus(dst);
    } catch (Exception e) {
      throw new KBenchException("Cannot copy " + srcFilePath + " to remote file system.", e);
    }
  }

}
