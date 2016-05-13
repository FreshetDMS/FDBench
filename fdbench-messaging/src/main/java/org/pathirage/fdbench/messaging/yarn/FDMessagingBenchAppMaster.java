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

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.pathirage.fdbench.messaging.FDMessagingBenchException;
import org.pathirage.fdbench.messaging.Utils;
import org.pathirage.fdbench.messaging.api.BenchmarkConfigurator;
import org.pathirage.fdbench.messaging.api.BenchmarkConfiguratorFactory;
import org.pathirage.fdbench.messaging.config.BenchConfig;
import org.pathirage.fdbench.messaging.yarn.config.YarnConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class FDMessagingBenchAppMaster implements AMRMClientAsync.CallbackHandler, Runnable {
  private static final Logger log = LoggerFactory.getLogger(FDMessagingBenchAppMaster.class);

  private final ContainerId containerId;
  private final ApplicationAttemptId attemptId;
  private final Configuration yarnConf;
  private final NMClient nmClient;

  private final Config rawBenchConf;
  private int numContainers;
  private AtomicInteger allocatedContainers = new AtomicInteger(0);
  private String benchmarkName;
  private BenchmarkConfigurator benchmarkConfigurator;
  private boolean initialized = false;

  public FDMessagingBenchAppMaster(ContainerId containerId, ApplicationAttemptId attemptId, Config rawBenchConf) {
    this.containerId = containerId;
    this.attemptId = attemptId;
    this.yarnConf = new YarnConfiguration();
    this.nmClient = NMClient.createNMClient();
    this.nmClient.init(yarnConf);
    this.nmClient.start();
    this.rawBenchConf = rawBenchConf;
  }

  public void init() {
    BenchConfig benchConfig = new BenchConfig(rawBenchConf);
    try {
      BenchmarkConfiguratorFactory benchmarkConfiguratorFactory = Utils.instantiate(benchConfig.getBenchmarkTaskConfiguratorFactoryClass(), BenchmarkConfiguratorFactory.class);
      this.benchmarkConfigurator = benchmarkConfiguratorFactory.getConfigurator(benchConfig.getParallelism(), rawBenchConf);
      this.benchmarkConfigurator.configureBenchmark();
    } catch (Exception e) {
      throw new FDMessagingBenchException("Cannot load task configurator factory.", e);
    }

    initialized = true;
  }

  public void run() {
    if(!initialized) {
      throw new FDMessagingBenchException("Please initialized the application master first.");
    }

    try {
      AMRMClientAsync<AMRMClient.ContainerRequest> rmClient = AMRMClientAsync.createAMRMClientAsync(100, this);
      rmClient.init(yarnConf);
      rmClient.start();

      // Register with ResourceManager
      log.info("[KBench-AM] registering application master");
      rmClient.registerApplicationMaster("", 0, "");
      log.info("[KBench-AM] application master registered.");

      // Process benchmark containers and request containers
      BenchConfig benchConfig = new BenchConfig(rawBenchConf);

      this.benchmarkName = benchConfig.getName();
      this.numContainers = benchConfig.getParallelism();

      log.info("[KBench-AM] benchmark name: " + benchmarkName);
      log.info("[KBench-AM] parallelism: " + numContainers);


      Priority priority = Records.newRecord(Priority.class);
      priority.setPriority(0);

      YarnConfig benchYarnConfig = new YarnConfig(rawBenchConf);

      Resource capability = Records.newRecord(Resource.class);
      capability.setMemory(benchYarnConfig.getContainerMaxMemory());
      capability.setVirtualCores(benchYarnConfig.getContainerMaxCPUCores());

      for (int i = 0; i < numContainers; i++) {
        AMRMClient.ContainerRequest containerRequest = new AMRMClient.ContainerRequest(capability, null, null, priority);
        log.info("[KBench-AM] making resource request " + i);
        rmClient.addContainerRequest(containerRequest);
        allocatedContainers.incrementAndGet();
      }
      // Wait for the children to finish
      while (allocatedContainers.get() > 0) {
        Thread.sleep(2000);
      }

      // Unregister application master and exit
      rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "", "");
      log.info("[KBench-AM] application master unregistered.");
    } catch (Exception e) {
      throw new FDMessagingBenchException(e);
    }
  }

  public static void main(String[] args) throws Exception {
    String containerIdStr = System.getenv(ApplicationConstants.Environment.CONTAINER_ID.toString());
    log.info(String.format("[KBench-AM] Got container id: %s", containerIdStr));
    ContainerId containerId = ConverterUtils.toContainerId(containerIdStr);
    ApplicationAttemptId attemptId = containerId.getApplicationAttemptId();
    log.info(String.format("[KBench-AM] Got app attempt id: %s", attemptId));

    File workingDir = new File(System.getenv(ApplicationConstants.Environment.PWD.toString()));
    File configuration = new File(workingDir, "__bench.conf");

    log.info("[KBench-AM] Package path: " + System.getenv(Constants.KBENCH_PACKAGE_PATH_ENV));
    log.info("[KBench-AM] Conf path: " + System.getenv(Constants.KBENCH_CONF_PATH_ENV));

    FDMessagingBenchAppMaster appMaster = new FDMessagingBenchAppMaster(containerId, attemptId, ConfigFactory.parseFile(configuration));
    appMaster.init();
    appMaster.run();
  }

  // Begin AMRMClientAsync.CallbackHandler interface

  @Override
  public void onContainersCompleted(List<ContainerStatus> statuses) {
    for (ContainerStatus containerStatus : statuses) {
      log.info("[KBench-AM] Container " + ConverterUtils.toString(containerStatus.getContainerId()) +
          " completed with exit code " + containerStatus.getExitStatus() + " and status " + containerStatus.getState());
      allocatedContainers.decrementAndGet();
    }
  }

  @Override
  public void onContainersAllocated(List<Container> containers) {
    int i = 0;
    for (Container container : containers) {
      try {
        log.info("[KBench-AM] Container " + ConverterUtils.toString(container.getId()) + " allocated at " + container.getNodeId());
        ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);

        ctx.setCommands(Collections.singletonList(String.format(
            "export KBENCH_LOG_DIR=%s && ln -sfn %s logs && exec ./__package/bin/run-container.sh 1>logs/%s 2>logs/%s",
            "<LOG_DIR>", "<LOG_DIR>", "stdout", "stderr")));

        Map<String, String> envMap = new HashMap<>();
        envMap.put(Constants.KBENCH_CONTAINER_ID_ENV, ConverterUtils.toString(container.getId()));
        envMap.put(Constants.KBENCH_TASK_ID_ENV, String.valueOf(i));
        envMap.put(Constants.KBENCH_BENCH_NAME_ENV, benchmarkName);

        // BenchmarkConfigurator should generate environment variables that can be used in the benchmark task
        envMap.putAll(benchmarkConfigurator.configureTask(i));

        Map<String, LocalResource> localResourceMap = new HashMap<>();
        localResourceMap.put("__package", localizeAppPackage(System.getenv(Constants.KBENCH_PACKAGE_PATH_ENV).trim()));
        localResourceMap.put("__bench.conf", localizeAppConf(System.getenv(Constants.KBENCH_CONF_PATH_ENV).trim()));

        ctx.setEnvironment(envMap);
        ctx.setLocalResources(localResourceMap);

        nmClient.startContainer(container, ctx);
      } catch (Exception e) {
        log.error("[KBench-AM] Couldn't launch container " + ConverterUtils.toString(container.getId()), e);
        allocatedContainers.set(0);
      }
    }
  }

  private LocalResource localizeAppPackage(String appPkgPath) throws IOException {
    FileSystem fs = FileSystem.get(yarnConf);
    FileStatus fileStatus = fs.getFileStatus(new Path(appPkgPath));
    return LocalResource.newInstance(ConverterUtils.getYarnUrlFromPath(fileStatus.getPath()),
        LocalResourceType.ARCHIVE, LocalResourceVisibility.APPLICATION, fileStatus.getLen(), fileStatus.getModificationTime());
  }

  private LocalResource localizeAppConf(String confPath) throws IOException {
    FileSystem fs = FileSystem.get(yarnConf);
    FileStatus fileStatus = fs.getFileStatus(new Path(confPath));
    return LocalResource.newInstance(ConverterUtils.getYarnUrlFromPath(fileStatus.getPath()),
        LocalResourceType.FILE, LocalResourceVisibility.APPLICATION, fileStatus.getLen(), fileStatus.getModificationTime());
  }

  @Override
  public void onShutdownRequest() {

  }

  @Override
  public void onNodesUpdated(List<NodeReport> updatedNodes) {

  }

  @Override
  public float getProgress() {
    return 0;
  }

  @Override
  public void onError(Throwable e) {

  }

  // End AMRMClientAsync.CallbackHandler interface
}
