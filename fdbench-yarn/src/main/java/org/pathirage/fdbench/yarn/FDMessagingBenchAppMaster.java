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

package org.pathirage.fdbench.yarn;

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
import org.pathirage.fdbench.FDBenchException;
import org.pathirage.fdbench.Utils;
import org.pathirage.fdbench.api.BenchmarkConfigurator;
import org.pathirage.fdbench.api.BenchmarkConfiguratorFactory;
import org.pathirage.fdbench.config.BenchConfig;
import org.pathirage.fdbench.yarn.config.YarnConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class FDMessagingBenchAppMaster implements AMRMClientAsync.CallbackHandler, Runnable {
  private static final Logger log = LoggerFactory.getLogger(FDMessagingBenchAppMaster.class);

  private final ContainerId containerId;
  private final Configuration yarnConf;
  private final NMClient nmClient;

  private final Config rawBenchConf;
  private AtomicInteger numContainers;
  private AtomicInteger numContainersAllocated = new AtomicInteger(0);
  private String benchmarkName;
  private BenchmarkConfigurator benchmarkConfigurator;
  private boolean initialized = false;
  private Map<ContainerId, NodeId> allocatedContainers = new ConcurrentHashMap<>();

  FDMessagingBenchAppMaster(ContainerId containerId, Config rawBenchConf) {
    this.containerId = containerId;
    this.yarnConf = new YarnConfiguration();
    this.nmClient = NMClient.createNMClient();
    this.rawBenchConf = rawBenchConf;
  }

  void init() {
    log.info("initializing FDMessagingBench AM in container " + containerId);
    BenchConfig benchConfig = new BenchConfig(rawBenchConf);

    this.benchmarkName = benchConfig.getName();
    this.numContainers = new AtomicInteger(benchConfig.getParallelism());

    log.info("benchmark name: " + benchmarkName);
    log.info("parallelism: " + numContainers);

    try {
      BenchmarkConfiguratorFactory benchmarkConfiguratorFactory = Utils.instantiate(benchConfig.getBenchmarkTaskConfiguratorFactoryClass(), BenchmarkConfiguratorFactory.class);
      this.benchmarkConfigurator = benchmarkConfiguratorFactory.getConfigurator(benchConfig.getParallelism(), rawBenchConf);
      this.benchmarkConfigurator.configureBenchmark();
    } catch (Exception e) {
      throw new FDBenchException("Cannot load task configurator factory.", e);
    }

    log.info("initializing and starting node manager client....");
    this.nmClient.init(yarnConf);
    this.nmClient.start();
    initialized = true;
    log.info("AM initialization completed.");
  }

  public void run() {
    if (!initialized) {
      throw new FDBenchException("Please initialized the application master first.");
    }

    try {
      AMRMClientAsync<AMRMClient.ContainerRequest> rmClient = AMRMClientAsync.createAMRMClientAsync(100, this);
      rmClient.init(yarnConf);
      rmClient.start();

      // Register with ResourceManager
      log.info("registering application master");
      rmClient.registerApplicationMaster("", 0, "");
      log.info("application master registered.");

      Priority priority = Records.newRecord(Priority.class);
      priority.setPriority(0);

      YarnConfig benchYarnConfig = new YarnConfig(rawBenchConf);

      Resource capability = Records.newRecord(Resource.class);
      capability.setMemory(benchYarnConfig.getContainerMaxMemory());
      capability.setVirtualCores(benchYarnConfig.getContainerMaxCPUCores());

      for (int i = 0; i < numContainers.get(); i++) {
        AMRMClient.ContainerRequest containerRequest = new AMRMClient.ContainerRequest(capability, null, null, priority);
        log.info("making resource request " + i);
        rmClient.addContainerRequest(containerRequest);
      }
      // Wait for the children to finish
      while (numContainers.get() > 0) {
        Thread.sleep(2000);
      }

      // Unregister application master and exit
      rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "", "");
      log.info("application master unregistered.");
    } catch (Exception e) {
      throw new FDBenchException(e);
    }
  }

  public static void main(String[] args) throws Exception {
    String containerIdStr = System.getenv(ApplicationConstants.Environment.CONTAINER_ID.toString());
    ContainerId containerId = ConverterUtils.toContainerId(containerIdStr);
    File workingDir = new File(System.getenv(ApplicationConstants.Environment.PWD.toString()));
    File configuration = new File(workingDir, "__bench.conf");

    log.info("Package path: " + System.getenv(Constants.KBENCH_PACKAGE_PATH_ENV));
    log.info("Conf path: " + System.getenv(Constants.KBENCH_CONF_PATH_ENV));

    FDMessagingBenchAppMaster appMaster = new FDMessagingBenchAppMaster(containerId, ConfigFactory.parseFile(configuration));
    appMaster.init();
    appMaster.run();
  }

  // Begin AMRMClientAsync.CallbackHandler interface

  @Override
  public void onContainersCompleted(List<ContainerStatus> statuses) {
    for (ContainerStatus containerStatus : statuses) {
      log.info("Container " + ConverterUtils.toString(containerStatus.getContainerId()) +
          " completed with exit code " + containerStatus.getExitStatus() + " and status " + containerStatus.getState());
      numContainers.decrementAndGet();
    }
  }

  @Override
  public void onContainersAllocated(List<Container> containers) {
    for (Container container : containers) {
      try {
        log.info("Container " + ConverterUtils.toString(container.getId()) + " allocated at " + container.getNodeId());
        ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);

        ctx.setCommands(Collections.singletonList(String.format(
            "export KBENCH_LOG_DIR=%s && ln -sfn %s logs && exec ./__package/bin/run-container.sh 1>logs/%s 2>logs/%s",
            "<LOG_DIR>", "<LOG_DIR>", "stdout", "stderr")));

        Map<String, String> envMap = new HashMap<>();
        envMap.put(Constants.KBENCH_CONTAINER_ID_ENV, ConverterUtils.toString(container.getId()));
        envMap.put(Constants.KBENCH_TASK_ID_ENV, String.valueOf(numContainersAllocated.get()));
        envMap.put(Constants.KBENCH_BENCH_NAME_ENV, benchmarkName);

        // BenchmarkConfigurator should generate environment variables that can be used in the benchmark task
        envMap.putAll(benchmarkConfigurator.configureTask(numContainersAllocated.get()));

        Map<String, LocalResource> localResourceMap = new HashMap<>();
        localResourceMap.put("__package", localizeAppPackage(System.getenv(Constants.KBENCH_PACKAGE_PATH_ENV).trim()));
        localResourceMap.put("__bench.conf", localizeAppConf(System.getenv(Constants.KBENCH_CONF_PATH_ENV).trim()));

        ctx.setEnvironment(envMap);
        ctx.setLocalResources(localResourceMap);

        nmClient.startContainer(container, ctx);
        allocatedContainers.put(container.getId(), container.getNodeId());
        numContainersAllocated.incrementAndGet();
      } catch (Exception e) {
        log.error("Couldn't launch container " + ConverterUtils.toString(container.getId()), e);
        cleanupContainers();
        numContainers.set(0);
      }
    }
  }

  private void cleanupContainers() {
    log.info("Cleaning up previously allocated containers....");
    for (Map.Entry<ContainerId, NodeId> e : allocatedContainers.entrySet()) {
      try {
        nmClient.stopContainer(e.getKey(), e.getValue());
      } catch (Exception ex) {
        log.error("Couldn't cleanup container " + e.getKey() + " in node " + e.getValue(), ex);
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
    log.info("AM shutdown was requested.");
    cleanupContainers();
    numContainers.set(0);
    nmClient.stop();
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
    log.error("Application master received an error.");
    cleanupContainers();
    numContainers.set(0);
  }

  // End AMRMClientAsync.CallbackHandler interface
}
