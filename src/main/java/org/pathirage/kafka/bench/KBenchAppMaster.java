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

package org.pathirage.kafka.bench;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class KBenchAppMaster implements AMRMClientAsync.CallbackHandler, NMClientAsync.CallbackHandler {
  private static final Logger log = LoggerFactory.getLogger(KBenchAppMaster.class);

  private final ContainerId containerId;
  private final ApplicationAttemptId attemptId;
  private final Configuration conf;
  private final NMClientAsync nmClient;

  public KBenchAppMaster(ContainerId containerId, ApplicationAttemptId attemptId) {
    this.containerId = containerId;
    this.attemptId = attemptId;
    this.conf = new YarnConfiguration();
    this.nmClient = NMClientAsync.createNMClientAsync(this);
  }

  @Override
  public void onContainersCompleted(List<ContainerStatus> statuses) {

  }

  @Override
  public void onContainersAllocated(List<Container> containers) {

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

  public void mainLoop() throws Exception {
    AMRMClientAsync<AMRMClient.ContainerRequest> rmClient = AMRMClientAsync.createAMRMClientAsync(100, this);
    rmClient.init(conf);
    rmClient.start();

    // Register with ResourceManager
    log.info("[KBench-AM] registering application master");
    rmClient.registerApplicationMaster("", 0, "");
    log.info("[KBench-AM] application master registered.");

    // Process benchmark containers and request containers

    // Wait for the children to finish

    Thread.sleep(20000);

    // Unregister application master and exit
    rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "", "");
    log.info("[KBench-AM] application master unregistered.");
  }

  public static void main(String[] args) throws Exception {
    String containerIdStr = System.getenv(ApplicationConstants.Environment.CONTAINER_ID.toString());
    log.info(String.format("Got container id: %s", containerIdStr));
    ContainerId containerId = ConverterUtils.toContainerId(containerIdStr);
    ApplicationAttemptId attemptId = containerId.getApplicationAttemptId();
    log.info(String.format("Got app attempt id: %s", attemptId));

    KBenchAppMaster appMaster = new KBenchAppMaster(containerId, attemptId);
    appMaster.mainLoop();
  }

  @Override
  public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> allServiceResponse) {

  }

  @Override
  public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {

  }

  @Override
  public void onContainerStopped(ContainerId containerId) {

  }

  @Override
  public void onStartContainerError(ContainerId containerId, Throwable t) {

  }

  @Override
  public void onGetContainerStatusError(ContainerId containerId, Throwable t) {

  }

  @Override
  public void onStopContainerError(ContainerId containerId, Throwable t) {

  }
}
