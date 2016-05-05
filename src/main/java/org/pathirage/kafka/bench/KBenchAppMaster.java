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

import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class KBenchAppMaster implements AMRMClientAsync.CallbackHandler {
  private static final Logger log = LoggerFactory.getLogger(KBenchAppMaster.class);

  private final String containerId;

  public KBenchAppMaster(String containerId) {
    this.containerId = containerId;
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

  public static void main(String[] args) {
    String containerIdStr = System.getenv(ApplicationConstants.Environment.CONTAINER_ID.toString());
    log.info(String.format("Got container id: %s", containerIdStr));
    ContainerId containerId = ConverterUtils.toContainerId(containerIdStr);
    ApplicationAttemptId attemptId = containerId.getApplicationAttemptId();
    log.info(String.format("Got app attempt id: %s", attemptId));
    String nodeManagerHost = System.getenv(ApplicationConstants.Environment.NM_HOST.toString());
    log.info(String.format("Got node manager host: %s", nodeManagerHost));
  }
}
