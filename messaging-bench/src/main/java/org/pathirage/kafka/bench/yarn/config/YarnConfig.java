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

package org.pathirage.kafka.bench.yarn.config;

import com.typesafe.config.Config;
import org.pathirage.kafka.bench.config.AbstractConfig;

public class YarnConfig extends AbstractConfig {
  private static final String PACKAGE_PATH = "yarn.package.path";
  private static final String CONTAINER_MAX_MEMORY = "yarn.container.max.memory";
  private static final String CONTAINER_MAX_CPU_CORES = "yarn.container.cpu.cores";
  private static final String QUEUE_NAME = "yarn.queue";
  private static final String AM_JVM_OPTIONS = "yarn.am.opts";
  private static final String AM_CONTAINER_MAX_MEMORY = "yarn.am.container.memory.mb";
  private static final String AM_CONTAINER_MAX_CPU_CORES = "yarn.am.container.cpu.cores";

  public YarnConfig(Config config) {
    super(config);
  }

  public String getPackagePath() {
    return getString(PACKAGE_PATH);
  }

  public int getContainerMaxMemory() {
    return getInt(CONTAINER_MAX_MEMORY, 512);
  }

  public int getContainerMaxCPUCores() {
    return getInt(CONTAINER_MAX_CPU_CORES, 1);
  }

  public String getQueueName() {
    return getString(QUEUE_NAME, "default");
  }

  public String getAMJVMOptions() {
    return getString(AM_JVM_OPTIONS, "");
  }

  public int getAMContainerMaxMemory() {
    return getInt(AM_CONTAINER_MAX_MEMORY, 512);
  }

  public int getAMContainerMaxCPUCores() {
    return getInt(AM_CONTAINER_MAX_CPU_CORES, 1);
  }
}
