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

package org.pathirage.fdbench.samza;

import org.apache.samza.config.Config;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.task.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class SamzaReadThroughputTask implements StreamTask, InitableTask {
  private static final Logger log = LoggerFactory.getLogger(SamzaReadThroughputTask.class);

  private static final String SLEEP_BASED_SIMULATION_ENABLED = "fdbench.sleep.based.simulation.enabled";
  private static final String CONSTANT_SLEEP_TIME = "fdbench.sleep.based.simulation.constant";
  private static final String VARIABLE_SLEEP_TIME_MAX = "fdbench.sleep.based.simulation.max";
  private static final String VARIABLE_SLEEP_TIME_MIN = "fdbench.sleep.based.simulation.min";

  private final Random random = new Random(System.currentTimeMillis());
  private boolean sleepBasedSimulation = false;
  private boolean constantSleepTime = false;
  private int sleepMills = 20;
  private int sleepMaxMills = Integer.MAX_VALUE;
  private int sleepMinMills = Integer.MIN_VALUE;

  private AtomicInteger messageCount = new AtomicInteger(0);

  @Override
  public void init(Config config, TaskContext context) throws Exception {
    sleepBasedSimulation = config.getBoolean(SLEEP_BASED_SIMULATION_ENABLED, false);
    Integer sleepTimeConstant = config.getInt(CONSTANT_SLEEP_TIME);
    if (sleepTimeConstant != null) {
      constantSleepTime = true;
      sleepMills = sleepTimeConstant;
    }

    Integer maxSleepTime = config.getInt(VARIABLE_SLEEP_TIME_MAX);
    Integer minSleepTime = config.getInt(VARIABLE_SLEEP_TIME_MIN);

    if (maxSleepTime == null || minSleepTime == null) {
      constantSleepTime = true;
    } else {
      sleepMaxMills = maxSleepTime;
      sleepMinMills = minSleepTime;
    }
  }

  @Override
  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
    if (sleepBasedSimulation) {
      if (constantSleepTime) {
        Thread.sleep(sleepMills);
      } else {
        Thread.sleep(sleepMinMills + random.nextInt(sleepMaxMills));
      }
    }

    int count = messageCount.incrementAndGet();
    if (count % 100000 == 0) {
      log.info("Read " + count + " messages.");
    }

  }
}
