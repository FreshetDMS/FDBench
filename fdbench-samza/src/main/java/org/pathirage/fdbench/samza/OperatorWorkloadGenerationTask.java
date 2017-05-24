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
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

/**
 * StreamTask for generating different types of query workloads including constant service times,
 * and exponential service times. Support following workloads:
 *   - Constant service time
 *   - Exponential service time
 *   - Empty
 */
public class OperatorWorkloadGenerationTask implements StreamTask, InitableTask{
  private static Logger log = LoggerFactory.getLogger(OperatorWorkloadGenerationTask.class);

  private static final String SERVICE_TIME_DISTRIBUTION = "streaming.operator.workload.service.time.dist";
  private static final String SERVICE_TIME_SCALE = "streaming.operator.workload.service.time.scale";
  private static final String RESULT_TOPIC = "streaming.operator.workload.result.topic";
  private static final String RESULT_SYSTEM = "streaming.operator.workload.result.system";

  private ServiceTimeDist serviceTimeDist;
  private int serviceTimeScale;
  private String resultTopic;
  private String resultSystem;
  private final Random random = new Random(System.currentTimeMillis());

  @Override
  public void init(Config config, TaskContext context) throws Exception {
    serviceTimeDist = ServiceTimeDist.valueOf(config.get(SERVICE_TIME_DISTRIBUTION, "NOPROCESSING"));
    serviceTimeScale = config.getInt(SERVICE_TIME_SCALE, 100);
    resultTopic = config.get(RESULT_TOPIC, "e2elatency-topic");
    resultSystem = config.get(RESULT_SYSTEM, "kafka");
  }

  @Override
  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
    if (serviceTimeDist == ServiceTimeDist.CONSTANT) {
      busyWait(serviceTimeScale);
    } else if (serviceTimeDist == ServiceTimeDist.NOPROCESSING) {
      // No processing
    } else {
      throw new RuntimeException("Not supported yet.");
    }

    collector.send(new OutgoingMessageEnvelope(new SystemStream(resultSystem, resultTopic), envelope.getKey(), envelope.getMessage()));
  }

  private void busyWait(int scale){
    double iterations = Math.pow(2, scale);
    int result = 0;
    for (double i = 0; i < iterations; i++) {
      result = random.nextInt(2000) * random.nextInt(500);
    }

    log.info("Result: " + result);
  }

  public static enum ServiceTimeDist {
    CONSTANT,
    EXPONENTIAL,
    NOPROCESSING
  }
}
