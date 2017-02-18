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

package org.pathirage.fdbench.kafka.perfmodeling;

public class SyntheticWorkloadGeneratorConstants {
  public static final String ENV_KAFKA_WORKLOAD_GENERATOR_TASK_TYPE = "FDBENCH_KAFKA_WORKLOAD_GENERATOR_TASK_TYPE";
  public static final String ENV_KAFKA_WORKLOAD_GENERATOR_MESSAGE_RATE = "FDBENCH_KAFKA_WORKLOAD_GENERATOR_MESSAGE_RATE";
  public static final String ENV_KAFKA_WORKLOAD_GENERATOR_TASK_DELAY = "FDBENCH_KAFKA_WORKLOAD_GENERATOR_TASK_DELAY";
  public static final String ENV_KAFKA_WORKLOAD_GENERATOR_TASK_GROUP = "FDBENCH_KAFKA_WORKLOAD_GENERATOR_TASK_GROUP";
  public static final String ENV_KAFKA_WORKLOAD_GENERATOR_MSG_SIZE_MEAN = "FDBENCH_KAFKA_WORKLOAD_GENERATOR_MSG_SIZE_MEAN";
  public static final String ENV_KAFKA_WORKLOAD_GENERATOR_MSG_SIZE_STDEV = "FDBENCH_KAFKA_WORKLOAD_GENERATOR_MSG_SIZE_STDEV";
  public static final String ENV_KAFKA_WORKLOAD_GENERATOR_MSG_SIZE_DIST = "FDBENCH_KAFKA_WORKLOAD_GENERATOR_MSG_SIZE_DIST";
  public static final String ENV_KAFKA_WORKLOAD_GENERATOR_MSG_PROC_MEAN = "FDBENCH_KAFKA_WORKLOAD_GENERATOR_MSG_PROC_MEAN";
  public static final String ENV_KAFKA_WORKLOAD_GENERATOR_MSG_PROC_STDEV = "FDBENCH_KAFKA_WORKLOAD_GENERATOR_MSG_PROC_STDEV";
  public static final String ENV_KAFKA_WORKLOAD_GENERATOR_MSG_PROC_DIST = "FDBENCH_KAFKA_WORKLOAD_GENERATOR_MSG_PROC_DIST";
}
