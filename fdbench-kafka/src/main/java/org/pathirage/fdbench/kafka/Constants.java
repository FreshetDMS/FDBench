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

package org.pathirage.fdbench.kafka;

public class Constants {
  public static final String ENV_PARTITIONS = "E2EBENCH_PARTITIONS";
  public static final String ENV_TOPIC = "E2EBENCH_TOPIC";
  public static final String ENV_BROKERS = "E2EBENCH_BROKERS";
  public static final String ENV_ZK = "E2EBENCH_ZK";

  public static final long MAX_RECORDABLE_LATENCY = 300000000000L;
  public static final int SIGNIFICANT_VALUE_DIGITS = 5;
}
