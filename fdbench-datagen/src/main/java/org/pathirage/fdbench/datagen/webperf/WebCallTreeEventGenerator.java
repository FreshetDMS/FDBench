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

package org.pathirage.fdbench.datagen.webperf;

import org.pathirage.fdbench.datagen.api.DataGenerator;

/**
 * This is a data generator that simulates a web application request logs of a SOA based application
 * involving multiple front-end servers and multiple back-end services that form a call-tree
 * to serve requests from front-end servers. Front-end servers calls back-end services to serve requests
 * from users for different pages in the website. This generator tries to simulate environment discussed
 * in <a href="http://goo.gl/0jU1ko">http://goo.gl/0jU1ko</a>.
 *
 * Multiple instances of this generator will run in parallel.
 */
public class WebCallTreeEventGenerator implements DataGenerator {
  private final Mode mode;
  private final int maxCallTreeDepth;
  private final int maxCallTreeBranchingFactor;
  private final int numberOfFrontEndServers;
  private final int numberOfWebPages;
  private final int generatorParallelism;
  private final int maxDeliveryDelayMilliseconds;

  public WebCallTreeEventGenerator(Mode mode, int maxCallTreeDepth, int maxCallTreeBranchingFactor,
                                   int numberOfFrontEndServers, int numberOfWebPages, int generatorParallelism,
                                   int maxDeliveryDelayMilliseconds) {
    this.mode = mode;
    this.maxCallTreeDepth = maxCallTreeDepth;
    this.maxCallTreeBranchingFactor = maxCallTreeBranchingFactor;
    this.numberOfFrontEndServers = numberOfFrontEndServers;
    this.numberOfWebPages = numberOfWebPages;
    this.generatorParallelism = generatorParallelism;
    this.maxDeliveryDelayMilliseconds = maxDeliveryDelayMilliseconds;
  }

  @Override
  public void run() {

  }
}
