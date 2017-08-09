/**
 * Copyright 2017 Milinda Pathirage
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


import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.uncommons.maths.random.ExponentialGenerator;
import org.uncommons.maths.random.PoissonGenerator;

import java.util.Random;

public class SatisticalDistributions {
  public static void main(String[] args) {
    String fileName = "poisson.txt";
    DescriptiveStatistics ds = new DescriptiveStatistics();
    ExponentialGenerator pg = new ExponentialGenerator(10, new Random(System.currentTimeMillis()));

    for (int i = 0; i < 1000000; i++) {
      ds.addValue(pg.nextValue());
    }

    System.out.println(1/ds.getMean());
  }
}
