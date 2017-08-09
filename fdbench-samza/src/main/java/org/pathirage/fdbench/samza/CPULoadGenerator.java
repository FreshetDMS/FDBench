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

import java.util.Random;

public class CPULoadGenerator {
  static Random random = new Random(System.currentTimeMillis());

  public static void main(String[] args) {
    int multiplications = 4096;
    Random random = new Random(System.currentTimeMillis());
    long result = 0;
    long start = System.currentTimeMillis();
    for (int j = 0; j < 1000; j++) {
      for (int i = 0; i < multiplications; i++) {
        result = random.nextInt(1000) * random.nextInt(200);
      }
    }
    long end = System.currentTimeMillis();

//    System.out.println(result);
    System.out.println("Time: " + new Float(end - start)/1000 + " microseconds");

    // Single multiplication approximately takes 0.019 nanoseconds
  }

  private static int randomExp() {
    float u = random.nextFloat();
    return new Double(Math.log(1 - u)/-0.00001).intValue();
  }
}
