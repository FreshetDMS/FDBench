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

package org.pathirage.fdbench.datagen.api;

/**
 * Defines the interface for a data generator that generates one or more event streams
 * imitating a real-world system such as logs from a cluster of web applications serving
 * one or more user facing services.
 */
public interface DataGenerator extends Runnable {
  public static enum Mode {
    /**
     * Imitate a real-world system. In this mode data generator
     * will be active during benchmarking.
     */
    SIMULATION,

    /**
     * Generate a historical event stream. Generates event streams
     * depicting a given time window as fast as possible. In this mode
     * data generator will be used to generate sample data set that will
     * be replayed during benchmarking.
     */
    GENERATION
  }
}
