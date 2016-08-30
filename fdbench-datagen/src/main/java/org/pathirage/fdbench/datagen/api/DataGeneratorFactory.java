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

import com.typesafe.config.Config;

/**
 * Creates a {@link DataGenerator} instance.
 */
public interface DataGeneratorFactory {
  /**
   * Create an instance of {@link DataGenerator}.
   * @param mode In which mode this generator instance should operate
   * @param config Data generator config
   * @return an instance of {@link DataGenerator}
   */
  DataGenerator create(DataGenerator.Mode mode, Config config);
}
