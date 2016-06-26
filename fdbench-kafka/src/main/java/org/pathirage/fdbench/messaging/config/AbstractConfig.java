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

package org.pathirage.fdbench.messaging.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;

public abstract class AbstractConfig {
  final Config config;

  public AbstractConfig(Config config) {
    this.config = config;
  }

  public String getString(String path) {
    return config.getString(path);
  }

  public String getString(String path, String defaultValue) {
    try {
      return getString(path);
    } catch (ConfigException.Missing e) {
      return defaultValue;
    }
  }

  public int getInt(String path) {
    return config.getInt(path);
  }

  public int getInt(String path, int defaultValue) {
    try{
      return getInt(path);
    } catch (ConfigException.Missing e) {
      return defaultValue;
    }
  }

  public Config getRawConfig() {
    return config;
  }
}
