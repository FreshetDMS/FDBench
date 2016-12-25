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

package org.pathirage.fdbench.automation.plays;

import com.typesafe.config.Config;
import freemarker.template.Configuration;
import org.pathirage.fdbench.automation.YAMLGenerator;

import java.util.HashMap;
import java.util.Map;

public abstract class AnsiblePlay implements YAMLGenerator {

  protected final Config config;

  protected final Configuration freemarkerConfig = new Configuration(Configuration.VERSION_2_3_25);

  public AnsiblePlay(Config config) {
    this.config = config;
    freemarkerConfig.setClassForTemplateLoading(AnsiblePlay.class, "/org/pathirage/fdbench/automation/templates");
  }

  public Map<String, Object> toMap() {
    Map<String, Object> data = new HashMap<>();

    data.put("ansible", config.getValue("ansible").unwrapped());

    return data;
  }
}
