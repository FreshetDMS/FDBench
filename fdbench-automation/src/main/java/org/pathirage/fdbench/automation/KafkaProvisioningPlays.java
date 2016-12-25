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

package org.pathirage.fdbench.automation;

import com.typesafe.config.Config;
import freemarker.template.TemplateException;
import org.pathirage.fdbench.automation.plays.EC2Play;
import org.pathirage.fdbench.automation.plays.RolesPlay;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

public class KafkaProvisioningPlays implements YAMLGenerator  {

  private final Config config;
  private final Config kafkaConfig;

  public KafkaProvisioningPlays(Config config) {
    this.config = config;
    this.kafkaConfig = config.getConfig("experiment.services.kafka");
  }

  @Override
  public String genYAML() throws IOException, TemplateException {
    String createInstance = new EC2Play(kafkaConfig).genYAML();
    String setupEnv = new RolesPlay(kafkaConfig, Arrays.<String>asList(new String[]{"makefs", "jdk"}),
        (Map<String, Object>)kafkaConfig.getValue("config").unwrapped()).genYAML();
    return createInstance + "\n" + setupEnv;
  }
}
