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
import freemarker.template.SimpleHash;
import freemarker.template.Template;
import freemarker.template.TemplateException;

import java.io.IOException;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RolesPlay extends AnsiblePlay {

  private final List<String> roles;
  private final Map<String, Object> vars;

  public RolesPlay(Config config, List<String> roles, Map<String, Object> vars) {
    super(config);
    this.roles = roles;
    this.vars = vars;
  }

  @Override
  public String genYAML() throws IOException, TemplateException {
    Template ec2Template = freemarkerConfig.getTemplate("roles.ftl");

    StringWriter stringWriter = new StringWriter();

    ec2Template.process(toMap(), stringWriter);

    return stringWriter.toString();
  }

  @Override
  public Map<String, Object> toMap() {
    Map<String, Object> data = new HashMap<String, Object>();

    data.putAll(super.toMap());
    data.put("roles", roles);
    data.put("vars", vars);

    return data;
  }
}
