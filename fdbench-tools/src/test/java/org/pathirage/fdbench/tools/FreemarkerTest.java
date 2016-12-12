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

package org.pathirage.fdbench.tools;

import freemarker.core.ParseException;
import freemarker.template.*;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.StringWriter;
import java.util.*;


public class FreemarkerTest {

  @Test
  public void testSimpleTemplate() throws IOException, TemplateException {
    Configuration config = new Configuration(Configuration.VERSION_2_3_25);
    config.setClassForTemplateLoading(FreemarkerTest.class, "/org/pathirage/fdbench/templates");

    Template template = config.getTemplate("throughput.ftl");
    Map<String, Object> data = new HashMap<>();

    List<Integer> latencies = new ArrayList<>();
    latencies.add(123);
    latencies.add(210);
    latencies.add(280);

    List<String> labels = new ArrayList<>();
    labels.add("50");
    labels.add("70");
    labels.add("90");

    data.put("successLabels", labels);
    data.put("successLatencies", latencies);
    data.put("benchmark", "Test Template");
    data.put("containermemusage", Collections.emptyList());

    StringWriter sw = new StringWriter();

    template.process(data, sw);
    sw.flush();

    String output = sw.toString();

    Assert.assertTrue(output.contains("50"));
    Assert.assertTrue(output.contains("70"));
  }
}
