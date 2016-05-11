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

package org.pathirage.kafka.bench.yarn;

import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

public class KBenchContainer {
  private static final Logger log = LoggerFactory.getLogger(KBenchContainer.class);

  public static void main(String[] args) throws IOException {
    log.info("Hi from first container!");

    File workingDir = new File(System.getenv(ApplicationConstants.Environment.PWD.toString()));
    File configuration = new File(workingDir, "__bench.conf");

    log.info("Printing configuration file content:");

    try (BufferedReader br = new BufferedReader(new FileReader(configuration))) {
      String line = null;
      while ((line = br.readLine()) != null) {
        log.info("[LOG] " + line);
      }
    }
  }
}
