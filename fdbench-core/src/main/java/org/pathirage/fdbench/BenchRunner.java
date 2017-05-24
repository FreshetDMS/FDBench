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

package org.pathirage.fdbench;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.commons.cli.*;
import org.pathirage.fdbench.api.BenchmarkJob;
import org.pathirage.fdbench.config.BenchConfig;
import org.pathirage.fdbench.api.BenchmarkJobFactory;
import org.pathirage.fdbench.utils.Utils;

import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class BenchRunner {
  public static void main(String[] args) throws FileNotFoundException, ParseException, IllegalAccessException, InstantiationException, ClassNotFoundException {
    Options options = new Options();
    options.addOption("c", "config-file", true, "KBench configuration file");

    CommandLineParser cmdParser = new DefaultParser();
    CommandLine cmd = cmdParser.parse(options, args);
    if(cmd.hasOption("c")){
      Path configFilePath = Paths.get(cmd.getOptionValue("c"));

      if(Files.notExists(configFilePath)) {
        throw new FDBenchException("Config file " + configFilePath.toString() + " does not exist.");
      }

      Config rawConfig = ConfigFactory.parseFile(configFilePath.toFile());
      BenchConfig config = new BenchConfig(rawConfig);

      BenchmarkJobFactory jobFactory = Utils.instantiate(config.getJobFactoryClass(), BenchmarkJobFactory.class);

      BenchmarkJob job = jobFactory.getJob(config.getName(), rawConfig);
      job.submit(configFilePath);
    } else {
      String header = "Execute Kafka benchmarks\n\n";
      String footer = "\nPlease report issues at https://github.com/milinda/FDBench/issues";

      HelpFormatter helpFormatter = new HelpFormatter();
      helpFormatter.printHelp("kbench", header, options, footer, true);
    }
  }
}
