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

package org.pathirage.fdbench.samza.metrics;

import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MetricsConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.metrics.MetricsReporter;
import org.apache.samza.metrics.MetricsReporterFactory;
import scala.Function0;
import scala.Option;

public class DynamoDBSamzaMetricsSnapshotReporterFactory implements MetricsReporterFactory {
  private static final String AWS_ACCESS_KEY_ID = "aws.access.key.id";
  private static final String AWS_ACCESS_KEY_SECRET = "aws.access.key.secret";

  @Override
  public MetricsReporter getMetricsReporter(String name, String containerName, Config config) {
    JobConfig jobConfig = new JobConfig(config);
    TaskConfig taskConfig = new TaskConfig(config);
    MetricsConfig metricsConfig = new MetricsConfig(config);

    String jobName = jobConfig.getName().getOrElse(new Function0<String>() {
      @Override
      public String apply() {
        throw new SamzaException("Job must be defined in config.");
      }
    });

    String jobId = jobConfig.getJobId().getOrElse(new Function0<String>() {
      @Override
      public String apply() {
        return new Integer(1).toString();
      }
    });

    String taskClass = taskConfig.getTaskClass().getOrElse(new Function0<String>() {
      @Override
      public String apply() {
        throw new SamzaException("No task class defined in config.");
      }
    });

    String version;
    try {
      version = Option.apply(Class.forName(taskClass).getPackage().getImplementationVersion()).getOrElse(new Function0<String>() {
        @Override
        public String apply() {
          return "0.0.1";
        }
      });
    } catch (ClassNotFoundException e) {
      throw new SamzaException("Task class not found.", e);
    }

    String metricsTableName = metricsConfig.getMetricsReporterStream(name).getOrElse(new Function0<String>() {
      @Override
      public String apply() {
        throw new SamzaException("No metrics table defined in config.");
      }
    });

    Integer pollingInterval = new Integer(metricsConfig.getMetricsReporterInterval(name).getOrElse(new Function0<String>() {
      @Override
      public String apply() {
        return "60";
      }
    }));

    String awsKeyId = config.get(AWS_ACCESS_KEY_ID);
    if (awsKeyId == null) {
      throw new SamzaException("No AWS Key ID defined.");
    }

    String awsKeySecret = config.get(AWS_ACCESS_KEY_SECRET);
    if (awsKeySecret == null) {
      throw new SamzaException("No AWS Key Secret defined.");
    }

    return new DynamoDBSamzaMetricsSnapshotReporter(name, containerName, jobName, jobId, version, metricsTableName,
        awsKeyId, awsKeySecret);
  }
}
