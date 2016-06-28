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

package org.pathirage.fdbench.metrics;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import org.apache.samza.metrics.Counter;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;

public class MetricsTest {
  private static final Logger log = LoggerFactory.getLogger(MetricsTest.class);

  @Test
  public void testFSMetricsReporter() throws IOException {
    Config config = ConfigFactory.load(getClass().getClassLoader(), "metrics-test.conf");
    Path snapshotsDirectory = Files.createTempDirectory("metricssnapshots");

    FSMetricsSnapshotReporter reporter = (FSMetricsSnapshotReporter) new FSMetricsSnapshotReporterFactory()
        .getMetricsReporter("test", "test-container-1", config.withValue("metrics.reporter.test.snapshots.directory",
            ConfigValueFactory.fromAnyRef(snapshotsDirectory.toString())));

    InMemoryMetricsRegistry metricsRegistry = new InMemoryMetricsRegistry();

    reporter.register("metricstest", metricsRegistry);
    reporter.start();

    Counter c = metricsRegistry.newCounter("test", "simplecount");
    c.inc();
    c.inc();

    try {
      Thread.sleep(11 * 1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    reporter.stop();

    Assert.assertTrue(Files.list(snapshotsDirectory).count() > 0);
  }

}
