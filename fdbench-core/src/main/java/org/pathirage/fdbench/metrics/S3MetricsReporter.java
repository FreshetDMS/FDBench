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

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.google.gson.Gson;
import org.apache.commons.lang3.tuple.Pair;
import org.pathirage.fdbench.metrics.api.MetricsRegistry;
import org.pathirage.fdbench.metrics.api.MetricsReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class S3MetricsReporter extends AbstractMetricsSnapshotReporter implements MetricsReporter, Runnable {
  private static final Logger log = LoggerFactory.getLogger(S3MetricsReporter.class);

  private static final String CONTENT_TYPE = "application/json";

  private final AmazonS3 s3Client;
  private final String bucketName;

  public S3MetricsReporter(String name, String jobName, String containerName, S3MetricsReporterFactory.S3MetricsReporterConfig config) {
    super(name, jobName, containerName, config.getReportingInterval(), Executors.newScheduledThreadPool(1, new ThreadFactory() {
      @Override
      public Thread newThread(Runnable r) {
        Thread thread = new Thread(r);
        thread.setDaemon(true);
        thread.setName("FDBenchMessaging-S3MetricsReporter");
        return thread;
      }
    }));

    AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
    builder.setRegion(config.getAWSRegion());
    builder.setCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(config.getAWSAccessKeyId(), config.getAWSAccessKeySecret())));
    s3Client = builder.build();
    bucketName = String.format("%s-%s", config.getS3BucketNamePrefix(), jobName);
  }


  @Override
  public void start() {
    createBucket(bucketName);
    super.start();
  }

  @Override
  public void flush() {
    run();
  }

  @Override
  public void run() {
    try {
      for (Pair<String, MetricsRegistry> registry : registries) {
        log.info("Flushing metrics for " + registry.getValue());

        Map<String, Map<String, Object>> metricsEvent = metricRegistryToMap(registry.getValue());
        Map<String, Object> metricsEventRoot = new HashMap<>();

        long recordingTime = System.currentTimeMillis();
        metricsEventRoot.put("BenchName", jobName);
        metricsEventRoot.put("Container", containerName);
        metricsEventRoot.put("Timestamp", recordingTime);
        metricsEventRoot.put("TotalMem", getTotalMemory());
        metricsEventRoot.put("FreeMem", getFreeMemory());
        metricsEventRoot.put("UsedMem", getUsedMemory());
        metricsEventRoot.put("MaxMem", getMaxMemory());
        metricsEventRoot.put("JVMCPUTime", getJVMCPUTime());
        metricsEventRoot.put("Snapshot", metricsEvent);


        if (log.isDebugEnabled()) {
          log.debug("Putting an item for registry " + registry.getKey() + " with id " + recordingTime);
        }

        String eventJson = new Gson().toJson(metricsEventRoot);
        byte[] content = eventJson.getBytes();
        InputStream fileInputStream = new ByteArrayInputStream(content);
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentType(CONTENT_TYPE);
        metadata.setContentLength(content.length);
        PutObjectResult result = s3Client.putObject(new PutObjectRequest(bucketName,
            String.format("%s-%s-%s", jobName, containerName, recordingTime),
            fileInputStream,
            metadata));

        if (log.isDebugEnabled()) {
          log.debug("Done putting the item for registry " + registry.getKey() + " with the id " + recordingTime);
        }

        if (log.isDebugEnabled()) {
          log.debug("Published metrics snapshot to S3 bucket " + bucketName + " with outcome " + result.toString());
        }
      }
    } catch (Exception e) {
      String errMessage = "Error occurred while publishing metrics to S3 bucket " + bucketName;
      log.error(errMessage, e);
      throw new RuntimeException(errMessage, e);
    }
  }

  private void createBucket(String bucketName) {
    if (!s3Client.doesBucketExist(bucketName)) {
      try {
        s3Client.createBucket(new CreateBucketRequest(bucketName));
      } catch (Exception e) {
        log.warn("Bucket creation failed. This may be due to existing bucket.", e);
      }
    }
  }
}
