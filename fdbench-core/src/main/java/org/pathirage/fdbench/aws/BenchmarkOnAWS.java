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

package org.pathirage.fdbench.aws;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
import com.amazonaws.services.cloudwatch.model.Datapoint;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.GetMetricStatisticsRequest;
import com.amazonaws.services.cloudwatch.model.GetMetricStatisticsResult;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.google.gson.*;
import com.google.gson.stream.JsonWriter;
import com.typesafe.config.Config;
import org.pathirage.fdbench.api.Benchmark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.*;

public abstract class BenchmarkOnAWS implements Benchmark {
  private static final Logger log = LoggerFactory.getLogger(BenchmarkOnAWS.class);

  private static final SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd-hh-mm-ss");

  private final int fifteenMinutes = 15 * 60;
  private final int oneMinute = 60;
  private final int period;

  private final AWSConfiguration config;
  private Date startTime;
  private Date endTime;
  private final AmazonCloudWatch cloudWatch;
  private final AmazonS3 s3Client;

  protected BenchmarkOnAWS(Config rawConfig) {
    this.config = new AWSConfiguration(rawConfig);
    this.cloudWatch = config.isAWSBench() ? getCloudWatchClient() : null;
    this.s3Client = config.isAWSBench() ? getS3Client() : null;
    this.period = config.getMetricsPeriod();
  }

  private AmazonCloudWatch getCloudWatchClient() {
    AmazonCloudWatchClientBuilder builder = AmazonCloudWatchClientBuilder.standard();
    builder.setCredentials(new AWSStaticCredentialsProvider(
        new BasicAWSCredentials(config.getAWSAccessKeyId(), config.getAWSAccessKeySecret())));
    builder.setRegion(config.getAWSRegion());
    return builder.build();
  }

  private AmazonS3 getS3Client() {
    AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
    builder.setRegion(config.getAWSRegion());
    builder.setCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(config.getAWSAccessKeyId(), config.getAWSAccessKeySecret())));
    return builder.build();
  }

  @Override
  public void setup() {
    startTime = new Date();
    if (config.isAWSBench()) {
      setupHook();
    }
  }

  @Override
  public void teardown() {
    endTime = new Date();
    if (config.isAWSBench()) {
      // We have to wait until another metric reporting period is over to get all the metrics from cloudwatch.
      try {
        Thread.sleep(period * 1000);
      } catch (InterruptedException e) {
        log.warn("Sleep interrupted.", e);
      }
      File additionalInfo = teardownHook();
      getAndReportCloudWatchMetrics(additionalInfo);
    }
  }

  private JsonArray metricStatisticsResultsToJson(List<GetMetricStatisticsResult> metricStatisticsResults) {
    JsonArray metricsArray = new JsonArray();
    for (GetMetricStatisticsResult metric : metricStatisticsResults) {
      JsonObject metricObj = new JsonObject();
      JsonArray dataPoints = new JsonArray();
      metricObj.add("label", new JsonPrimitive(metric.getLabel()));

      for (Datapoint dp : metric.getDatapoints()) {
        JsonObject dataPoint = new JsonObject();
        if (dp.getTimestamp() != null)
          dataPoint.add("timestamp", new JsonPrimitive(dp.getTimestamp().getTime()));
        if (dp.getSampleCount() != null)
          dataPoint.add("sample-count", new JsonPrimitive(dp.getSampleCount()));
        if (dp.getAverage() != null)
          dataPoint.add("average", new JsonPrimitive(dp.getAverage()));
        if (dp.getSum() != null)
          dataPoint.add("sum", new JsonPrimitive(dp.getSum()));
        if (dp.getMinimum() != null)
          dataPoint.add("min", new JsonPrimitive(dp.getMinimum()));
        if (dp.getMaximum() != null)
          dataPoint.add("max", new JsonPrimitive(dp.getMaximum()));
        if (dp.getUnit() != null)
          dataPoint.add("unit", new JsonPrimitive(dp.getUnit()));
        if (dp.getExtendedStatistics() != null) {
          Map<String, Double> extStats = dp.getExtendedStatistics();
          for (Map.Entry<String, Double> e : extStats.entrySet()) {
            dataPoint.add(e.getKey(), new JsonPrimitive(e.getValue()));
          }
        }

        dataPoints.add(dataPoint);
      }

      metricObj.add("data-points", dataPoints);

      metricsArray.add(metricObj);
    }

    return metricsArray;
  }

  private void getAndReportCloudWatchMetrics(File additionalInfo) {

    JsonObject metrics = new JsonObject();
    JsonArray instances = new JsonArray();
    JsonArray volumes = new JsonArray();

    Map<String, List<GetMetricStatisticsResult>> instanceMetrics = getInstanceMetrics();
    Map<String, List<GetMetricStatisticsResult>> volumeMetrics = getVolumeMetrics();

    for (String instance : instanceMetrics.keySet()) {
      JsonObject i = new JsonObject();

      i.add("instance", new JsonPrimitive(instance));
      i.add("metrics", metricStatisticsResultsToJson(instanceMetrics.get(instance)));

      instances.add(i);
    }

    metrics.add("instances", instances);

    for (String volume : volumeMetrics.keySet()) {
      JsonObject v = new JsonObject();

      v.add("volume", new JsonPrimitive(volume));
      v.add("metrics", metricStatisticsResultsToJson(volumeMetrics.get(volume)));

      volumes.add(v);
    }

    metrics.add("volumes", volumes);
    metrics.add("timestamp", new JsonPrimitive(System.currentTimeMillis()));
    metrics.add("start-time", new JsonPrimitive(startTime.getTime()));
    metrics.add("end-time", new JsonPrimitive(endTime.getTime()));

    Path tempFile = null;
    FileWriter writer = null;
    try {
      tempFile = Files.createTempFile("cloudwatch-metrics", null);
      writer = new FileWriter(tempFile.toFile());
      Gson gson = new GsonBuilder().setPrettyPrinting().create();
      gson.toJson(metrics, new JsonWriter(writer));
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      if (writer != null) {
        try {
          writer.flush();
          writer.close();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }

    createBucket(getBucketName());
    s3Client.putObject(new PutObjectRequest(getBucketName(), metricsFileKey(), tempFile.toFile()));
    s3Client.putObject(new PutObjectRequest(getBucketName(), additionalInfo.getName(), additionalInfo));
  }

  private String metricsFileKey() {
    return String.format("cloudwatch-metrics-%s.txt", formatter.format(new Date()));
  }

  private String getBucketName() {
    return config.getS3BucketPrefix() + "-" + getBenchName();
  }

  protected abstract String getBenchName();

  /**
   * Sub classes are open to implement setup and tear down hook to collect any additional information and hand them over
   * as a file during teardown. This class will make sure that file get uploaded to S3 in the same bucket as cloudwatch
   * metrics data.
   */
  protected abstract void setupHook();

  protected abstract File teardownHook();

  private void createBucket(String bucketName) {
    if (!s3Client.doesBucketExist(bucketName)) {
      try {
        s3Client.createBucket(new CreateBucketRequest(bucketName));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  private Map<String, List<GetMetricStatisticsResult>> getInstanceMetrics() {
    Map<String, List<GetMetricStatisticsResult>> instanceMetrics = new HashMap<>();

    for (String instanceId : config.getEC2Instances()) {
      List<GetMetricStatisticsResult> metrics = new ArrayList<>();

      metrics.add(getCPUUtilizationMetrics(instanceId));
      metrics.add(getNetworkBytesInMetrics(instanceId));
      metrics.add(getNetworkBytesOutMetrics(instanceId));
      metrics.add(getDiskReadBytesMetrics(instanceId));
      metrics.add(getDiskReadOpsMetrics(instanceId));
      metrics.add(getDiskWriteBytesMetrics(instanceId));
      metrics.add(getDiskWriteOpsMetrics(instanceId));

      instanceMetrics.put(instanceId, metrics);
    }

    return instanceMetrics;
  }

  private Map<String, List<GetMetricStatisticsResult>> getVolumeMetrics() {
    Map<String, List<GetMetricStatisticsResult>> volumeMetrics = new HashMap<>();
    for (String volumeId : config.getEBSVolumes()) {
      List<GetMetricStatisticsResult> metrics = new ArrayList<>();

      metrics.add(getEBSVolumeQueueLengthMetrics(volumeId));
      metrics.add(getEBSVolumeReadBytesMetrics(volumeId));
      metrics.add(getEBSVolumeReadOpsMetrics(volumeId));
      metrics.add(getEBSVolumeWriteBytesMetrics(volumeId));
      metrics.add(getEBSVolumeWriteOpsMetrics(volumeId));
      metrics.add(getEBSVolumeTotalReadTimeMetrics(volumeId));
      metrics.add(getEBSVolumeTotalWriteTimeMetrics(volumeId));
      metrics.add(getEBSVolumeIdleTimeMetrics(volumeId));

      volumeMetrics.put(volumeId, metrics);
    }

    return volumeMetrics;
  }

  private GetMetricStatisticsResult getCPUUtilizationMetrics(String instanceId) {
    GetMetricStatisticsRequest request = new GetMetricStatisticsRequest()
        .withStartTime(new Date(startTime.getTime() - fifteenMinutes))
        .withNamespace("AWS/EC2")
        .withPeriod(oneMinute)
        .withDimensions(new Dimension().withName("InstanceId").withValue(instanceId))
        .withMetricName("CPUUtilization")
        .withStatistics("Average")
        .withEndTime(new Date());
    return cloudWatch.getMetricStatistics(request);
  }

  private GetMetricStatisticsResult getNetworkBytesInMetrics(String instanceId) {
    List<GetMetricStatisticsResult> results = new ArrayList<>();

    GetMetricStatisticsRequest request = new GetMetricStatisticsRequest()
        .withStartTime(new Date(startTime.getTime() - fifteenMinutes))
        .withNamespace("AWS/EC2")
        .withPeriod(oneMinute)
        .withDimensions(new Dimension().withName("InstanceId").withValue(instanceId))
        .withMetricName("NetworkIn")
        .withStatistics("Average", "Sum")
        .withEndTime(new Date());
    return cloudWatch.getMetricStatistics(request);
  }

  private GetMetricStatisticsResult getNetworkBytesOutMetrics(String instanceId) {
    GetMetricStatisticsRequest request = new GetMetricStatisticsRequest()
        .withStartTime(new Date(startTime.getTime() - fifteenMinutes))
        .withNamespace("AWS/EC2")
        .withPeriod(oneMinute)
        .withDimensions(new Dimension().withName("InstanceId").withValue(instanceId))
        .withMetricName("NetworkOut")
        .withStatistics("Average", "Sum")
        .withEndTime(new Date());
    return cloudWatch.getMetricStatistics(request);
  }

  private GetMetricStatisticsResult getDiskWriteOpsMetrics(String instanceId) {
    GetMetricStatisticsRequest request = new GetMetricStatisticsRequest()
        .withStartTime(new Date(startTime.getTime() - fifteenMinutes))
        .withNamespace("AWS/EC2")
        .withPeriod(oneMinute)
        .withDimensions(new Dimension().withName("InstanceId").withValue(instanceId))
        .withMetricName("DiskWriteOps")
        .withStatistics("Average", "Sum")
        .withEndTime(new Date());
    return cloudWatch.getMetricStatistics(request);
  }

  private GetMetricStatisticsResult getDiskReadOpsMetrics(String instanceId) {
    GetMetricStatisticsRequest request = new GetMetricStatisticsRequest()
        .withStartTime(new Date(startTime.getTime() - fifteenMinutes))
        .withNamespace("AWS/EC2")
        .withPeriod(oneMinute)
        .withDimensions(new Dimension().withName("InstanceId").withValue(instanceId))
        .withMetricName("DiskReadOps")
        .withStatistics("Average", "Sum")
        .withEndTime(new Date());
    return cloudWatch.getMetricStatistics(request);
  }

  private GetMetricStatisticsResult getDiskWriteBytesMetrics(String instanceId) {
    GetMetricStatisticsRequest request = new GetMetricStatisticsRequest()
        .withStartTime(new Date(startTime.getTime() - fifteenMinutes))
        .withNamespace("AWS/EC2")
        .withPeriod(oneMinute)
        .withDimensions(new Dimension().withName("InstanceId").withValue(instanceId))
        .withMetricName("DiskWriteBytes")
        .withStatistics("Average", "Sum")
        .withEndTime(new Date());
    return cloudWatch.getMetricStatistics(request);
  }

  private GetMetricStatisticsResult getDiskReadBytesMetrics(String instanceId) {
    GetMetricStatisticsRequest request = new GetMetricStatisticsRequest()
        .withStartTime(new Date(startTime.getTime() - fifteenMinutes))
        .withNamespace("AWS/EC2")
        .withPeriod(oneMinute)
        .withDimensions(new Dimension().withName("InstanceId").withValue(instanceId))
        .withMetricName("DiskReadBytes")
        .withStatistics("Average", "Sum")
        .withEndTime(new Date());
    return cloudWatch.getMetricStatistics(request);
  }

  private GetMetricStatisticsResult getEBSVolumeWriteOpsMetrics(String volumeId) {
    GetMetricStatisticsRequest request = new GetMetricStatisticsRequest()
        .withStartTime(new Date(startTime.getTime() - fifteenMinutes))
        .withNamespace("AWS/EBS")
        .withPeriod(period)
        .withDimensions(new Dimension().withName("VolumeId").withValue(volumeId))
        .withMetricName("VolumeWriteOps")
        .withStatistics("Average", "Sum")
        .withEndTime(new Date());
    return cloudWatch.getMetricStatistics(request);
  }

  private GetMetricStatisticsResult getEBSVolumeWriteBytesMetrics(String volumeId) {
    GetMetricStatisticsRequest request = new GetMetricStatisticsRequest()
        .withStartTime(new Date(startTime.getTime() - fifteenMinutes))
        .withNamespace("AWS/EBS")
        .withPeriod(period)
        .withDimensions(new Dimension().withName("VolumeId").withValue(volumeId))
        .withMetricName("VolumeWriteBytes")
        .withStatistics("Average", "Sum", "SampleCount")
        .withEndTime(new Date());
    return cloudWatch.getMetricStatistics(request);
  }

  private GetMetricStatisticsResult getEBSVolumeReadOpsMetrics(String volumeId) {
    GetMetricStatisticsRequest request = new GetMetricStatisticsRequest()
        .withStartTime(new Date(startTime.getTime() - fifteenMinutes))
        .withNamespace("AWS/EBS")
        .withPeriod(period)
        .withDimensions(new Dimension().withName("VolumeId").withValue(volumeId))
        .withMetricName("VolumeReadOps")
        .withStatistics("Average", "Sum")
        .withEndTime(new Date());
    return cloudWatch.getMetricStatistics(request);
  }

  private GetMetricStatisticsResult getEBSVolumeReadBytesMetrics(String volumeId) {
    GetMetricStatisticsRequest request = new GetMetricStatisticsRequest()
        .withStartTime(new Date(startTime.getTime() - fifteenMinutes))
        .withNamespace("AWS/EBS")
        .withPeriod(period)
        .withDimensions(new Dimension().withName("VolumeId").withValue(volumeId))
        .withMetricName("VolumeReadBytes")
        .withStatistics("Average", "Sum", "SampleCount")
        .withEndTime(new Date());
    return cloudWatch.getMetricStatistics(request);
  }

  private GetMetricStatisticsResult getEBSVolumeQueueLengthMetrics(String volumeId) {
    GetMetricStatisticsRequest request = new GetMetricStatisticsRequest()
        .withStartTime(new Date(startTime.getTime() - fifteenMinutes))
        .withNamespace("AWS/EBS")
        .withPeriod(period)
        .withDimensions(new Dimension().withName("VolumeId").withValue(volumeId))
        .withMetricName("VolumeQueueLength")
        .withStatistics("Average", "Sum")
        .withEndTime(new Date());
    return cloudWatch.getMetricStatistics(request);
  }

  private GetMetricStatisticsResult getEBSVolumeTotalReadTimeMetrics(String volumeId) {
    GetMetricStatisticsRequest request = new GetMetricStatisticsRequest()
        .withStartTime(new Date(startTime.getTime() - fifteenMinutes))
        .withNamespace("AWS/EBS")
        .withPeriod(period)
        .withDimensions(new Dimension().withName("VolumeId").withValue(volumeId))
        .withMetricName("VolumeTotalReadTime")
        .withStatistics("Average")
        .withEndTime(new Date());
    return cloudWatch.getMetricStatistics(request);
  }

  private GetMetricStatisticsResult getEBSVolumeTotalWriteTimeMetrics(String volumeId) {
    GetMetricStatisticsRequest request = new GetMetricStatisticsRequest()
        .withStartTime(new Date(startTime.getTime() - fifteenMinutes))
        .withNamespace("AWS/EBS")
        .withPeriod(period)
        .withDimensions(new Dimension().withName("VolumeId").withValue(volumeId))
        .withMetricName("VolumeTotalWriteTime")
        .withStatistics("Average")
        .withEndTime(new Date());
    return cloudWatch.getMetricStatistics(request);
  }

  private GetMetricStatisticsResult getEBSVolumeIdleTimeMetrics(String volumeId) {
    GetMetricStatisticsRequest request = new GetMetricStatisticsRequest()
        .withStartTime(new Date(startTime.getTime() - fifteenMinutes))
        .withNamespace("AWS/EBS")
        .withPeriod(period)
        .withDimensions(new Dimension().withName("VolumeId").withValue(volumeId))
        .withMetricName("VolumeIdleTime")
        .withStatistics("Sum")
        .withEndTime(new Date());
    return cloudWatch.getMetricStatistics(request);
  }
}


