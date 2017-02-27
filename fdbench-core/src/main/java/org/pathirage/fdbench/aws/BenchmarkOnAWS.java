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
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.GetMetricStatisticsRequest;
import com.amazonaws.services.cloudwatch.model.GetMetricStatisticsResult;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.typesafe.config.Config;
import org.pathirage.fdbench.api.Benchmark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public abstract class BenchmarkOnAWS implements Benchmark {
  private static final Logger log = LoggerFactory.getLogger(BenchmarkOnAWS.class);

  private static final SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd-hh-mm-ss");

  private final int tenMinutes = 10 * 60;
  private final int oneMinute = 60;

  private final AWSConfiguration config;
  private Date startTime;
  private final AmazonCloudWatch cloudWatch;
  private final AmazonS3 s3Client;
  private final String bucketName;

  protected BenchmarkOnAWS(Config rawConfig) {
    this.config = new AWSConfiguration(rawConfig);
    this.cloudWatch = config.isAWSBench() ? getCloudWatchClient() : null;
    this.s3Client = config.isAWSBench() ? getS3Client() : null;
    this.bucketName = config.isAWSBench() ? getBucketName() : null;
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
  }

  @Override
  public void teardown() {
    if (config.isAWSBench()) {
      getAndReportCloudWatchMetrics();
    }
  }

  private void getAndReportCloudWatchMetrics() {
    List<GetMetricStatisticsResult> metrics = new ArrayList<>();

    metrics.addAll(getCPUUtilizationMetrics());
    metrics.addAll(getDiskReadBytesMetrics());
    metrics.addAll(getDiskReadOpsMetrics());
    metrics.addAll(getDiskWriteBytesMetrics());
    metrics.addAll(getDiskWriteOpsMetrics());
    metrics.addAll(getNetworkBytesInMetrics());
    metrics.addAll(getNetworkBytesOutMetrics());
    metrics.addAll(getEBSVolumeQueueLengthMetrics());
    metrics.addAll(getEBSVolumeReadBytesMetrics());
    metrics.addAll(getEBSVolumeReadOpsMetrics());
    metrics.addAll(getEBSVolumeWriteBytesMetrics());
    metrics.addAll(getEBSVolumeWriteOpsMetrics());

    Path tempFile = null;
    FileWriter writer = null;
    try {
      tempFile = Files.createTempFile("cloudwatch-metrics", null);
      writer = new FileWriter(tempFile.toFile());
      for (GetMetricStatisticsResult r : metrics) {
        writer.write(String.format("%s%n", r.toString()));
      }
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

    createBucket(bucketName);
    s3Client.putObject(new PutObjectRequest(bucketName, metricsFileKey(), tempFile.toFile()));
  }

  private String metricsFileKey() {
    return String.format("cloudwatch-metrics-%s.txt", formatter.format(new Date()));
  }

  private String getBucketName(){
    return config.getS3BucketPrefix() + "-" + getBenchName();
  }

  protected abstract String getBenchName();

  private void createBucket(String bucketName) {
    if (!s3Client.doesBucketExist(bucketName)) {
      try {
        s3Client.createBucket(new CreateBucketRequest(bucketName));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  private List<GetMetricStatisticsResult> getCPUUtilizationMetrics() {
    List<GetMetricStatisticsResult> results = new ArrayList<>();
    for (String instanceId : config.getEC2Instances()) {
      GetMetricStatisticsRequest request = new GetMetricStatisticsRequest()
          .withStartTime(new Date(startTime.getTime() - tenMinutes))
          .withNamespace("AWS/EC2")
          .withPeriod(oneMinute)
          .withDimensions(new Dimension().withName("InstanceId").withValue(instanceId))
          .withMetricName("CPUUtilization")
          .withStatistics("Average")
          .withEndTime(new Date());
      results.add(cloudWatch.getMetricStatistics(request));
    }

    return results;
  }

  private List<GetMetricStatisticsResult> getNetworkBytesInMetrics() {
    List<GetMetricStatisticsResult> results = new ArrayList<>();

    for (String instanceId : config.getEC2Instances()) {
      GetMetricStatisticsRequest request = new GetMetricStatisticsRequest()
          .withStartTime(new Date(startTime.getTime() - tenMinutes))
          .withNamespace("AWS/EC2")
          .withPeriod(oneMinute)
          .withDimensions(new Dimension().withName("InstanceId").withValue(instanceId))
          .withMetricName("NetworkIn")
          .withStatistics("Average", "Sum")
          .withEndTime(new Date());
      results.add(cloudWatch.getMetricStatistics(request));
    }

    return results;
  }

  private List<GetMetricStatisticsResult> getNetworkBytesOutMetrics() {
    List<GetMetricStatisticsResult> results = new ArrayList<>();

    for (String instanceId : config.getEC2Instances()) {
      GetMetricStatisticsRequest request = new GetMetricStatisticsRequest()
          .withStartTime(new Date(startTime.getTime() - tenMinutes))
          .withNamespace("AWS/EC2")
          .withPeriod(oneMinute)
          .withDimensions(new Dimension().withName("InstanceId").withValue(instanceId))
          .withMetricName("NetworkOut")
          .withStatistics("Average", "Sum")
          .withEndTime(new Date());
      results.add(cloudWatch.getMetricStatistics(request));
    }

    return results;
  }

  private List<GetMetricStatisticsResult> getDiskWriteOpsMetrics() {
    List<GetMetricStatisticsResult> results = new ArrayList<>();

    for (String instanceId : config.getEC2Instances()) {
      GetMetricStatisticsRequest request = new GetMetricStatisticsRequest()
          .withStartTime(new Date(startTime.getTime() - tenMinutes))
          .withNamespace("AWS/EC2")
          .withPeriod(oneMinute)
          .withDimensions(new Dimension().withName("InstanceId").withValue(instanceId))
          .withMetricName("DiskWriteOps")
          .withStatistics("Average", "Sum")
          .withEndTime(new Date());
      results.add(cloudWatch.getMetricStatistics(request));
    }

    return results;
  }

  private List<GetMetricStatisticsResult> getDiskReadOpsMetrics() {
    List<GetMetricStatisticsResult> results = new ArrayList<>();

    for (String instanceId : config.getEC2Instances()) {
      GetMetricStatisticsRequest request = new GetMetricStatisticsRequest()
          .withStartTime(new Date(startTime.getTime() - tenMinutes))
          .withNamespace("AWS/EC2")
          .withPeriod(oneMinute)
          .withDimensions(new Dimension().withName("InstanceId").withValue(instanceId))
          .withMetricName("DiskReadOps")
          .withStatistics("Average", "Sum")
          .withEndTime(new Date());
      results.add(cloudWatch.getMetricStatistics(request));
    }

    return results;
  }

  private List<GetMetricStatisticsResult> getDiskWriteBytesMetrics() {
    List<GetMetricStatisticsResult> results = new ArrayList<>();

    for (String instanceId : config.getEC2Instances()) {
      GetMetricStatisticsRequest request = new GetMetricStatisticsRequest()
          .withStartTime(new Date(startTime.getTime() - tenMinutes))
          .withNamespace("AWS/EC2")
          .withPeriod(oneMinute)
          .withDimensions(new Dimension().withName("InstanceId").withValue(instanceId))
          .withMetricName("DiskWriteBytes")
          .withStatistics("Average", "Sum")
          .withEndTime(new Date());
      results.add(cloudWatch.getMetricStatistics(request));
    }

    return results;
  }

  private List<GetMetricStatisticsResult> getDiskReadBytesMetrics() {
    List<GetMetricStatisticsResult> results = new ArrayList<>();

    for (String instanceId : config.getEC2Instances()) {
      GetMetricStatisticsRequest request = new GetMetricStatisticsRequest()
          .withStartTime(new Date(startTime.getTime() - tenMinutes))
          .withNamespace("AWS/EC2")
          .withPeriod(oneMinute)
          .withDimensions(new Dimension().withName("InstanceId").withValue(instanceId))
          .withMetricName("DiskReadBytes")
          .withStatistics("Average", "Sum")
          .withEndTime(new Date());
      results.add(cloudWatch.getMetricStatistics(request));
    }

    return results;
  }

  private List<GetMetricStatisticsResult> getEBSVolumeWriteOpsMetrics() {
    List<GetMetricStatisticsResult> results = new ArrayList<>();

    for (String volumeId : config.getEBSVolumes()) {
      GetMetricStatisticsRequest request = new GetMetricStatisticsRequest()
          .withStartTime(new Date(startTime.getTime() - tenMinutes))
          .withNamespace("AWS/EBS")
          .withPeriod(oneMinute)
          .withDimensions(new Dimension().withName("VolumeId").withValue(volumeId))
          .withMetricName("VolumeWriteOps")
          .withStatistics("Average")
          .withEndTime(new Date());
      results.add(cloudWatch.getMetricStatistics(request));
    }

    return results;
  }

  private List<GetMetricStatisticsResult> getEBSVolumeWriteBytesMetrics() {
    List<GetMetricStatisticsResult> results = new ArrayList<>();

    for (String volumeId : config.getEBSVolumes()) {
      GetMetricStatisticsRequest request = new GetMetricStatisticsRequest()
          .withStartTime(new Date(startTime.getTime() - tenMinutes))
          .withNamespace("AWS/EBS")
          .withPeriod(oneMinute)
          .withDimensions(new Dimension().withName("VolumeId").withValue(volumeId))
          .withMetricName("VolumeWriteBytes")
          .withStatistics("Average", "Sum", "SampleCount")
          .withEndTime(new Date());
      results.add(cloudWatch.getMetricStatistics(request));
    }

    return results;
  }

  private List<GetMetricStatisticsResult> getEBSVolumeReadOpsMetrics() {
    List<GetMetricStatisticsResult> results = new ArrayList<>();

    for (String volumeId : config.getEBSVolumes()) {
      GetMetricStatisticsRequest request = new GetMetricStatisticsRequest()
          .withStartTime(new Date(startTime.getTime() - tenMinutes))
          .withNamespace("AWS/EBS")
          .withPeriod(oneMinute)
          .withDimensions(new Dimension().withName("VolumeId").withValue(volumeId))
          .withMetricName("VolumeReadOps")
          .withStatistics("Average")
          .withEndTime(new Date());
      results.add(cloudWatch.getMetricStatistics(request));
    }

    return results;
  }

  private List<GetMetricStatisticsResult> getEBSVolumeReadBytesMetrics() {
    List<GetMetricStatisticsResult> results = new ArrayList<>();

    for (String volumeId : config.getEBSVolumes()) {
      GetMetricStatisticsRequest request = new GetMetricStatisticsRequest()
          .withStartTime(new Date(startTime.getTime() - tenMinutes))
          .withNamespace("AWS/EBS")
          .withPeriod(oneMinute)
          .withDimensions(new Dimension().withName("VolumeId").withValue(volumeId))
          .withMetricName("VolumeReadBytes")
          .withStatistics("Average", "Sum", "SampleCount")
          .withEndTime(new Date());
      results.add(cloudWatch.getMetricStatistics(request));
    }

    return results;
  }

  private List<GetMetricStatisticsResult> getEBSVolumeQueueLengthMetrics() {
    List<GetMetricStatisticsResult> results = new ArrayList<>();

    for (String volumeId : config.getEBSVolumes()) {
      GetMetricStatisticsRequest request = new GetMetricStatisticsRequest()
          .withStartTime(new Date(startTime.getTime() - tenMinutes))
          .withNamespace("AWS/EBS")
          .withPeriod(oneMinute)
          .withDimensions(new Dimension().withName("VolumeId").withValue(volumeId))
          .withMetricName("VolumeQueueLength")
          .withStatistics("Average", "Sum")
          .withEndTime(new Date());
      results.add(cloudWatch.getMetricStatistics(request));
    }

    return results;
  }
}


