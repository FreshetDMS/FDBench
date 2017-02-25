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
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
import com.amazonaws.services.cloudwatch.model.Datapoint;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.GetMetricStatisticsRequest;
import com.amazonaws.services.cloudwatch.model.GetMetricStatisticsResult;

import java.util.Date;

public class CloudWatchMetricsDownloader {
  public static void main(String[] args) {
    final String awsAccessKey = "";
    final String awsSecretKey = "";
    final String instanceId = "";
    final String volumeId = "";
    final long threeDays = 3 * 1000 * 60 * 60 * 24;
    final long oneDay = 1000 * 60 * 60 * 24;
    final int oneHour = 60 * 60;
    final int oneMinute = 5 * 60;

    AmazonCloudWatchClientBuilder builder = AmazonCloudWatchClientBuilder.standard();
    builder.setCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(awsAccessKey, awsSecretKey)));
    builder.setRegion("us-west-2");
    AmazonCloudWatch cloudWatch = builder.build();
    GetMetricStatisticsRequest request = new GetMetricStatisticsRequest()
        .withStartTime(new Date(new Date().getTime() - threeDays))
        .withNamespace("AWS/EC2")
        .withPeriod(oneMinute)
        .withDimensions(new Dimension().withName("InstanceId").withValue(instanceId))
        .withMetricName("CPUUtilization")
        .withStatistics("Average")
        .withEndTime(new Date());
    GetMetricStatisticsResult result = cloudWatch.getMetricStatistics(request);
    toStdOut(result, instanceId);

    GetMetricStatisticsRequest ebsRequest = new GetMetricStatisticsRequest()
        .withStartTime(new Date(new Date().getTime() - threeDays))
        .withNamespace("AWS/EBS")
        .withPeriod(oneMinute)
        .withDimensions(new Dimension().withName("VolumeId").withValue(volumeId))
        .withMetricName("VolumeWriteOps")
        .withStatistics("Average")
        .withEndTime(new Date(new Date().getTime() - oneDay));

    GetMetricStatisticsResult r = cloudWatch.getMetricStatistics(ebsRequest);
    System.out.println(r);
  }

  private static void toStdOut(final GetMetricStatisticsResult result, final String instanceId) {
    System.out.println(result);
  }
}
