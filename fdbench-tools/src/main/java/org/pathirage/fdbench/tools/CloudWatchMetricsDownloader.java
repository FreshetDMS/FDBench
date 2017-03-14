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

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
import com.amazonaws.services.cloudwatch.model.Datapoint;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.GetMetricStatisticsRequest;
import com.amazonaws.services.cloudwatch.model.GetMetricStatisticsResult;
import com.google.gson.*;

import java.util.*;

public class CloudWatchMetricsDownloader {

  private final AmazonCloudWatch cloudWatch;
  private final int fiveMinutes = 5 * 60;

  public static void main(String[] args) {
    AmazonCloudWatchClientBuilder builder = AmazonCloudWatchClientBuilder.standard();
    builder.setCredentials(new AWSStaticCredentialsProvider(
        new BasicAWSCredentials("", "")));
    builder.setRegion("us-west-2");
    AmazonCloudWatch amazonCloudWatch = builder.build();

    new CloudWatchMetricsDownloader(amazonCloudWatch).printVolumeMetrics("vol-06c29a52416414429");
  }


  public CloudWatchMetricsDownloader(AmazonCloudWatch cloudWatch) {
    this.cloudWatch = cloudWatch;
  }

  public void printVolumeMetrics(String volumeId){
    List<GetMetricStatisticsResult> volumeMetrics = getVolumeMetrics(volumeId, new Date(1489293720000L), new Date(1489294020000L));
    JsonObject metricsObject = metricStatisticsResultsToJson(volumeId, "ebs", volumeMetrics);

    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    String json = gson.toJson(metricsObject);
    System.out.println(json);
  }

  private Set<String> getEBSVolumes() {
    Set<String> vols = new HashSet<>();
    vols.add("vol-08f1395d1c29ad1bd");
    return vols;
  }

  private JsonObject metricStatisticsResultsToJson(String objectId, String objectType, List<GetMetricStatisticsResult> metricStatisticsResults) {
    JsonObject metrics = new JsonObject();
    metrics.add("object-id", new JsonPrimitive(objectId));
    metrics.add("object-type", new JsonPrimitive(objectType));

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

    metrics.add("metrics", metricsArray);

    return metrics;
  }

  private List<GetMetricStatisticsResult> getVolumeMetrics(String volumeId, Date startTime, Date endTime) {

    List<GetMetricStatisticsResult> metrics = new ArrayList<>();

    metrics.add(getEBSVolumeQueueLengthMetrics(volumeId, startTime, endTime));
    metrics.add(getEBSVolumeReadBytesMetrics(volumeId, startTime, endTime));
    metrics.add(getEBSVolumeReadOpsMetrics(volumeId, startTime, endTime));
    metrics.add(getEBSVolumeWriteBytesMetrics(volumeId, startTime, endTime));
    metrics.add(getEBSVolumeWriteOpsMetrics(volumeId, startTime, endTime));
    metrics.add(getEBSVolumeTotalReadTimeMetrics(volumeId, startTime, endTime));
    metrics.add(getEBSVolumeTotalWriteTimeMetrics(volumeId, startTime, endTime));
    metrics.add(getEBSVolumeIdleTimeMetrics(volumeId, startTime, endTime));

    return metrics;
  }

  private GetMetricStatisticsResult getEBSVolumeWriteOpsMetrics(String volumeId, Date startTime, Date endTime) {
    GetMetricStatisticsRequest request = new GetMetricStatisticsRequest()
        .withStartTime(startTime)
        .withEndTime(endTime)
        .withNamespace("AWS/EBS")
        .withPeriod(fiveMinutes)
        .withDimensions(new Dimension().withName("VolumeId").withValue(volumeId))
        .withMetricName("VolumeWriteOps")
        .withStatistics("Average")
        .withEndTime(new Date());
    return cloudWatch.getMetricStatistics(request);
  }

  private GetMetricStatisticsResult getEBSVolumeWriteBytesMetrics(String volumeId, Date startTime, Date endTime) {
    GetMetricStatisticsRequest request = new GetMetricStatisticsRequest()
        .withStartTime(startTime)
        .withEndTime(endTime)
        .withNamespace("AWS/EBS")
        .withPeriod(fiveMinutes)
        .withDimensions(new Dimension().withName("VolumeId").withValue(volumeId))
        .withMetricName("VolumeWriteBytes")
        .withStatistics("Average", "Sum", "SampleCount")
        .withEndTime(new Date());
    return cloudWatch.getMetricStatistics(request);
  }

  private GetMetricStatisticsResult getEBSVolumeReadOpsMetrics(String volumeId, Date startTime, Date endTime) {
    GetMetricStatisticsRequest request = new GetMetricStatisticsRequest()
        .withStartTime(startTime)
        .withEndTime(endTime)
        .withNamespace("AWS/EBS")
        .withPeriod(fiveMinutes)
        .withDimensions(new Dimension().withName("VolumeId").withValue(volumeId))
        .withMetricName("VolumeReadOps")
        .withStatistics("Average")
        .withEndTime(new Date());
    return cloudWatch.getMetricStatistics(request);
  }

  private GetMetricStatisticsResult getEBSVolumeReadBytesMetrics(String volumeId, Date startTime, Date endTime) {
    GetMetricStatisticsRequest request = new GetMetricStatisticsRequest()
        .withStartTime(startTime)
        .withEndTime(endTime)
        .withNamespace("AWS/EBS")
        .withPeriod(fiveMinutes)
        .withDimensions(new Dimension().withName("VolumeId").withValue(volumeId))
        .withMetricName("VolumeReadBytes")
        .withStatistics("Average", "Sum", "SampleCount")
        .withEndTime(new Date());
    return cloudWatch.getMetricStatistics(request);
  }

  private GetMetricStatisticsResult getEBSVolumeQueueLengthMetrics(String volumeId, Date startTime, Date endTime) {
    GetMetricStatisticsRequest request = new GetMetricStatisticsRequest()
        .withStartTime(startTime)
        .withEndTime(endTime)
        .withNamespace("AWS/EBS")
        .withPeriod(fiveMinutes)
        .withDimensions(new Dimension().withName("VolumeId").withValue(volumeId))
        .withMetricName("VolumeQueueLength")
        .withStatistics("Average", "Sum")
        .withEndTime(new Date());
    return cloudWatch.getMetricStatistics(request);
  }

  private GetMetricStatisticsResult getEBSVolumeTotalReadTimeMetrics(String volumeId, Date startTime, Date endTime) {
    GetMetricStatisticsRequest request = new GetMetricStatisticsRequest()
        .withStartTime(startTime)
        .withEndTime(endTime)
        .withNamespace("AWS/EBS")
        .withPeriod(fiveMinutes)
        .withDimensions(new Dimension().withName("VolumeId").withValue(volumeId))
        .withMetricName("VolumeTotalReadTime")
        .withStatistics("Average", "Sum", "Minimum", "Maximum")
        .withEndTime(new Date());
    return cloudWatch.getMetricStatistics(request);
  }

  private GetMetricStatisticsResult getEBSVolumeTotalWriteTimeMetrics(String volumeId, Date startTime, Date endTime) {
    GetMetricStatisticsRequest request = new GetMetricStatisticsRequest()
        .withStartTime(startTime)
        .withEndTime(endTime)
        .withNamespace("AWS/EBS")
        .withPeriod(fiveMinutes)
        .withDimensions(new Dimension().withName("VolumeId").withValue(volumeId))
        .withMetricName("VolumeTotalWriteTime")
        .withStatistics("Average", "Sum", "Minimum", "Maximum")
        .withEndTime(new Date());
    return cloudWatch.getMetricStatistics(request);
  }

  private GetMetricStatisticsResult getEBSVolumeIdleTimeMetrics(String volumeId, Date startTime, Date endTime) {
    GetMetricStatisticsRequest request = new GetMetricStatisticsRequest()
        .withStartTime(startTime)
        .withEndTime(endTime)
        .withNamespace("AWS/EBS")
        .withPeriod(fiveMinutes)
        .withDimensions(new Dimension().withName("VolumeId").withValue(volumeId))
        .withMetricName("VolumeIdleTime")
        .withStatistics("Average", "Sum", "Minimum", "Maximum")
        .withEndTime(new Date());
    return cloudWatch.getMetricStatistics(request);
  }
}
