/**
 * Copyright 2017 Milinda Pathirage
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
package org.pathirage.fdbench.samza;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.fasterxml.jackson.databind.JsonNode;
import com.typesafe.config.Config;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.Gauge;
import org.pathirage.fdbench.api.BenchmarkTask;
import org.pathirage.fdbench.aws.AWSConfiguration;
import org.pathirage.fdbench.kafka.KafkaBenchmarkConstants;
import org.pathirage.fdbench.metrics.api.Histogram;
import org.pathirage.fdbench.metrics.api.MetricsRegistry;
import org.pathirage.fdbench.metrics.api.MetricsReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

public class SamzaE2ELatencyConsumeTask implements BenchmarkTask {
  private static Logger log = LoggerFactory.getLogger(SamzaE2ELatencyConsumeTask.class);

  private final String benchmarkName;
  private final String taskId;
  private final String containerId;
  private final Config config;
  private final MetricsRegistry metricsRegistry;
  private final KafkaConsumer<String, JsonNode> consumer;
  private final Histogram latency;
  private final org.apache.samza.metrics.Timer e2eLatency;
  private final Gauge<Long> elapsedTime;
  private final Counter messagesConsumed;
  private final PrintWriter latencyWriter;
  private final Path latencyFile;
  private final Integer benchmarkDuration;

  public SamzaE2ELatencyConsumeTask(String benchmarkName, String taskId, String containerID, Config config, MetricsRegistry metricsRegistry) {
    this.benchmarkName = benchmarkName;
    this.taskId = taskId;
    this.containerId = containerID;
    this.config = config;
    this.metricsRegistry = metricsRegistry;
    this.consumer = new KafkaConsumer<String, JsonNode>(getConsumerProperties());
    this.elapsedTime = metricsRegistry.<Long>newGauge(getGroup(), "elapsed-time", 0L);
    this.e2eLatency = metricsRegistry.newTimer(getGroup(), "e2e-latency");
    this.latency = metricsRegistry.newHistogram(getGroup(), "produce-latency", KafkaBenchmarkConstants.MAX_RECORDABLE_LATENCY, KafkaBenchmarkConstants.SIGNIFICANT_VALUE_DIGITS);
    this.messagesConsumed = metricsRegistry.newCounter(getGroup(), "messages-consumed");
    this.benchmarkDuration = new SamzaE2ELatencyBenchmarkConfig(config).getDurationSeconds();
    this.latencyFile = getTempFile();
    this.latencyWriter = getWriterForFile(this.latencyFile);
  }

  private String getGroup() {
    return benchmarkName + "-" + getTaskId();
  }

  private Path getTempFile() {
    try {
      return Files.createTempFile(benchmarkName + "-" + taskId, ".csv");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private AmazonS3 getS3Client() {
    AWSConfiguration awsConfiguration = new AWSConfiguration(config);
    AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
    builder.setRegion(awsConfiguration.getAWSRegion());
    builder.setCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(awsConfiguration.getAWSAccessKeyId(), awsConfiguration.getAWSAccessKeySecret())));
    return builder.build();
  }

  private void saveLatenciesToAmazonS3() {
    log.info("Storing task " + taskId + "'s latencies to S3.....");
    AmazonS3 s3Client = getS3Client();
    if (!s3Client.doesBucketExist(benchmarkName + "-latencies")) {
      try {
        s3Client.createBucket(new CreateBucketRequest(benchmarkName + "-latencies"));
      } catch (Exception e) {
        log.error("An error occurred during S3 bucket creation.", e);
        throw new RuntimeException(e);
      }
    }

    latencyWriter.flush();
    latencyWriter.close();

    log.info("Latency file size: " + latencyFile.toFile().length());

    s3Client.putObject(new PutObjectRequest(benchmarkName + "-latencies", benchmarkName + "-" + taskId + "-latencies.csv", new File(latencyFile.toString())));
  }

  private PrintWriter getWriterForFile(Path filePath) {
    try {
      return new PrintWriter(new BufferedWriter(new FileWriter(filePath.toFile())));
    } catch (IOException e) {
      throw new RuntimeException("Cannot create write for temp file " + filePath.toString(), e);
    }
  }

  @Override
  public String getTaskId() {
    return taskId;
  }

  @Override
  public String getBenchmarkName() {
    return benchmarkName;
  }

  @Override
  public String getContainerId() {
    return containerId;
  }

  @Override
  public void registerMetrics(Collection<MetricsReporter> reporters) {
    for (MetricsReporter reporter : reporters) {
      reporter.register(String.format("%s-%s-%s", benchmarkName, containerId, taskId), metricsRegistry);
    }
  }

  @Override
  public void stop() {
    if (consumer != null) {
      consumer.close();
    }

    saveLatenciesToAmazonS3();
  }

  @Override
  public void setup() {

  }

  @Override
  public void run() {
    List<String> topics = new ArrayList<>();
    topics.add(System.getenv(SamzaE2ELatencyBenchmarkConstants.RESULT_TOPIC));
    consumer.subscribe(topics);
    long stopAfter = System.currentTimeMillis() + (benchmarkDuration + 30) * 1000;
    log.info("Current time: " + System.currentTimeMillis() + " bechmark end time: " + stopAfter);
    long startedAt = System.currentTimeMillis();

    while (true) {
      ConsumerRecords<String, JsonNode> records = consumer.poll(30000);
      long receivedAt = System.currentTimeMillis();

      if (System.currentTimeMillis() >= stopAfter) {
        break;
      }

      for (ConsumerRecord<String, JsonNode> record : records) {
        JsonNode root = record.value();
        long createdAt = root.get("created-at").asLong();
        long lat = (receivedAt - createdAt);
        if (lat < 0) {
          log.info("Created At: " + createdAt + " Received At: " + receivedAt);
          lat = 0;
        }
        latency.recordValue(lat);
        latencyWriter.println(lat);
        e2eLatency.update(lat);
        messagesConsumed.inc();
        elapsedTime.set(System.currentTimeMillis() - startedAt);
      }
    }

    log.info("Done consuming messages.");

  }

  private Properties getConsumerProperties() {
    Properties consumerProps = new Properties();

    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv(KafkaBenchmarkConstants.ENV_KAFKA_BENCH_BROKERS));
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, getBenchmarkName() + "-consumer-group-" + getTaskId());
    consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    consumerProps.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
    consumerProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonDeserializer");

    return consumerProps;
  }
}
