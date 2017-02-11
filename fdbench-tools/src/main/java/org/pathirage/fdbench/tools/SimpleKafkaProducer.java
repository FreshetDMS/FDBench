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

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.HdrHistogram.Histogram;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.Random;

public class SimpleKafkaProducer {

  private static Random random = new Random(System.currentTimeMillis());
  private static DescriptiveStatistics statistics = new DescriptiveStatistics();
  private static DescriptiveStatistics intervalStats = new DescriptiveStatistics();
  private static Histogram latency = new Histogram(300000000000L, 5);
  private static long completedRequests = 0;

  public static void main(String[] args) {
    Options options = new Options();
    new JCommander(options, args);

    System.out.println(String.format("Starting produce with message-size: %s, brokers: %s, message-rate: %s, topic: %s, and duration: %s seconds",
        options.messageSize, options.kafkaBrokers, options.messageRate, options.topic, options.duration));

    KafkaProducer<byte[], byte[]> kafkaProducer = new KafkaProducer<byte[], byte[]>(getProducerProperties(options.kafkaBrokers, options.batchSize));

    long startTime = System.currentTimeMillis();
    int i = 0;
    while ((System.currentTimeMillis() - startTime) < options.duration * 1000) {
      long interval = 0L;
      if (!options.fullThrottle) {
        if (options.constantInterarrival) {
          interval = 1000000000 / options.messageRate;
        } else {
          interval = (long) poissonRandomInterArrivalDelay((1 / options.messageRate) * 1000000000);
        }

        intervalStats.addValue(interval);
        // This is not a high accuracy sleep. But will work for microsecond sleep times
        // http://www.rationaljava.com/2015/10/measuring-microsecond-in-java.html
        waitNanos(interval);
      }
      long sendStartNanos = System.nanoTime();
      kafkaProducer.send(new ProducerRecord<byte[], byte[]>(options.topic, generateRandomMessage(options.messageSize)), new ProduceCompletionCallback(startTime, sendStartNanos, interval, options.constantInterarrival && !options.fullThrottle));
      i++;
    }

    System.out.println("\nInterval Stats:");
    System.out.println("\tMean Interval: " + intervalStats.getMean() / 1000000000);
    System.out.println("\nStats:");
    System.out.println("\tMean Response Time: " + statistics.getMean() / 1000000000);
    System.out.println("\tSDV Response Time: " + statistics.getStandardDeviation() / 1000000000);
    System.out.println("\tAverage Requests/s: " + (completedRequests / ((System.currentTimeMillis() - startTime) / 1000)));
    System.out.println("\tTotal Requests Completed: " + completedRequests);
    System.out.println("\tTotal Time (in seconds): " + ((System.currentTimeMillis() - startTime) / 1000));
    System.out.println("\nHistogram:");
    System.out.println("\t50th Percentile: " + latency.getValueAtPercentile(50.0f));
    System.out.println("\t75th Percentile: " + latency.getValueAtPercentile(75.0f));
    System.out.println("\t90th Percentile: " + latency.getValueAtPercentile(90.0f));
    System.out.println("\t99th Percentile: " + latency.getValueAtPercentile(99.0f));
    System.out.println("\t99.99th Percentile: " + latency.getValueAtPercentile(99.99f));
    System.exit(0);
  }

  private static void waitNanos(long nanos) {
    long start = System.nanoTime();
    long end = 0;
    do {
      end = System.nanoTime();
    } while (start + nanos >= end);
  }

  private static Properties getProducerProperties(String brokers, int batchSize) {
    Properties producerProps = new Properties();

    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
    producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, "simple-producer");
    producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);

    return producerProps;
  }

  private static byte[] generateRandomMessage(int size) {
    byte[] payload = new byte[size];

    for (int i = 0; i < payload.length; i++) {
      payload[i] = (byte) (random.nextInt(26) + 65);
    }

    return payload;
  }

  private static double poissonRandomInterArrivalDelay(double L) {
    return Math.log(1.0 - random.nextDouble()) / -L;
  }

  public static class Options {
    @Parameter(names = {"--msg-size", "-m"})
    public int messageSize = 100;

    @Parameter(names = {"--brokers", "-b"})
    public String kafkaBrokers;

    @Parameter(names = {"--msg-rate", "-r"})
    public int messageRate = 100;

    @Parameter(names = {"--duration", "-d"})
    public int duration = 120;

    @Parameter(names = {"--topic", "-t"})
    public String topic = "test";

    @Parameter(names = {"--full-throttle", "-f"})
    public boolean fullThrottle = false;

    @Parameter(names = {"--constant-interarrival", "-c"})
    public boolean constantInterarrival = false;

    @Parameter(names = {"--batch-size", "-bs"})
    public int batchSize = 1024;
  }

  public static class ProduceCompletionCallback implements Callback {
    private final long startNs;
    private final long sendStartNanos;
    private final long expectedInterval;
    private final boolean constantInterval;

    public ProduceCompletionCallback(long startNs, long sendStartNanos, long expectedInterval, boolean constantInterval) {
      this.startNs = startNs;
      this.sendStartNanos = sendStartNanos;
      this.expectedInterval = expectedInterval;
      this.constantInterval = constantInterval;
    }

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
      long now = System.nanoTime();
      long l = now - sendStartNanos;
      if (exception == null) {
        statistics.addValue(l); // Since this is workload generation, corrected histogram is not possible.
        if (constantInterval) {
          latency.recordValueWithExpectedInterval(l, expectedInterval);
        } else {
          latency.recordValue(l);
        }
        completedRequests++;
      }
    }
  }
}
