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
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.tools.ThroughputThrottler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class SimpleKafkaProducer {
  private static final Logger log = LoggerFactory.getLogger(SimpleKafkaProducer.class);
  private static Random random = new Random(System.currentTimeMillis());
  private static Histogram latency = new Histogram(300000000000L, 5);
  private static AtomicLong totalMessagesSent = new AtomicLong(0);
  private static AtomicLong totalBytesSent = new AtomicLong(0);

  public static void main(String[] args) {
    Options options = new Options();
    new JCommander(options, args);

    log.info(String.format("Starting produce with message-size: %s, brokers: %s, message-rate: %s, topic: %s, and duration: %s seconds",
        options.messageSize, options.kafkaBrokers, options.messageRate, options.topic, options.duration));

    KafkaProducer<byte[], byte[]> kafkaProducer = new KafkaProducer<byte[], byte[]>(getProducerProperties(options.kafkaBrokers, options.batchSize));
    byte[] payload = generateRandomMessage(options.messageSize);
    ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[], byte[]>(options.topic, payload);
    long startMs = System.currentTimeMillis();
    ThroughputThrottler throughputThrottler = new ThroughputThrottler(options.fullThrottle ? -1 : options.messageRate, startMs);
    int i = 0;
    while ((System.currentTimeMillis() - startMs) < options.duration * 1000) {
      long sendStartNanos = System.nanoTime();
      long sendStarMs = System.currentTimeMillis();
      kafkaProducer.send(record, new ProduceCompletionCallback(sendStartNanos, payload.length));
      i++;
      if (throughputThrottler.shouldThrottle(i, sendStarMs)) {
        throughputThrottler.throttle();
      }
    }

    kafkaProducer.close();
    log.info("Benchmark completed.");

    System.out.println("\nStats:");
    System.out.println("\tMean Response Time: " + latency.getMean() / 1000000000);
    System.out.println("\tSDV Response Time: " + latency.getStdDeviation() / 1000000000);
    System.out.println("\tAverage Requests/s: " + (totalMessagesSent.get() / ((System.currentTimeMillis() - startMs) / 1000)));
    System.out.println("\tAverage bytes/s: " + (totalBytesSent.get() / ((System.currentTimeMillis() - startMs) / 1000)));
    System.out.println("\tTotal Requests Completed: " + totalMessagesSent);
    System.out.println("\tTotal Time (in seconds): " + ((System.currentTimeMillis() - startMs) / 1000));
    System.out.println("\nLatency Histogram:");
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

    @Parameter(names = {"--batch-size", "-bs"})
    public int batchSize = 1024;
  }

  public static class ProduceCompletionCallback implements Callback {
    private final long sendStartNanos;
    private final int payloadLength;

    public ProduceCompletionCallback(long sendStartNanos, int payloadLength) {
      this.sendStartNanos = sendStartNanos;
      this.payloadLength = payloadLength;
    }

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
      long now = System.nanoTime();
      long l = now - sendStartNanos;
      latency.recordValue(l);
      totalMessagesSent.incrementAndGet();
      totalBytesSent.addAndGet(payloadLength);
      if (exception != null) {
        exception.printStackTrace();
      }
    }
  }
}
