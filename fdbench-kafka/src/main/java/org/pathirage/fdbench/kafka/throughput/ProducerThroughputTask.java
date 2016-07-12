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

package org.pathirage.fdbench.kafka.throughput;

import com.typesafe.config.Config;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.Gauge;
import org.pathirage.fdbench.kafka.Constants;
import org.pathirage.fdbench.kafka.KafkaBenchmarkTask;
import org.pathirage.fdbench.kafka.NSThroughputThrottler;
import org.pathirage.fdbench.metrics.api.Histogram;
import org.pathirage.fdbench.metrics.api.MetricsRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerThroughputTask extends KafkaBenchmarkTask {
  private static final Logger log = LoggerFactory.getLogger(ProducerThroughputTask.class);

  private static final String PRODUCER_THROUGHPUT_BENCH = "kafka-producer-throughput";

  private final Gauge<Long> elapsedTime;
  private final Counter successTotal;
  private final Counter errorTotal;
  private final Histogram produceLatency;
  private final Histogram errorLatency;
  private final KafkaProducer<byte[], byte[]> producer;

  public ProducerThroughputTask(String taskId, String benchmarkName, String containerId, MetricsRegistry metricsRegistry, Config rawConfig) {
    super(taskId, "kafka-producer-throughput", benchmarkName, containerId, metricsRegistry, new ThroughputBenchmarkConfig(rawConfig));
    this.producer = new KafkaProducer<byte[], byte[]>(getProducerProperties());
    this.elapsedTime = metricsRegistry.<Long>newGauge(PRODUCER_THROUGHPUT_BENCH, "elapsed-time", 0L);
    this.successTotal = metricsRegistry.newCounter(PRODUCER_THROUGHPUT_BENCH, "success-total");
    this.errorTotal = metricsRegistry.newCounter(PRODUCER_THROUGHPUT_BENCH, "error-total");
    this.produceLatency = metricsRegistry.newHistogram(PRODUCER_THROUGHPUT_BENCH, "produce-latency",
        Constants.MAX_RECORDABLE_LATENCY,
        Constants.SIGNIFICANT_VALUE_DIGITS);
    this.errorLatency = metricsRegistry.newHistogram(PRODUCER_THROUGHPUT_BENCH, "produce-error-latency",
        Constants.MAX_RECORDABLE_LATENCY,
        Constants.SIGNIFICANT_VALUE_DIGITS);
  }

  @Override
  public void stop() {
    if (producer != null) {
      producer.close();
    }
  }

  @Override
  public void run() {
    log.info("Starting producer throughput benchmark task " + getTaskId() + " in container: " + getContainerId() + " with partition assignment: " + System.getenv(Constants.ENV_PARTITIONS));
    ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[], byte[]>(getTopic(), generateRandomMessage());
    ThroughputBenchmarkConfig c = (ThroughputBenchmarkConfig) getConfig();

    long startNs = System.nanoTime();
    NSThroughputThrottler throttler = new NSThroughputThrottler(getMessageRate(), startNs);
    for (int i = 0; i < getRecordLimit(); i++) {
      long sendStartNanos = System.nanoTime();
      producer.send(c.isReuseMessage() ?
              record : new ProducerRecord<byte[], byte[]>(getTopic(), generateRandomMessage()),
          new ThroughputCallback(startNs, sendStartNanos));

      if (throttler.shouldThrottle(i, sendStartNanos)) {
        throttler.throttle();
      }
    }

  }

  public class ThroughputCallback implements Callback {
    private final long startNs;
    private final long sendStartNanos;

    public ThroughputCallback(long startNs, long sendStartNanos) {
      this.startNs = startNs;
      this.sendStartNanos = sendStartNanos;
    }

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
      long now = System.nanoTime();
      elapsedTime.set(now - startNs);
      long latency = now - sendStartNanos;
      if (exception == null) {
        successTotal.inc();
        produceLatency.recordValue(latency);
      } else {
        errorTotal.inc();
        errorLatency.recordValue(latency);
      }
    }
  }

}
