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
package org.pathirage.fdbench.kafka.perfmodeling;

import org.apache.commons.math3.distribution.AbstractRealDistribution;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.samza.metrics.Gauge;
import org.pathirage.fdbench.kafka.KafkaBenchmarkConfig;
import org.pathirage.fdbench.kafka.NSThroughputThrottler;
import org.pathirage.fdbench.metrics.api.MetricsRegistry;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class MultiRateMultiSizeProducerTask extends ProducerTask {

    private final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    private final Gauge<Integer> messageSizeGuage;
    private final Gauge<Integer> messageRateGuage;

    public MultiRateMultiSizeProducerTask(String taskId, String benchmarkName, String containerId, MetricsRegistry metricsRegistry, KafkaBenchmarkConfig config) {
        super(taskId, benchmarkName, containerId, metricsRegistry, config);
        this.messageSizeGuage = metricsRegistry.<Integer>newGauge(getGroup(), "message-size", 0);
        this.messageRateGuage = metricsRegistry.<Integer>newGauge(getGroup(), "message-rate", 0);
    }


    private List<Integer> getMessageSizes() {
        System.out.println(System.getenv(SyntheticWorkloadGeneratorConstants.ENV_KAFKA_WORKLOAD_GENERATOR_MSG_SIZE_MEANS));
        return Arrays.stream(System.getenv(SyntheticWorkloadGeneratorConstants.ENV_KAFKA_WORKLOAD_GENERATOR_MSG_SIZE_MEANS).split(","))
                .map((s) -> Integer.valueOf(s.trim())).collect(Collectors.toList());
    }

    private List<Integer> getMessageRates() {
        return Arrays.stream(System.getenv(SyntheticWorkloadGeneratorConstants.ENV_KAFKA_WORKLOAD_GENERATOR_MESSAGE_RATES).split(","))
                .map((s) -> Integer.valueOf(s.trim())).collect(Collectors.toList());
    }

    @Override
    public void run() {
        CountDownLatch countDownLatch = new CountDownLatch(getMessageSizes().size() * getMessageRates().size());

        int i = 0;
        for (Integer messageSize : getMessageSizes()) {
            for (Integer messageRate : getMessageRates()) {
                executorService.schedule(
                        new ScheduledProducerTask(180, messageSize, messageRate, countDownLatch),
                        i * 180 + i == 0 ? 0 : 45, TimeUnit.SECONDS);
            }
        }

        try {
            countDownLatch.await();
            executorService.shutdown();
        } catch (InterruptedException e) {
            throw new RuntimeException("Benchmark got interrupted.", e);
        }
    }

    public class ScheduledProducerTask implements Runnable {
        private final int duration;
        private final AbstractRealDistribution messageSizeDist;
        private final int messageSize;
        private final int messageRate;
        private final Random random = new Random(System.currentTimeMillis());
        private final CountDownLatch countDownLatch;

        public ScheduledProducerTask(int duration, int messageSize, int messageRate, CountDownLatch countDownLatch) {
            this.duration = duration;
            this.messageSize = messageSize;
            this.messageSizeDist = new NormalDistribution(messageSize, getStandardDeviation(messageSize));
            this.messageRate = messageRate;
            this.countDownLatch = countDownLatch;
        }

        private int getStandardDeviation(int messageSize) {
            if (messageSize < 500) {
                return 100;
            } else if (messageSize < 1000) {
                return 200;
            }
            return 250;
        }

        @Override
        public void run() {
            messageSizeGuage.set(messageSize);
            messageRateGuage.set(messageRate);
            int i = 0;
            long startTime = System.currentTimeMillis();
            long startNS = System.nanoTime();
            NSThroughputThrottler throttler = new NSThroughputThrottler(messageRate, startNS);
            while ((System.currentTimeMillis() - startTime) < duration * 1000) {
                long sendStartNanos = System.nanoTime();
                byte[] msg = generateRandomMessage();
                producer.send(new ProducerRecord<byte[], byte[]>(getTopic(),
                                partitionAssignment.get(ThreadLocalRandom.current().nextInt(partitionAssignment.size())),
                                msgToKey(msg),
                                msg),
                        new ProduceCompletionCallback(startTime, sendStartNanos, msg.length));
                i++;
                if (throttler.shouldThrottle(i, sendStartNanos)) {
                    throttler.throttle();
                }
            }

            countDownLatch.countDown();
        }

        private byte[] generateRandomMessage() {
            int msgSize = (int) messageSizeDist.sample() + 100;

            byte[] payload = new byte[msgSize];

            for (int i = 0; i < payload.length; i++) {
                payload[i] = (byte) (random.nextInt(26) + 65);
            }

            return payload;
        }
    }
}
