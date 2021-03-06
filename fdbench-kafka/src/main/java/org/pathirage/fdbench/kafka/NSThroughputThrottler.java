/**
 * Copyright 2016 Apache Software Foundation
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

package org.pathirage.fdbench.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a customization of {@link org.apache.kafka.tools.ThroughputThrottler} to work with nanosecond precision.
 */
public class NSThroughputThrottler {
  private static Logger log = LoggerFactory.getLogger(NSThroughputThrottler.class);

  private static final long NS_PER_MS = 1000000L;
  private static final long NS_PER_SEC = 1000 * NS_PER_MS;
  private static final long MIN_SLEEP_NS = 2 * NS_PER_MS;

  long sleepTimeNs;
  long sleepDeficitNs = 0;
  long targetThroughput = -1;
  long startNs;
  private boolean wakeup = false;

  /**
   * @param targetThroughput Can be messages/sec or bytes/sec
   * @param startNs          When the very first message is sent
   */
  public NSThroughputThrottler(long targetThroughput, long startNs) {
    this.targetThroughput = targetThroughput;
    this.startNs = startNs;
    this.sleepTimeNs = targetThroughput > 0 ? NS_PER_SEC / targetThroughput : Long.MAX_VALUE;
  }

  /**
   * @param amountSoFar bytes produced so far if you want to throttle data throughput, or
   *                    messages produced so far if you want to throttle message throughput.
   * @param sendStartNs timestamp of the most recently sent message
   * @return
   */
  public boolean shouldThrottle(long amountSoFar, long sendStartNs) {
    if (this.targetThroughput < 0) {
      // No throttling in this case
      return false;
    }

    float elapsedSeconds = (sendStartNs - startNs) / 1000000000.f;
    return elapsedSeconds > 0 && (amountSoFar / elapsedSeconds) > this.targetThroughput;
  }

  /**
   * Occasionally blocks for small amounts of time to achieve targetThroughput.
   * <p>
   * Note that if targetThroughput is 0, this will block extremely aggressively.
   */
  public void throttle() {
    if(log.isDebugEnabled()) {
      log.debug("Throttling producer..");
    }
    if (targetThroughput == 0) {
      try {
        synchronized (this) {
          while (!wakeup) {
            this.wait();
          }
        }
      } catch (InterruptedException e) {
        // do nothing
      }
      return;
    }

    // throttle throughput by sleeping, on average,
    // (1 / this.throughput) seconds between "things sent"
    sleepDeficitNs += sleepTimeNs;

    // If enough sleep deficit has accumulated, sleep a little
    if (sleepDeficitNs >= MIN_SLEEP_NS) {
      long sleepStartNs = System.nanoTime();
      long currentTimeNs = sleepStartNs;
      try {
        synchronized (this) {
          long elapsed = currentTimeNs - sleepStartNs;
          long remaining = sleepDeficitNs - elapsed;
          while (!wakeup && remaining > 0) {
            long sleepMs = remaining / 1000000;
            long sleepNs = remaining - sleepMs * 1000000;
            this.wait(sleepMs, (int) sleepNs);
            elapsed = System.nanoTime() - sleepStartNs;
            remaining = sleepDeficitNs - elapsed;
          }
          wakeup = false;
        }
        sleepDeficitNs = 0;
      } catch (InterruptedException e) {
        // If sleep is cut short, reduce deficit by the amount of
        // time we actually spent sleeping
        long sleepElapsedNs = System.nanoTime() - sleepStartNs;
        if (sleepElapsedNs <= sleepDeficitNs) {
          sleepDeficitNs -= sleepElapsedNs;
        }
      }
    }
  }

  /**
   * Wakeup the throttler if its sleeping.
   */
  public void wakeup() {
    synchronized (this) {
      wakeup = true;
      this.notifyAll();
    }
  }
}
