/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.network.server;

import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The FetchBusyChecker class is responsible for monitoring the fetch latency of the system and
 * determining whether it is in a fetch busy mode. It keeps track of the fetch latency and switches
 * the system to fetch busy mode when the latency exceeds a certain threshold (highWatermarkMs).
 * Once in fetch busy mode, the system will remain in that state until the fetch latency falls below
 * another threshold (lowWatermarkMs) and a cooldown period (coolDownMs) has passed since the last
 * fetch busy time.
 *
 * This class provides thread-safe methods to handle fetch latency updates and check the current
 * fetch busy state. It also logs informative messages when entering or exiting the fetch busy
 * mode.
 */
@ThreadSafe
public class FetchBusyChecker {

  private static final Logger logger = LoggerFactory.getLogger(FetchBusyChecker.class);
  private final int highWatermarkMs;
  private final int lowWatermarkMs;
  private final int coolDownMs;
  private boolean isFetchBusy = false;
  private long lastFetchBusyTime = Long.MAX_VALUE;

  public FetchBusyChecker(int highWatermarkMs, int lowWatermarkMs, int coolDownMs) {
    this.highWatermarkMs = highWatermarkMs;
    this.lowWatermarkMs = lowWatermarkMs;
    this.coolDownMs = coolDownMs;
  }

  public synchronized void handle(long latency) {
    if (highWatermarkMs == 0) {
      return;
    }

    if (latency > highWatermarkMs) {
      lastFetchBusyTime = System.currentTimeMillis();
      if (!isFetchBusy) {
        isFetchBusy = true;
        logger.info("Entered fetchBusy mode.");
      }
    }

    if (isFetchBusy && latency < lowWatermarkMs
        && lastFetchBusyTime + coolDownMs < System.currentTimeMillis()) {
      isFetchBusy = false;
      logger.info("Exited fetchBusy mode.");
    }
  }

  public synchronized boolean isFetchBusy() {
    return isFetchBusy;
  }
}
