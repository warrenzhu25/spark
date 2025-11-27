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

package org.apache.spark.network.shuffle;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;

/**
 * A simple container for shuffle fetch request metrics exposed by shuffle RPC handlers.
 */
public class ShuffleFetchMetrics {
  private final Timer chunkFetchLatencyMillis;
  private final Timer chunkReadLatencyMillis;
  private final Timer responseSendLatencyMillis;
  private final Counter chunkFetchQueueDepth;

  public ShuffleFetchMetrics(
      Timer chunkFetchLatencyMillis,
      Timer chunkReadLatencyMillis,
      Timer responseSendLatencyMillis,
      Counter chunkFetchQueueDepth) {
    this.chunkFetchLatencyMillis = chunkFetchLatencyMillis;
    this.chunkReadLatencyMillis = chunkReadLatencyMillis;
    this.responseSendLatencyMillis = responseSendLatencyMillis;
    this.chunkFetchQueueDepth = chunkFetchQueueDepth;
  }

  public Timer getChunkFetchLatencyMillis() {
    return chunkFetchLatencyMillis;
  }

  public Timer getChunkReadLatencyMillis() {
    return chunkReadLatencyMillis;
  }

  public Timer getResponseSendLatencyMillis() {
    return responseSendLatencyMillis;
  }

  public Counter getChunkFetchQueueDepth() {
    return chunkFetchQueueDepth;
  }
}
