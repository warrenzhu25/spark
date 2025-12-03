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

import java.util.concurrent.ConcurrentHashMap;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Timer;

/**
 * A simple container for shuffle fetch request metrics exposed by shuffle RPC handlers.
 */
public class ShuffleFetchMetrics {
  private final Timer chunkFetchLatencyMillis;
  private final Timer chunkReadLatencyMillis;
  private final Timer responseSendLatencyMillis;
  private final Counter chunkFetchQueueDepth;
  private final Timer queueWaitTimeMillis;
  private final Histogram queueLengthHistogram;
  private final ConcurrentHashMap<Long, Integer> streamToShuffleMap;
  private final ConcurrentHashMap<Integer, Timer> perShuffleLatencyTimers;
  private final ConcurrentHashMap<Integer, Timer> perShuffleReadLatencyTimers;
  private final ConcurrentHashMap<Integer, Timer> perShuffleResponseSendLatencyTimers;

  public ShuffleFetchMetrics(
      Timer chunkFetchLatencyMillis,
      Timer chunkReadLatencyMillis,
      Timer responseSendLatencyMillis,
      Counter chunkFetchQueueDepth,
      Timer queueWaitTimeMillis,
      Histogram queueLengthHistogram,
      ConcurrentHashMap<Long, Integer> streamToShuffleMap,
      ConcurrentHashMap<Integer, Timer> perShuffleLatencyTimers,
      ConcurrentHashMap<Integer, Timer> perShuffleReadLatencyTimers,
      ConcurrentHashMap<Integer, Timer> perShuffleResponseSendLatencyTimers) {
    this.chunkFetchLatencyMillis = chunkFetchLatencyMillis;
    this.chunkReadLatencyMillis = chunkReadLatencyMillis;
    this.responseSendLatencyMillis = responseSendLatencyMillis;
    this.chunkFetchQueueDepth = chunkFetchQueueDepth;
    this.queueWaitTimeMillis = queueWaitTimeMillis;
    this.queueLengthHistogram = queueLengthHistogram;
    this.streamToShuffleMap = streamToShuffleMap;
    this.perShuffleLatencyTimers = perShuffleLatencyTimers;
    this.perShuffleReadLatencyTimers = perShuffleReadLatencyTimers;
    this.perShuffleResponseSendLatencyTimers = perShuffleResponseSendLatencyTimers;
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

  public ConcurrentHashMap<Long, Integer> getStreamToShuffleMap() {
    return streamToShuffleMap;
  }

  public ConcurrentHashMap<Integer, Timer> getPerShuffleLatencyTimers() {
    return perShuffleLatencyTimers;
  }

  public ConcurrentHashMap<Integer, Timer> getPerShuffleReadLatencyTimers() {
    return perShuffleReadLatencyTimers;
  }

  public ConcurrentHashMap<Integer, Timer> getPerShuffleResponseSendLatencyTimers() {
    return perShuffleResponseSendLatencyTimers;
  }

  public Timer getQueueWaitTimeMillis() {
    return queueWaitTimeMillis;
  }

  public Histogram getQueueLengthHistogram() {
    return queueLengthHistogram;
  }
}
