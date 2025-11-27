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

import com.codahale.metrics.Timer;

/**
 * A simple container for shuffle fetch request metrics exposed by shuffle RPC handlers.
 */
public class ShuffleFetchMetrics {
  private final Timer shuffleFetchRequestDurationMillis;
  private final Timer getChunkLatencyMillis;
  private final Timer respondLatencyMillis;

  public ShuffleFetchMetrics(
      Timer shuffleFetchRequestDurationMillis,
      Timer getChunkLatencyMillis,
      Timer respondLatencyMillis) {
    this.shuffleFetchRequestDurationMillis = shuffleFetchRequestDurationMillis;
    this.getChunkLatencyMillis = getChunkLatencyMillis;
    this.respondLatencyMillis = respondLatencyMillis;
  }

  public Timer getShuffleFetchRequestDurationMillis() {
    return shuffleFetchRequestDurationMillis;
  }

  public Timer getGetChunkLatencyMillis() {
    return getChunkLatencyMillis;
  }

  public Timer getRespondLatencyMillis() {
    return respondLatencyMillis;
  }
}
