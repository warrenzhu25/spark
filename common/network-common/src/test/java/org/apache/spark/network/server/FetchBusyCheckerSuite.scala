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
package org.apache.spark.network.server

import org.junit.Assert
import org.junit.Test

class FetchBusyCheckerSuite {

  val highWatermarkMs = 100
  val lowWatermarkMs = 50
  val coolDownMs = 10
  val fetchBusyChecker = new FetchBusyChecker(highWatermarkMs, lowWatermarkMs, coolDownMs)
  @Test
  def testEnterFetchBusyWhenLatencyReachHighWatermark(): Unit = {
    fetchBusyChecker.handle(150)
    Assert.assertTrue(fetchBusyChecker.isFetchBusy)
  }

  @Test
  def testEnterFetchBusyWhenLatencyReachLowWatermark(): Unit = {
    fetchBusyChecker.handle(150)
    Assert.assertTrue(fetchBusyChecker.isFetchBusy)

    // Still in fetch busy mode when latency reach low watermark and cool down period not reached
    fetchBusyChecker.handle(30)
    Assert.assertTrue(fetchBusyChecker.isFetchBusy)

    // Exit fetch busy mode when latency reach low watermark and cool down period reached
    Thread.sleep(coolDownMs + 1)
    fetchBusyChecker.handle(40)
    Assert.assertFalse(fetchBusyChecker.isFetchBusy)
  }
}
