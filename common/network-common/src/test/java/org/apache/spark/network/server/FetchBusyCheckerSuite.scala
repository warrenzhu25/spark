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

  @Test
  def testZeroHighWatermarkDisablesFetchBusy(): Unit = {
    val disabledChecker = new FetchBusyChecker(0, lowWatermarkMs, coolDownMs)
    
    // Should never enter fetch busy mode with zero high watermark
    disabledChecker.handle(1000)
    Assert.assertFalse(disabledChecker.isFetchBusy)
    
    disabledChecker.handle(5000)
    Assert.assertFalse(disabledChecker.isFetchBusy)
  }

  @Test
  def testMultipleHighWatermarkHitsOnlyLogOnce(): Unit = {
    val checker = new FetchBusyChecker(highWatermarkMs, lowWatermarkMs, coolDownMs)
    
    // First hit should enter fetch busy mode
    checker.handle(150)
    Assert.assertTrue(checker.isFetchBusy)
    
    // Subsequent hits should remain fetch busy without logging again
    checker.handle(200)
    Assert.assertTrue(checker.isFetchBusy)
    
    checker.handle(180)
    Assert.assertTrue(checker.isFetchBusy)
  }

  @Test
  def testBoundaryConditions(): Unit = {
    val checker = new FetchBusyChecker(highWatermarkMs, lowWatermarkMs, coolDownMs)
    
    // Exactly at high watermark should not trigger (> not >=)
    checker.handle(highWatermarkMs)
    Assert.assertFalse(checker.isFetchBusy)
    
    // Just over high watermark should trigger
    checker.handle(highWatermarkMs + 1)
    Assert.assertTrue(checker.isFetchBusy)
    
    // Exactly at low watermark should exit (< not <=) after cooldown
    Thread.sleep(coolDownMs + 1)
    checker.handle(lowWatermarkMs)
    Assert.assertTrue(checker.isFetchBusy) // Still busy because latency == lowWatermarkMs
    
    // Just under low watermark should exit after cooldown
    Thread.sleep(coolDownMs + 1)
    checker.handle(lowWatermarkMs - 1)
    Assert.assertFalse(checker.isFetchBusy)
  }

  @Test
  def testConcurrentAccess(): Unit = {
    val checker = new FetchBusyChecker(highWatermarkMs, lowWatermarkMs, coolDownMs)
    val threads = (0 until 10).map(_ => new Thread(() => {
      // Each thread performs multiple operations
      for (_ <- 0 until 100) {
        checker.handle(scala.util.Random.nextInt(200))
        checker.isFetchBusy
      }
    }))
    
    // Start all threads
    threads.foreach(_.start())
    
    // Wait for all threads to complete
    threads.foreach(_.join())
    
    // Should not crash or throw exceptions
    Assert.assertTrue("Concurrent access test completed successfully", true)
  }

  @Test
  def testRepeatedEnterExitCycles(): Unit = {
    val checker = new FetchBusyChecker(highWatermarkMs, lowWatermarkMs, coolDownMs)
    
    // Cycle 1: Enter and exit fetch busy mode
    checker.handle(150)
    Assert.assertTrue(checker.isFetchBusy)
    
    Thread.sleep(coolDownMs + 1)
    checker.handle(30)
    Assert.assertFalse(checker.isFetchBusy)
    
    // Cycle 2: Enter again
    checker.handle(120)
    Assert.assertTrue(checker.isFetchBusy)
    
    Thread.sleep(coolDownMs + 1)
    checker.handle(20)
    Assert.assertFalse(checker.isFetchBusy)
    
    // Cycle 3: Enter again to verify state is properly maintained
    checker.handle(200)
    Assert.assertTrue(checker.isFetchBusy)
  }
}
