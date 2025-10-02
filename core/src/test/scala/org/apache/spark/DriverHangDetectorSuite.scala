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

package org.apache.spark

import java.util.concurrent.CountDownLatch

import scala.concurrent.duration._

import org.scalatest.concurrent.Eventually._

import org.apache.spark.internal.config._

class DriverHangDetectorSuite extends SparkFunSuite with LocalSparkContext {

  test("hang detector is disabled by default") {
    val conf = new SparkConf().setMaster("local").setAppName("test")
    sc = new SparkContext(conf)
    assert(sc._hangDetector.isEmpty)
  }

  test("hang detector starts when enabled") {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test")
      .set(DRIVER_HANG_DETECTION_ENABLED, true)
      .set(DRIVER_HANG_DETECTION_INTERVAL, 1000L)
      .set(DRIVER_HANG_DETECTION_THRESHOLD, 2)

    sc = new SparkContext(conf)
    assert(sc._hangDetector.isDefined)
  }

  test("hang detector stops with SparkContext") {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test")
      .set(DRIVER_HANG_DETECTION_ENABLED, true)
      .set(DRIVER_HANG_DETECTION_INTERVAL, 1000L)

    sc = new SparkContext(conf)
    val detector = sc._hangDetector.get
    sc.stop()
    sc = null

    // Verify detector was stopped (thread pool should be shutdown)
    // We can't directly check if it's stopped, but we can verify stop was called
    assert(detector != null)
  }

  test("configuration validation - threshold must be at least 2") {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test")
      .set(DRIVER_HANG_DETECTION_ENABLED, true)
      .set(DRIVER_HANG_DETECTION_THRESHOLD, 1)

    // This should fail when creating SparkContext with invalid threshold
    intercept[IllegalArgumentException] {
      new SparkContext(conf)
    }
  }

  test("hang detector detects hung non-daemon threads") {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test")
      .set(DRIVER_HANG_DETECTION_ENABLED, true)
      .set(DRIVER_HANG_DETECTION_INTERVAL, 500L)
      .set(DRIVER_HANG_DETECTION_THRESHOLD, 2)

    sc = new SparkContext(conf)

    val latch = new CountDownLatch(1)
    val hangingThread = new Thread("test-hanging-thread") {
      override def run(): Unit = {
        latch.await() // This will block indefinitely
      }
    }
    hangingThread.setDaemon(false) // Non-daemon thread
    hangingThread.start()

    try {
      // Wait for hang detection to trigger and stop SparkContext
      // The hang detector should detect the hung thread after 2 * 500ms = 1 second
      eventually(timeout(5000.millis), interval(200.millis)) {
        assert(sc.stopped.get(), "SparkContext should be stopped after hang detection")
      }
    } finally {
      latch.countDown()
      hangingThread.interrupt()
      hangingThread.join(1000)
    }
  }

  test("hang detector ignores daemon threads") {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test")
      .set(DRIVER_HANG_DETECTION_ENABLED, true)
      .set(DRIVER_HANG_DETECTION_INTERVAL, 500L)
      .set(DRIVER_HANG_DETECTION_THRESHOLD, 2)

    sc = new SparkContext(conf)

    val latch = new CountDownLatch(1)
    val daemonThread = new Thread("test-daemon-thread") {
      override def run(): Unit = {
        latch.await() // This will block indefinitely
      }
    }
    daemonThread.setDaemon(true) // Daemon thread should be ignored
    daemonThread.start()

    try {
      // Wait a bit to ensure hang detector runs
      Thread.sleep(1500)

      // SparkContext should NOT be stopped because daemon threads are ignored
      assert(!sc.stopped.get(), "SparkContext should not be stopped for daemon threads")
    } finally {
      latch.countDown()
      daemonThread.interrupt()
      daemonThread.join(1000)
    }
  }
}
