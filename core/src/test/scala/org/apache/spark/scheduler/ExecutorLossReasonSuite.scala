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

package org.apache.spark.scheduler

import scala.concurrent.duration._

import org.apache.spark.SparkFunSuite
import org.apache.spark.storage.{MigrationInfo, MigrationStat}

class ExecutorLossReasonSuite extends SparkFunSuite {

  test("DecommissionSummary toString") {
    val decommissionSummary = DecommissionSummary(
      decommissionTime = 1000.millis,
      migrationTime = 500.millis,
      taskWaitingTime = 100.millis,
      migrationInfo = MigrationInfo(
        100000L,
        true,
        MigrationStat(
          numMigratedBlock = 10,
          totalMigratedSize = 1024,
          numBlocksLeft = 2
        )
      )
    )
    val expectedString = "Decommission finished in 1,000 ms. " +
      "Migration finished in 500 ms " +
      "(including 100 ms running task waiting time). " +
      "10 blocks of size 1024.0 B migrated, " +
      "2 blocks not migrated."
    assert(decommissionSummary.toString === expectedString)
  }
}
