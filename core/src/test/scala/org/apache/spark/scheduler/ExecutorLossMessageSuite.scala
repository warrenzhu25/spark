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

import org.apache.spark.SparkFunSuite

class ExecutorLossMessageSuite extends SparkFunSuite {

  test("ExecutorLossMessage contains standard decommission message") {
    assert(ExecutorLossMessage.decommissionFinished === "Finished decommissioning")
  }

  test("ExecutorLossMessage decommission message is non-empty") {
    assert(ExecutorLossMessage.decommissionFinished.nonEmpty)
    assert(ExecutorLossMessage.decommissionFinished.length > 0)
  }

  test("ExecutorLossMessage can be concatenated with additional info") {
    val additionalInfo = " - 5 blocks migrated"
    val fullMessage = ExecutorLossMessage.decommissionFinished + additionalInfo
    
    assert(fullMessage === "Finished decommissioning - 5 blocks migrated")
    assert(fullMessage.startsWith(ExecutorLossMessage.decommissionFinished))
  }

  test("ExecutorLossMessage format is consistent") {
    val message = ExecutorLossMessage.decommissionFinished
    
    // Verify standard message format
    assert(message.startsWith("Finished"))
    assert(message.contains("decommissioning"))
    assert(!message.endsWith(".")) // Should not end with punctuation for concatenation
  }
}