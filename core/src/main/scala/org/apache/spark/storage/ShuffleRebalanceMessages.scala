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

package org.apache.spark.storage


private[spark] object ShuffleRebalanceMessages {

  //////////////////////////////////////////////////////////////////////////////////
  // Core message for shuffle rebalancing
  //////////////////////////////////////////////////////////////////////////////////

  /**
   * Driver to Source Executor: Transfer shuffle blocks to target executor.
   *
   * This is the only message needed for shuffle rebalancing:
   * 1. Driver sends this to source executor
   * 2. Source executor uploads blocks directly to target executor
   * 3. Target executor sends UpdateBlockInfo to driver (existing Spark mechanism)
   */
  case class SendShuffleBlocks(
      targetExecutor: BlockManagerId,
      blocks: Seq[(BlockId, Long)],  // (blockId, size) pairs
      operationId: String,           // Unique ID for this transfer operation
      priority: Int = 1              // Transfer priority (1=high, 3=low)
  )

  /**
   * Driver to Executor: Cancel an ongoing shuffle rebalancing operation.
   */
  case class CancelShuffleRebalance(
      operationId: String,
      reason: String = "Driver requested cancellation"
  )

  /**
   * Driver to Executor: Update configuration for shuffle rebalancing.
   */
  case class UpdateShuffleRebalanceConfig(
      maxConcurrentTransfers: Int,
      transferTimeoutMs: Long,
      batchSize: Int
  )

  //////////////////////////////////////////////////////////////////////////////////
  // Optional messages for rate limiting and coordination
  //////////////////////////////////////////////////////////////////////////////////

  /**
   * Executor to Driver: Request permission to start a transfer operation.
   * Used for rate limiting and coordination (optional).
   */
  case class RequestShuffleTransferPermission(
      sourceExecutor: String,
      targetExecutor: String,
      estimatedBytes: Long,
      estimatedBlocks: Int
  )

  /**
   * Driver to Executor: Response to transfer permission request.
   */
  case class ShuffleTransferPermissionResponse(
      permitted: Boolean,
      maxBytesAllowed: Long,
      maxBlocksAllowed: Int,
      retryAfterMs: Option[Long] = None
  )
}
