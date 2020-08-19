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

package org.apache.spark.status.insight.heuristics

object ConfigurationHeuristicsConstants {
  val JVM_USED_MEMORY = "jvmUsedMemory"

  // Spark configuration parameters
  val SPARK_EXECUTOR_MEMORY = "spark.executor.memory"
  val SPARK_DRIVER_MEMORY = "spark.driver.memory"
  val SPARK_EXECUTOR_MEMORY_OVERHEAD = "spark.yarn.executor.memoryOverhead"
  val SPARK_DRIVER_MEMORY_OVERHEAD = "spark.yarn.driver.memoryOverhead"
  val SPARK_EXECUTOR_CORES = "spark.executor.cores"
  val SPARK_DRIVER_CORES = "spark.driver.cores"
  val SPARK_EXECUTOR_INSTANCES = "spark.executor.instances"
  val SPARK_EXECUTOR_OPTIONS = "spark.executor.extraJavaOptions"
  val SPARK_SQL_SHUFFLE_PARTITIONS = "spark.sql.shuffle.partitions"
  val SPARK_MEMORY_FRACTION = "spark.memory.fraction"
  val SPARK_SERIALIZER = "spark.serializer"
  val SPARK_APPLICATION_DURATION = "spark.application.duration"
  val SPARK_SHUFFLE_SERVICE_ENABLED = "spark.shuffle.service.enabled"
  val SPARK_DYNAMIC_ALLOCATION_ENABLED = "spark.dynamicAllocation.enabled"
  val SPARK_OFF_HEAP_ENABLED = "spark.memory.offHeap.enabled"
  val SPARK_OFF_HEAP_SIZE = "spark.memory.offHeap.size"
  val SPARK_DYNAMIC_ALLOCATION_MIN_EXECUTORS = "spark.dynamicAllocation.minExecutors"
  val SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS = "spark.dynamicAllocation.maxExecutors"

  // Spark default configuration values
  val SPARK_EXECUTOR_MEMORY_DEFAULT = "1g"
  val SPARK_EXECUTOR_MEMORY_OVERHEAD_MIN = 384 * 1024 * 1024
  val SPARK_DRIVER_MEMORY_DEFAULT = "1g"
  val SPARK_EXECUTOR_CORES_DEFAULT = 1
  val SPARK_DRIVER_CORES_DEFAULT = 1
  val SPARK_SQL_SHUFFLE_PARTITIONS_DEFAULT = 200
  val SPARK_MEMORY_FRACTION_DEFAULT = 0.6
}