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

package org.apache.spark.examples

import scala.util.Random

import org.apache.spark.sql.SparkSession

/**
 * Example demonstrating Spark's shuffle rebalancing feature for balancing shuffle data
 * across executors. This helps with skewed shuffle scenarios where some executors
 * hold significantly more shuffle data than others.
 */
object ShuffleRebalanceExample {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("Shuffle Move Example")
      .config("spark.shuffle.rebalance.enabled", "true")           // Enable shuffle rebalancing
      .config("spark.shuffle.rebalance.threshold", "1.5")          // Trigger when imbalance > 1.5x
      .config("spark.shuffle.rebalance.minSizeMB", "100")          // Only move if difference > 100MB
      .config("spark.shuffle.rebalance.checkIntervalMs", "30000")  // Check every 30 seconds
      .config("spark.shuffle.rebalance.maxConcurrent", "2")        // Max 2 concurrent moves
      .getOrCreate()

    import spark.implicits._

    try {
      // Create a dataset that will result in skewed shuffle
      val data = spark.sparkContext.parallelize(1 to 1000000, 100)
        .map { i =>
          // Create artificial skew - most data goes to a few partitions
          val key = if (i % 10 == 0) s"hot_key_${i % 3}" else s"normal_key_${i % 100}"
          val value = Random.nextString(100) // Some data to make it substantial
          (key, value)
        }

      println("Creating skewed shuffle data...")
      
      // This will trigger a shuffle operation
      val shuffledData = data
        .groupByKey()
        .map { case (key, values) => 
          (key, values.size)
        }

      // Force evaluation to trigger shuffle
      val result = shuffledData.collect()

      println(s"Processed ${result.length} unique keys")
      
      // Show some results to demonstrate the skew
      val sortedResults = result.sortBy(_._2).reverse
      println("\nTop 10 keys by count (showing skew):")
      sortedResults.take(10).foreach { case (key, count) =>
        println(s"$key: $count")
      }

      // Perform another shuffle operation to potentially trigger shuffle rebalancing
      println("\nPerforming another shuffle operation...")
      val repartitionedData = shuffledData
        .repartition(50) // This will cause another shuffle
        .cache()

      val finalCount = repartitionedData.count()
      println(s"Final count after repartitioning: $finalCount")

      // Additional operations to keep the job running and allow shuffle rebalancings
      Thread.sleep(60000) // Wait for potential shuffle rebalancings to occur

      println("Example completed. Check logs for shuffle rebalancing activity.")

    } finally {
      spark.stop()
    }
  }
}