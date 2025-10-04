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

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._

/**
 * Matches submitted stages against historical profiles.
 *
 * Uses a multi-level matching strategy:
 * 1. Exact match by signature hash (fastest)
 * 2. Fuzzy match with confidence scoring (for variations)
 *
 * The matching algorithm weighs different signature components:
 * - SQL plan hash (50% weight) - strongest signal for SQL workloads
 * - RDD operation chain (30% weight) - for RDD API workloads
 * - Partition count (10% weight) - allows some variance
 * - Dependencies (10% weight) - shuffle and parent stages
 *
 * @param profileStore Store containing historical stage profiles
 * @param conf SparkConf for configuration parameters
 */
private[spark] class StageMatcher(
    profileStore: StageProfileStore,
    conf: SparkConf) extends Logging {

  private val confidenceThreshold = conf.get(DYN_ALLOCATION_STAGE_MATCHING_CONFIDENCE_THRESHOLD)
  private val useSQLPlan = conf.get(DYN_ALLOCATION_STAGE_MATCHING_USE_SQL_PLAN)
  private val useRDDChain = conf.get(DYN_ALLOCATION_STAGE_MATCHING_USE_RDD_CHAIN)

  /**
   * Match a submitted stage against historical profiles.
   *
   * First attempts exact match by signature hash. If that fails, performs fuzzy matching
   * across all profiles and returns the best match above confidence threshold.
   *
   * @param stageInfo The StageInfo of the submitted stage
   * @param sqlPlan Optional SQL physical plan description from SQLExecution
   * @return Some((profile, confidence)) if a match is found, None otherwise
   */
  def matchStage(
      stageInfo: StageInfo,
      sqlPlan: Option[String] = None): Option[(StageProfile, Double)] = {
    val signature = extractSignature(stageInfo, sqlPlan)
    val signatureHash = signature.toHash

    // Try exact match first
    profileStore.getProfile(signatureHash) match {
      case Some(profile) =>
        logDebug(s"Exact match found for stage ${stageInfo.stageId} with hash $signatureHash")
        Some((profile, 1.0))
      case None =>
        // Try fuzzy matching
        fuzzyMatch(signature)
    }
  }

  /**
   * Extract a stable signature from StageInfo.
   *
   * @param stageInfo Stage information
   * @param sqlPlan Optional SQL physical plan description
   * @return StageSignature for matching
   */
  def extractSignature(
      stageInfo: StageInfo,
      sqlPlan: Option[String] = None): StageSignature = {
    // Extract RDD operation chain
    val rddOps = stageInfo.rddInfos.map(_.name)
    val rddChainHash = hashString(rddOps.mkString("->"))

    // Extract SQL plan info if available
    val (sqlPlanHash, sqlPlanOps) = sqlPlan match {
      case Some(plan) if useSQLPlan =>
        val operators = extractSQLOperators(plan)
        (Some(hashString(operators.mkString("->"))), Some(operators))
      case _ =>
        (None, None)
    }

    // Extract call site pattern (optional, less weight)
    val callSitePattern = extractCallSitePattern(stageInfo.details)

    StageSignature(
      sqlPlanHash = sqlPlanHash,
      sqlPlanOperators = sqlPlanOps,
      rddOperationChain = rddOps,
      rddChainHash = rddChainHash,
      stageName = stageInfo.name,
      hasShuffleDependency = stageInfo.shuffleDepId.isDefined,
      shuffleDepId = stageInfo.shuffleDepId,
      numPartitions = stageInfo.numTasks,
      parentStageCount = stageInfo.parentIds.size,
      callSitePattern = callSitePattern
    )
  }

  /**
   * Calculate similarity score between two signatures.
   *
   * Returns a score from 0.0 (completely different) to 1.0 (identical).
   * Uses weighted components based on stability and relevance.
   *
   * @param sig1 First signature
   * @param sig2 Second signature
   * @return Similarity score between 0.0 and 1.0
   */
  def calculateSimilarity(sig1: StageSignature, sig2: StageSignature): Double = {
    var score = 0.0
    var totalWeight = 0.0

    // SQL plan hash (50% weight - strongest signal)
    if (sig1.sqlPlanHash.isDefined && sig2.sqlPlanHash.isDefined && useSQLPlan) {
      totalWeight += 0.5
      if (sig1.sqlPlanHash == sig2.sqlPlanHash) {
        score += 0.5
      } else {
        // Partial credit for operator sequence similarity
        val opSimilarity = sig1.sqlPlanOperators.flatMap { ops1 =>
          sig2.sqlPlanOperators.map { ops2 =>
            sequenceSimilarity(ops1, ops2)
          }
        }.getOrElse(0.0)
        score += 0.5 * opSimilarity
      }
    }

    // RDD operation chain (30% weight)
    if (useRDDChain && sig1.rddOperationChain.nonEmpty && sig2.rddOperationChain.nonEmpty) {
      totalWeight += 0.3
      if (sig1.rddChainHash == sig2.rddChainHash) {
        score += 0.3
      } else {
        val rddSimilarity = sequenceSimilarity(sig1.rddOperationChain, sig2.rddOperationChain)
        score += 0.3 * rddSimilarity
      }
    }

    // Partition count (10% weight - allow variance)
    totalWeight += 0.1
    val partitionRatio = math.min(sig1.numPartitions, sig2.numPartitions).toDouble /
      math.max(sig1.numPartitions, sig2.numPartitions)
    if (partitionRatio > 0.8) {
      score += 0.1 * partitionRatio
    }

    // Dependencies (10% weight)
    totalWeight += 0.1
    if (sig1.hasShuffleDependency == sig2.hasShuffleDependency) {
      score += 0.05
    }
    if (sig1.parentStageCount == sig2.parentStageCount) {
      score += 0.05
    }

    // Normalize by total weight
    if (totalWeight > 0) score / totalWeight else 0.0
  }

  /**
   * Generate hash string from input string.
   *
   * @param s Input string
   * @return SHA-256 hash as hex string
   */
  def hashString(s: String): String = {
    val digest = java.security.MessageDigest.getInstance("SHA-256")
    digest.digest(s.getBytes("UTF-8")).map("%02x".format(_)).mkString
  }

  /**
   * Perform fuzzy matching against all profiles.
   *
   * @param signature Signature to match
   * @return Best matching profile with confidence score, if above threshold
   */
  private def fuzzyMatch(signature: StageSignature): Option[(StageProfile, Double)] = {
    val profiles = profileStore.getAllProfiles()
    if (profiles.isEmpty) {
      logDebug("No profiles available for fuzzy matching")
      return None
    }

    val scored = profiles.map { profile =>
      val similarity = calculateSimilarity(signature, profile.signature)
      (profile, similarity)
    }

    val bestMatch = scored.maxBy(_._2)
    if (bestMatch._2 >= confidenceThreshold) {
      logInfo(s"Fuzzy match found with confidence ${bestMatch._2 * 100}%")
      Some(bestMatch)
    } else {
      logDebug(s"Best match had confidence ${bestMatch._2 * 100}%, " +
        s"below threshold ${confidenceThreshold * 100}%")
      None
    }
  }

  /**
   * Calculate similarity between two sequences using longest common subsequence.
   *
   * @param seq1 First sequence
   * @param seq2 Second sequence
   * @return Similarity score between 0.0 and 1.0
   */
  private def sequenceSimilarity(seq1: Seq[String], seq2: Seq[String]): Double = {
    if (seq1.isEmpty || seq2.isEmpty) return 0.0
    if (seq1 == seq2) return 1.0

    val lcsLength = longestCommonSubsequence(seq1, seq2)
    lcsLength.toDouble / math.max(seq1.length, seq2.length)
  }

  /**
   * Compute length of longest common subsequence.
   *
   * @param seq1 First sequence
   * @param seq2 Second sequence
   * @return Length of LCS
   */
  private def longestCommonSubsequence(seq1: Seq[String], seq2: Seq[String]): Int = {
    val m = seq1.length
    val n = seq2.length
    val dp = Array.ofDim[Int](m + 1, n + 1)

    for (i <- 1 to m) {
      for (j <- 1 to n) {
        if (seq1(i - 1) == seq2(j - 1)) {
          dp(i)(j) = dp(i - 1)(j - 1) + 1
        } else {
          dp(i)(j) = math.max(dp(i - 1)(j), dp(i)(j - 1))
        }
      }
    }

    dp(m)(n)
  }

  /**
   * Extract SQL operators from physical plan description.
   *
   * Parses plan text to extract operator types in execution order.
   *
   * @param planDescription Physical plan description string
   * @return Sequence of operator names
   */
  private def extractSQLOperators(planDescription: String): Seq[String] = {
    // Common SQL operators to extract
    val operatorPatterns = Seq(
      "Scan" -> """Scan\s+\w+""".r,
      "Filter" -> """Filter\s+""".r,
      "Project" -> """Project\s+""".r,
      "HashJoin" -> """.*HashJoin.*""".r,
      "SortMergeJoin" -> """.*SortMergeJoin.*""".r,
      "BroadcastHashJoin" -> """.*BroadcastHashJoin.*""".r,
      "HashAggregate" -> """HashAggregate\(""".r,
      "SortAggregate" -> """SortAggregate\(""".r,
      "Exchange" -> """Exchange\s+""".r,
      "Sort" -> """Sort\s+""".r,
      "Window" -> """Window\s+""".r
    )

    val lines = planDescription.split("\n")
    val operators = scala.collection.mutable.ArrayBuffer[String]()

    for (line <- lines) {
      for ((name, pattern) <- operatorPatterns) {
        if (pattern.findFirstIn(line).isDefined && !operators.contains(name)) {
          operators += name
        }
      }
    }

    operators.toSeq
  }

  /**
   * Extract a pattern from call site details for matching.
   *
   * Extracts class and method names while ignoring line numbers.
   *
   * @param details Call site details string
   * @return Optional call site pattern
   */
  private def extractCallSitePattern(details: String): Option[String] = {
    if (details == null || details.isEmpty) return None

    // Extract class and method, ignore line numbers
    val classMethodPattern = """(\w+\.\w+(?:\.\w+)*)\(""".r
    classMethodPattern.findFirstMatchIn(details).map(_.group(1))
  }
}
