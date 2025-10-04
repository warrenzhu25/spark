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

import java.io.{FileOutputStream, IOException}
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import scala.io.Source
import scala.util.control.NonFatal

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging

/**
 * Storage and retrieval of historical stage profiles.
 *
 * Supports loading stage profiles from external JSON files and provides
 * in-memory caching for fast lookup during stage submission. Profiles can
 * be loaded from local files or HDFS/cloud storage paths.
 *
 * The store maintains profiles indexed by their signature hash for O(1) lookup.
 *
 * @param conf SparkConf for Hadoop configuration access
 */
private[spark] class StageProfileStore(conf: SparkConf) extends Logging {

  // In-memory cache: signature hash -> profile
  private val profiles = new ConcurrentHashMap[String, StageProfile]()

  // Jackson mapper for JSON serialization
  private val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  /**
   * Load stage profiles from a JSON file.
   *
   * Supports both local file:// paths and HDFS paths. The file should contain
   * a StageProfileCollection in JSON format.
   *
   * @param path Path to the JSON file (local or HDFS)
   * @throws IOException if the file cannot be read or parsed
   */
  def loadFromFile(path: String): Unit = {
    try {
      val json = readFileContent(path)
      val loadedProfiles = loadFromJson(json)
      logInfo(s"Loaded ${loadedProfiles.size} stage profiles from $path")
    } catch {
      case NonFatal(e) =>
        logError(s"Failed to load stage profiles from $path", e)
        throw new IOException(s"Failed to load stage profiles from $path", e)
    }
  }

  /**
   * Load stage profiles from a JSON string.
   *
   * @param json JSON string containing a StageProfileCollection
   * @return Sequence of loaded StageProfile objects
   * @throws IOException if JSON parsing fails
   */
  def loadFromJson(json: String): Seq[StageProfile] = {
    try {
      val collection = mapper.readValue(json, classOf[StageProfileCollection])
      collection.profiles.foreach { profile =>
        profiles.put(profile.signatureHash, profile)
      }
      collection.profiles
    } catch {
      case NonFatal(e) =>
        logError("Failed to parse stage profiles from JSON", e)
        throw new IOException("Failed to parse stage profiles from JSON", e)
    }
  }

  /**
   * Get a stage profile by its signature hash.
   *
   * @param signatureHash Hash of the stage signature
   * @return Some(StageProfile) if found, None otherwise
   */
  def getProfile(signatureHash: String): Option[StageProfile] = {
    Option(profiles.get(signatureHash))
  }

  /**
   * Get all cached profiles.
   *
   * Used for fuzzy matching when exact hash match fails.
   *
   * @return Sequence of all StageProfile objects
   */
  def getAllProfiles(): Seq[StageProfile] = {
    profiles.values().asScala.toSeq
  }

  /**
   * Update or add a stage profile.
   *
   * If a profile with the same signature hash exists, it will be replaced.
   *
   * @param profile StageProfile to add or update
   */
  def updateProfile(profile: StageProfile): Unit = {
    profiles.put(profile.signatureHash, profile)
  }

  /**
   * Export all cached profiles to a JSON string.
   *
   * @param applicationName Optional application name for the collection
   * @param appIds Optional list of application IDs
   * @return JSON string containing a StageProfileCollection
   */
  def exportToJson(
      applicationName: Option[String] = None,
      appIds: Seq[String] = Seq.empty): String = {
    val collection = StageProfileCollection(
      version = "1.0",
      applicationName = applicationName,
      collectedFrom = appIds,
      collectionTimestamp = Some(java.time.Instant.now().toString),
      profiles = profiles.values().asScala.toSeq
    )
    mapper.writerWithDefaultPrettyPrinter().writeValueAsString(collection)
  }

  /**
   * Save all cached profiles to a JSON file.
   *
   * @param path Output file path (local or HDFS)
   * @param applicationName Optional application name for the collection
   * @param appIds Optional list of application IDs
   * @throws IOException if the file cannot be written
   */
  def saveToFile(
      path: String,
      applicationName: Option[String] = None,
      appIds: Seq[String] = Seq.empty): Unit = {
    try {
      val json = exportToJson(applicationName, appIds)
      writeFileContent(path, json)
      logInfo(s"Saved ${profiles.size()} stage profiles to $path")
    } catch {
      case NonFatal(e) =>
        logError(s"Failed to save stage profiles to $path", e)
        throw new IOException(s"Failed to save stage profiles to $path", e)
    }
  }

  /**
   * Clear all cached profiles.
   */
  def clear(): Unit = {
    profiles.clear()
  }

  /**
   * Get the number of cached profiles.
   *
   * @return Number of profiles in the cache
   */
  def size: Int = profiles.size()

  /**
   * Read file content from local filesystem or HDFS.
   *
   * @param path File path
   * @return File content as string
   */
  private def readFileContent(path: String): String = {
    if (path.startsWith("file://") || !path.contains("://")) {
      // Local file
      val localPath = path.stripPrefix("file://")
      Source.fromFile(localPath).mkString
    } else {
      // HDFS or other Hadoop-compatible filesystem
      val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)
      val hdfsPath = new Path(path)
      val fs = hdfsPath.getFileSystem(hadoopConf)
      val stream = fs.open(hdfsPath)
      try {
        Source.fromInputStream(stream).mkString
      } finally {
        stream.close()
      }
    }
  }

  /**
   * Write content to local filesystem or HDFS.
   *
   * @param path File path
   * @param content Content to write
   */
  private def writeFileContent(path: String, content: String): Unit = {
    if (path.startsWith("file://") || !path.contains("://")) {
      // Local file
      val localPath = path.stripPrefix("file://")
      val out = new FileOutputStream(localPath)
      try {
        out.write(content.getBytes("UTF-8"))
      } finally {
        out.close()
      }
    } else {
      // HDFS or other Hadoop-compatible filesystem
      val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)
      val hdfsPath = new Path(path)
      val fs = hdfsPath.getFileSystem(hadoopConf)
      val stream = fs.create(hdfsPath, true)
      try {
        stream.write(content.getBytes("UTF-8"))
      } finally {
        stream.close()
      }
    }
  }
}
