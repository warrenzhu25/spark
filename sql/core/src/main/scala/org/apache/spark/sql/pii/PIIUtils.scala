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

package org.apache.spark.sql.pii

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.internal.Logging

private object PIIUtils extends Logging {
  def makeQualified(path: Path, conf: Configuration): Path = {
    val fileSystem = path.getFileSystem(conf)
    fileSystem.makeQualified(path)
  }

  def isParentOf(path1: Path, path2: Path, conf: Configuration): Boolean = {
    val qualifiedPath1 = makeQualified(path1, conf).toString
    val qualifiedPath2 = makeQualified(path2, conf).toString
    qualifiedPath2.length > qualifiedPath1.length &&
    qualifiedPath2.startsWith(qualifiedPath1) &&
    qualifiedPath2.charAt(qualifiedPath1.length) == '/'
  }

  @annotation.tailrec
  def retry[T](n: Int)(f: => T): T = {
    try {
      return f
    } catch {
      case e if n > 1 =>
        logError(s"Exception occurred, retrying (${n - 1} attempt(s) left)", e)
      case e: Throwable =>
        logError(s"Exception occurred, retry attempts exhausted")
        throw e
    }
    retry(n - 1)(f)
  }
}
