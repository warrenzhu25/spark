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

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{
  AnalysisException,
  DataFrame,
  DataFrameReader,
  SparkSession
}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.pii.PIIConf.{isUsePII, piiDataPath}
import org.apache.spark.sql.pii.PIIUtils.isParentOf

private[pii] class PIIDataFrameReader(_sparkSession: SparkSession,
                                      usePii: Boolean,
                                      piiRootPath: String)
    extends DataFrameReader(_sparkSession) {
  override def load(paths: String*): DataFrame = {
    if (!usePii) {
      val allPaths = CaseInsensitiveMap(extraOptions.toMap).get("path").toSeq ++
        paths
      val root = new Path(piiRootPath)
      val conf = sparkSession.sessionState.newHadoopConf()

      if (allPaths.exists(path => isParentOf(root, new Path(path), conf))) {
        throw new AnalysisException(
          s"PII data under $root can only be read when spark.pii.enabled=true"
        )
      }
    }

    super.load(paths: _*)
  }
}

private[sql] object PIIDataFrameReader {
  def apply(sparkSession: SparkSession): PIIDataFrameReader = {
    new PIIDataFrameReader(
      sparkSession,
      isUsePII(sparkSession.conf),
      piiDataPath
    )
  }
}
