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

import java.time.LocalDateTime
import java.time.LocalDateTime.now
import java.time.ZoneOffset.UTC

import com.microsoft.magnetar.pii.metadata.{
  DataType,
  JdbcMetadataRepository,
  MetadataEntity,
  MetadataRepository
}
import com.microsoft.magnetar.pii.metadata.DataType.NonPersonal
import org.apache.commons.dbcp2.BasicDataSource
import org.apache.hadoop.fs.Path

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{AnalysisException, DataFrameWriter, Dataset}
import org.apache.spark.sql.SaveMode.{ErrorIfExists, Overwrite}
import org.apache.spark.sql.pii.PIIConf._
import org.apache.spark.sql.pii.PIIUtils.{isParentOf, makeQualified, retry}

private[pii] final class PIIDataFrameWriter[T](
  ds: Dataset[T],
  metadataRepository: MetadataRepository,
  usePii: Boolean,
  controlEnv: String,
  piiRootPath: String,
  saveTime: => LocalDateTime
) extends DataFrameWriter[T](ds)
    with Logging {
  import PIIDataFrameWriter._

  override def save(): Unit = {
    withMetadata("save") {
      super.save()
    }
  }

  override def insertInto(tableName: String): Unit = {
    withMetadata("insertInto") {
      super.insertInto(tableName)
    }
  }

  override def saveAsTable(tableName: String): Unit = {
    withMetadata("saveAsTable") {
      super.saveAsTable(tableName)
    }
  }

  private def withMetadata(methodName: String)(save: => Unit): Unit = {
    if (!usePii) {
      save
      return
    }

    val dataType = extraOptions
      .get("dataType")
      .map(
        s =>
          DataType.values
            .find(_.toString == s)
            .getOrElse(
              throw new AnalysisException(s"Invalid PII data type '$s'.")
          )
      )
      .getOrElse(
        throw new AnalysisException("Option 'dataType' must be provided.")
      )

    if (dataType == NonPersonal) {
      save
      return
    }

    val path = new Path(
      extraOptions.getOrElse(
        "path",
        throw new AnalysisException(s"Option 'path' must be provided.")
      )
    )

    if (source != "caspian") {
      throw new AnalysisException(
        s"Format '$source' is not supported for storing PII data for compliance purposes."
      )
    }

    val root = new Path(piiRootPath)
    val conf = df.sparkSession.sessionState.newHadoopConf()

    if (!isParentOf(root, path, conf)) {
      throw new AnalysisException(
        s"PII data can only be stored in a sub folder of $root for compliance purposes."
      )
    }

    if (methodName != "save") {
      throw new AnalysisException(
        s"Method '$methodName' is not supported for storing PII data for compliance purposes."
      )
    }

    save

    logInfo(s"Writing metadata for $path")

    val qualifiedPath = makeQualified(path, conf).toString

    retry(MetadataWriteAttemptNum) {
      metadataRepository.findByPath(qualifiedPath) match {
        case Some(entity) =>
          if (mode == Overwrite || mode == ErrorIfExists) {
            metadataRepository.save(
              entity.copy(
                format = source,
                dataType = dataType,
                taggedColumns = None,
                createTime = saveTime,
                softDeleteStartTime = None,
                hardDeleteStartTimes = None,
                ipRemoved = false
              )
            )
          } else {
            metadataRepository.save(
              entity.copy(
                format = source,
                dataType = dataType,
                taggedColumns = None,
                ipRemoved = false
              )
            )
          }

        case None =>
          metadataRepository.save(
            MetadataEntity(
              qualifiedPath,
              controlEnv,
              source,
              dataType,
              None,
              saveTime,
              None,
              None,
              ipRemoved = false,
              None,
              None
            )
          )
      }
    }

    logInfo(s"Metadata for $path was written")
  }
}

private[sql] object PIIDataFrameWriter {
  private final val MetadataWriteAttemptNum = 3

  private lazy val defaultMetadataRepository = {
    val dataSource = new BasicDataSource()
    dataSource.setUrl(metadataDbUrl)
    dataSource.setUsername(metadataDbUser)
    dataSource.setPassword(metadataDbPassword)

    new JdbcMetadataRepository(dataSource)
  }

  def apply[T](ds: Dataset[T]): PIIDataFrameWriter[T] = {
    new PIIDataFrameWriter[T](
      ds,
      defaultMetadataRepository,
      isUsePII(ds.sparkSession.conf),
      controlEnv,
      piiDataPath,
      now(UTC)
    )
  }
}
