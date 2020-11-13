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

import java.io.File
import java.time.LocalDateTime
import java.time.LocalDateTime.now
import java.time.ZoneOffset.UTC

import com.microsoft.magnetar.pii.metadata.{
  MetadataEntity,
  MetadataRepository
}
import com.microsoft.magnetar.pii.metadata.DataType.{
  IntermediateEngineering,
  LongTail
}
import org.apache.hadoop.fs.Path
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfter

import org.apache.spark.sql._
import org.apache.spark.sql.SaveMode.{Append, Overwrite}
import org.apache.spark.sql.pii.PIIUtils.makeQualified
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{
  IntegerType,
  StringType,
  StructField,
  StructType
}
import org.apache.spark.util.Utils.{createTempDir, deleteRecursively}

class PIIDataFrameWriterSuite
    extends QueryTest
    with SharedSparkSession
    with BeforeAndAfter {
  import testImplicits._

  private var metadataRepository: MetadataRepository = _
  private var piiRootPath: String = _
  private var path: String = _

  before {
    metadataRepository = mock(classOf[MetadataRepository])
    piiRootPath = createTempDir().getCanonicalPath
    path = new File(piiRootPath, "data").getCanonicalPath
  }

  after {
    try {
      deleteRecursively(new File(piiRootPath))
    } finally {
      piiRootPath = null
      path = null
      metadataRepository = null
    }
  }

  test("save should succeed when usePii is false") {
    val piiDataFrameWriter = new PIIDataFrameWriter[Row](
      createDataFrame(),
      metadataRepository,
      false,
      "controlEnv1",
      piiRootPath,
      now(UTC)
    ).format("caspian").mode(Overwrite)

    piiDataFrameWriter.save(path)

    verify(metadataRepository, never()).save(any())

    assertDataFrame(new DataFrameReader(spark).format("caspian").load(path))
  }

  test("save should fail when usePii is true and dataType is not provided") {
    val piiDataFrameWriter = new PIIDataFrameWriter[Row](
      createDataFrame(),
      metadataRepository,
      true,
      "controlEnv1",
      piiRootPath,
      now(UTC)
    ).format("caspian").mode(Overwrite)

    assertThrows[AnalysisException] {
      piiDataFrameWriter.save(path)
    }

    verify(metadataRepository, never()).save(any())
  }

  test("save should fail when usePii is true and dataType is invalid") {
    val piiDataFrameWriter = new PIIDataFrameWriter[Row](
      createDataFrame(),
      metadataRepository,
      true,
      "controlEnv1",
      piiRootPath,
      now(UTC)
    ).format("caspian").option("dataType", "dataType1").mode(Overwrite)

    assertThrows[AnalysisException] {
      piiDataFrameWriter.save(path)
    }

    verify(metadataRepository, never()).save(any())
  }

  test("save should succeed when usePii is true and dataType is NonPersonal") {
    val piiDataFrameWriter = new PIIDataFrameWriter[Row](
      createDataFrame(),
      metadataRepository,
      true,
      "controlEnv1",
      piiRootPath,
      now(UTC)
    ).format("caspian").option("dataType", "NonPersonal").mode(Overwrite)

    piiDataFrameWriter.save(path)

    verify(metadataRepository, never()).save(any())

    assertDataFrame(new DataFrameReader(spark).format("caspian").load(path))
  }

  test("save should fail when usePii is true and path is not provided") {
    val piiDataFrameWriter = new PIIDataFrameWriter[Row](
      createDataFrame(),
      metadataRepository,
      true,
      "controlEnv1",
      piiRootPath,
      now(UTC)
    ).format("caspian")
      .option("dataType", "IntermediateEngineering")
      .mode(Overwrite)

    assertThrows[AnalysisException] {
      piiDataFrameWriter.save()
    }

    verify(metadataRepository, never()).save(any())
  }

  test("save should fail when usePii is true and source is not caspian") {
    val piiDataFrameWriter = new PIIDataFrameWriter[Row](
      createDataFrame(),
      metadataRepository,
      true,
      "controlEnv1",
      piiRootPath,
      now(UTC)
    ).format("parquet")
      .option("dataType", "IntermediateEngineering")
      .mode(Overwrite)

    assertThrows[AnalysisException] {
      piiDataFrameWriter.save(path)
    }

    verify(metadataRepository, never()).save(any())
  }

  test("save should fail when usePii is true and source is not in piiRootPath") {
    val piiDataFrameWriter = new PIIDataFrameWriter[Row](
      createDataFrame(),
      metadataRepository,
      true,
      "controlEnv1",
      "path1",
      now(UTC)
    ).format("caspian")
      .option("dataType", "IntermediateEngineering")
      .mode(Overwrite)

    assertThrows[AnalysisException] {
      piiDataFrameWriter.save(path)
    }

    verify(metadataRepository, never()).save(any())
  }

  test("save should fail when usePii is true and methodName in insertInto") {
    val piiDataFrameWriter = new PIIDataFrameWriter[Row](
      createDataFrame(),
      metadataRepository,
      true,
      "controlEnv1",
      piiRootPath,
      now(UTC)
    ).format("caspian")
      .option("dataType", "IntermediateEngineering")
      .option("path", path)
      .mode(Overwrite)

    assertThrows[AnalysisException] {
      piiDataFrameWriter.insertInto("table1")
    }

    verify(metadataRepository, never()).save(any())
  }

  test("save should fail when usePii is true and methodName in saveAsTable") {
    val piiDataFrameWriter = new PIIDataFrameWriter[Row](
      createDataFrame(),
      metadataRepository,
      true,
      "controlEnv1",
      piiRootPath,
      now(UTC)
    ).format("caspian")
      .option("dataType", "IntermediateEngineering")
      .option("path", path)
      .mode(Overwrite)

    assertThrows[AnalysisException] {
      piiDataFrameWriter.saveAsTable("table1")
    }

    verify(metadataRepository, never()).save(any())
  }

  test(
    "save should save metadata when usePii is true, metadata exists and mode is Overwrite"
  ) {
    val controlEnv = "controlEnv1"

    val qualifiedPath =
      makeQualified(new Path(path), spark.sessionState.newHadoopConf()).toString

    when(metadataRepository.findByPath(qualifiedPath)).thenReturn(
      Some(
        MetadataEntity(
          qualifiedPath,
          controlEnv,
          "format1",
          LongTail,
          Some(Map("tag1" -> "column1")),
          LocalDateTime.of(2020, 1, 1, 1, 0),
          Some(LocalDateTime.of(2020, 1, 1, 2, 0)),
          Some(List(LocalDateTime.of(2020, 1, 1, 3, 0))),
          ipRemoved = true,
          Some(LocalDateTime.of(2020, 1, 1, 4, 0)),
          Some(2L)
        )
      )
    )

    when(
      metadataRepository.save(
        MetadataEntity(
          qualifiedPath,
          controlEnv,
          "caspian",
          IntermediateEngineering,
          None,
          LocalDateTime.of(2020, 1, 1, 5, 0),
          None,
          None,
          ipRemoved = false,
          Some(LocalDateTime.of(2020, 1, 1, 4, 0)),
          Some(2L)
        )
      )
    ).thenReturn(
      MetadataEntity(
        qualifiedPath,
        controlEnv,
        "caspian",
        IntermediateEngineering,
        None,
        LocalDateTime.of(2020, 1, 1, 5, 0),
        None,
        None,
        ipRemoved = false,
        Some(LocalDateTime.of(2020, 1, 1, 4, 0)),
        Some(3L)
      )
    )

    val piiDataFrameWriter = new PIIDataFrameWriter[Row](
      createDataFrame(),
      metadataRepository,
      true,
      controlEnv,
      piiRootPath,
      LocalDateTime.of(2020, 1, 1, 5, 0)
    ).format("caspian")
      .option("dataType", "IntermediateEngineering")
      .mode(Overwrite)

    piiDataFrameWriter.save(path)

    verify(metadataRepository).save(
      MetadataEntity(
        qualifiedPath,
        controlEnv,
        "caspian",
        IntermediateEngineering,
        None,
        LocalDateTime.of(2020, 1, 1, 5, 0),
        None,
        None,
        ipRemoved = false,
        Some(LocalDateTime.of(2020, 1, 1, 4, 0)),
        Some(2L)
      )
    )
  }

  test(
    "save should save metadata when usePii is true, metadata exists and mode is Append"
  ) {
    val controlEnv = "controlEnv1"

    val qualifiedPath =
      makeQualified(new Path(path), spark.sessionState.newHadoopConf()).toString

    when(metadataRepository.findByPath(qualifiedPath)).thenReturn(
      Some(
        MetadataEntity(
          qualifiedPath,
          controlEnv,
          "format1",
          LongTail,
          Some(Map("tag1" -> "column1")),
          LocalDateTime.of(2020, 1, 1, 1, 0),
          Some(LocalDateTime.of(2020, 1, 1, 2, 0)),
          Some(List(LocalDateTime.of(2020, 1, 1, 3, 0))),
          ipRemoved = true,
          Some(LocalDateTime.of(2020, 1, 1, 4, 0)),
          Some(2L)
        )
      )
    )

    when(
      metadataRepository.save(
        MetadataEntity(
          qualifiedPath,
          controlEnv,
          "caspian",
          IntermediateEngineering,
          None,
          LocalDateTime.of(2020, 1, 1, 1, 0),
          Some(LocalDateTime.of(2020, 1, 1, 2, 0)),
          Some(List(LocalDateTime.of(2020, 1, 1, 3, 0))),
          ipRemoved = false,
          Some(LocalDateTime.of(2020, 1, 1, 4, 0)),
          Some(2L)
        )
      )
    ).thenReturn(
      MetadataEntity(
        qualifiedPath,
        controlEnv,
        "caspian",
        IntermediateEngineering,
        None,
        LocalDateTime.of(2020, 1, 1, 1, 0),
        Some(LocalDateTime.of(2020, 1, 1, 2, 0)),
        Some(List(LocalDateTime.of(2020, 1, 1, 3, 0))),
        ipRemoved = false,
        Some(LocalDateTime.of(2020, 1, 1, 4, 0)),
        Some(3L)
      )
    )

    val piiDataFrameWriter = new PIIDataFrameWriter[Row](
      createDataFrame(),
      metadataRepository,
      true,
      controlEnv,
      piiRootPath,
      LocalDateTime.of(2020, 1, 1, 5, 0)
    ).format("caspian")
      .option("dataType", "IntermediateEngineering")
      .mode(Append)

    piiDataFrameWriter.save(path)

    verify(metadataRepository).save(
      MetadataEntity(
        qualifiedPath,
        controlEnv,
        "caspian",
        IntermediateEngineering,
        None,
        LocalDateTime.of(2020, 1, 1, 1, 0),
        Some(LocalDateTime.of(2020, 1, 1, 2, 0)),
        Some(List(LocalDateTime.of(2020, 1, 1, 3, 0))),
        ipRemoved = false,
        Some(LocalDateTime.of(2020, 1, 1, 4, 0)),
        Some(2L)
      )
    )
  }

  test("save should save metadata when usePii is true, metadata does not exist") {
    val controlEnv = "controlEnv1"

    val qualifiedPath =
      makeQualified(new Path(path), spark.sessionState.newHadoopConf()).toString

    when(metadataRepository.findByPath(qualifiedPath)).thenReturn(None)

    when(
      metadataRepository.save(
        MetadataEntity(
          qualifiedPath,
          controlEnv,
          "caspian",
          IntermediateEngineering,
          None,
          LocalDateTime.of(2020, 1, 1, 1, 0),
          None,
          None,
          ipRemoved = false,
          None,
          None
        )
      )
    ).thenReturn(
      MetadataEntity(
        qualifiedPath,
        controlEnv,
        "caspian",
        IntermediateEngineering,
        None,
        LocalDateTime.of(2020, 1, 1, 1, 0),
        None,
        None,
        ipRemoved = false,
        None,
        Some(1L)
      )
    )

    val piiDataFrameWriter = new PIIDataFrameWriter[Row](
      createDataFrame(),
      metadataRepository,
      true,
      controlEnv,
      piiRootPath,
      LocalDateTime.of(2020, 1, 1, 1, 0)
    ).format("caspian")
      .option("dataType", "IntermediateEngineering")
      .mode(Append)

    piiDataFrameWriter.save(path)

    verify(metadataRepository).save(
      MetadataEntity(
        qualifiedPath,
        controlEnv,
        "caspian",
        IntermediateEngineering,
        None,
        LocalDateTime.of(2020, 1, 1, 1, 0),
        None,
        None,
        ipRemoved = false,
        None,
        None
      )
    )
  }

  private def createDataFrame() =
    spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row("a", 1), Row("b", 2))),
      StructType(
        Seq(StructField("col1", StringType), StructField("col2", IntegerType))
      )
    )

  private def assertDataFrame(dataFrame: DataFrame): Unit = {
    checkDatasetUnorderly(dataFrame.as[(String, Int)], ("a", 1), ("b", 2))
  }
}
