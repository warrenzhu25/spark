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

import org.scalatest.BeforeAndAfter

import org.apache.spark.sql._
import org.apache.spark.sql.SaveMode.Overwrite
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{
  IntegerType,
  StringType,
  StructField,
  StructType
}
import org.apache.spark.util.Utils.{createTempDir, deleteRecursively}

class PIIDataFrameReaderSuite
    extends QueryTest
    with SharedSparkSession
    with BeforeAndAfter {
  import testImplicits._

  private var piiRootPath: String = _
  private var path: String = _

  before {
    piiRootPath = createTempDir().getCanonicalPath
    path = new File(piiRootPath, "data").getCanonicalPath
  }

  after {
    try {
      deleteRecursively(new File(piiRootPath))
    } finally {
      piiRootPath = null
      path = null
    }
  }

  test("load should fail when usePii is false and path is in piiRootPath") {
    new DataFrameWriter[Row](createDataFrame())
      .format("caspian")
      .mode(Overwrite)
      .save(path)

    val piiDataFrameReader =
      new PIIDataFrameReader(spark, false, piiRootPath).format("caspian")

    assertThrows[AnalysisException] {
      piiDataFrameReader.load(path)
    }
  }

  test("load should succeed when usePii is true") {
    new DataFrameWriter[Row](createDataFrame())
      .format("caspian")
      .mode(Overwrite)
      .save(path)

    val piiDataFrameReader =
      new PIIDataFrameReader(spark, true, piiRootPath).format("caspian")

    val dataFrame = piiDataFrameReader.load(path)

    assertDataFrame(dataFrame)
  }

  test(
    "load should succeed when usePii is false and path is not in piiRootPath"
  ) {
    new DataFrameWriter[Row](createDataFrame())
      .format("caspian")
      .mode(Overwrite)
      .save(path)

    val piiDataFrameReader =
      new PIIDataFrameReader(spark, false, "path1").format("caspian")

    val dataFrame = piiDataFrameReader.load(path)

    assertDataFrame(dataFrame)
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
