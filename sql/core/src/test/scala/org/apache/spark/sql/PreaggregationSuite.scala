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

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.optimizer.ms.preaggregation.PushdownLocalAggregate.{TESTING_PUSHDOWN_LEFT_CONF, TESTING_PUSHDOWN_RIGHT_CONF}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.{Join, LocalAggregate, LogicalPlan, Project}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.{SharedSQLContext, SQLTestUtils}
import org.apache.spark.sql.types.{DataTypes, Decimal, StringType}

class PreaggregationSuite extends QueryTest with SharedSQLContext {
  val tbl1 = "src_table_1"
  val tbl2 = "src_table_2"
  val tblWithNoStats1 = "src_table_with_nostats_1"
  val tblWithNoStats2 = "src_table_with_nostats_2"
  val tblWithTypes1 = "src_table_types_1"
  val tblWithTypes2 = "src_table_types_2"

  override def beforeAll() {
    super.beforeAll()
    case class Data(col1: Int, col2: Int, col3: Int)
    import org.apache.spark.sql.functions.col
    import testImplicits._

    Seq(tbl1, tbl2, tblWithNoStats1, tblWithNoStats2).foreach { tbl =>
      sparkContext.parallelize(1 to 1000).toDF("col1")
        .withColumn("col2", col("col1") % 100)
        .withColumn("col3", col("col1") % 10)
        .write.format("parquet").saveAsTable(tbl)
    }

    case class DataWithTypes(col1: String, col2: Float, col3: Decimal, col4: Int, col5: Double)
    val idToStringUDF = spark.udf.register("idToString", (value: Int) => s"id-$value")
    Seq(tblWithTypes1, tblWithTypes2).foreach { tbl =>
      sparkContext.parallelize(1 to 1000).toDF("col4")
        .withColumn("col1", idToStringUDF(col("col4")).cast(StringType))
        .withColumn("col2", col("col4").cast(DataTypes.FloatType) / 1000)
        .withColumn("col3", col("col4").cast(DataTypes.createDecimalType()) / 1000000)
        .withColumn("col5", col("col4").cast(DataTypes.DoubleType) / 100)
        .write.format("parquet").saveAsTable(tbl)
    }

    withSQLConf(SQLConf.CBO_ENABLED.key -> "true") {
      Seq(tbl1, tbl2).foreach { tbl =>
        sql(s"ANALYZE TABLE $tbl COMPUTE STATISTICS FOR COLUMNS col1, col2, col3")
      }
      Seq(tblWithTypes1, tblWithTypes2).foreach { tbl =>
        sql(s"ANALYZE TABLE $tbl COMPUTE STATISTICS FOR COLUMNS col1, col2, col3, col4, col5")
      }
    }
  }

  override def afterAll() {
    Seq(tbl1, tbl2, tblWithTypes1, tblWithTypes2, tblWithNoStats1, tblWithNoStats2)
      .foreach { tbl =>
        sql(s"DROP TABLE IF EXISTS $tbl")
      }
    super.afterAll()
  }

  /**
   * ----------------------------------------------------------------------------------------
   * For following tests, preaggregation optimization won't trigger due to validation checks.
   * ----------------------------------------------------------------------------------------
   */
  test("test non deterministic expression for aggregate expression") {
    val query =
      s"""
         | SELECT sum($tbl1.col1), $tbl2.col2*rand() from $tbl1 join $tbl2
         | on $tbl1.col2=$tbl2.col2 group by $tbl2.col2
         |""".stripMargin
    assertPreagg(query, checkLaForLeftJoin = false, checkLaForRightJoin = false,
      checkAnswer = false)
  }

  test("optimization shouldn't trigger with distinct") {
    val query =
      s"""
         | SELECT count(distinct $tbl1.col1), $tbl2.col2 from $tbl1
         | join $tbl2 on $tbl1.col1=$tbl2.col1 group by $tbl2.col2
         |""".stripMargin
    assertPreagg(query, checkLaForLeftJoin = false, checkLaForRightJoin = false)
  }

  test("test non deterministic expression in aggregate function") {
    intercept[AnalysisException] {
      assertPreagg("SELECT sum($tbl1.col1 * rand()), $tbl2.col2 from $tbl1 join $tbl2" +
        s" on $tbl1.col2=$tbl2.col2 group by $tbl2.col2", checkLaForLeftJoin = false,
        checkLaForRightJoin = false)
    }

    val query =
      s"""
         | SELECT sum($tbl1.col1) * rand(), $tbl2.col2 from $tbl1 join $tbl2
         | on $tbl1.col2=$tbl2.col2 group by $tbl2.col2
         |""".stripMargin
    assertPreagg(query, checkLaForLeftJoin = false, checkLaForRightJoin = false,
      checkAnswer = false)
  }

  test("preaggregate with complicated grouping keys") {
    spark.udf.register("concat",
      (value1: String, value2: String) => s"$value1-$value2")
    val query =
      s"""
         | SELECT sum($tblWithTypes1.col4), concat($tblWithTypes1.col1, $tblWithTypes2.col1)
         | from $tblWithTypes1 join $tblWithTypes2 on $tblWithTypes1.col2=$tblWithTypes2.col2
         | group by
         | $tblWithTypes2.col2/2, concat($tblWithTypes1.col1, $tblWithTypes2.col1)
         |""".stripMargin
    // Grouping expression has multiple Attributes
    assertPreagg(query, checkLaForLeftJoin = false, checkLaForRightJoin = false)
  }

  test("preaggregate with complicated aggregate expressions") {
    val query =
      s"""
         | SELECT sum($tbl1.col1 + $tbl2.col2), max($tbl1.col1*2) from $tbl1 join $tbl2
         | on $tbl1.col2=$tbl2.col2 group by $tbl2.col3
         |""".stripMargin
    // Aggregate expression belongs to both side
    assertPreagg(query, checkLaForLeftJoin = false, checkLaForRightJoin = false)
  }

  Seq(LeftOuter, RightOuter, FullOuter, Cross, LeftSemi, LeftAnti).foreach { join =>
    test(s"non inner joins shouldn't trigger optimization (join = $join)") {
      val query = if (join != LeftSemi && join != LeftAnti) {
        s"""
           | SELECT sum($tbl1.col1), $tbl2.col2 from $tbl1 ${join.sql}
           | join $tbl2 on $tbl1.col2=$tbl2.col2 group by $tbl2.col2
           |""".stripMargin
      } else {
        s"""
           | select sum(col1) from $tbl1 ${join.sql} join $tbl2 on $tbl1.col1 = $tbl2.col2
           | group by col2
           |""".stripMargin
      }
      assertPreagg(query, checkLaForLeftJoin = false, checkLaForRightJoin = false)
    }
  }

  test("local aggregate shouldn't push below filter") {
    val query =
      s"""
         | SELECT sum($tbl1.col1), $tbl2.col2 from $tbl1 join $tbl2
         | on $tbl1.col2=$tbl2.col2 where $tbl1.col1 + 1 < $tbl2.col1
         | group by $tbl2.col2
         |""".stripMargin

    assertPreagg(query, checkLaForLeftJoin = false, checkLaForRightJoin = false)
  }

  /**
   * -------------------------------------------------------------------------------------------
   * For following tests, preaggregation optimization won't trigger due to stats check failures.
   * -------------------------------------------------------------------------------------------
   */
  test("check the LA node if one of the side is broadcastable") {
    val conf = Map(
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "10000",
      SQLConf.PREAGGREGATION_CBO_ENABLED.key -> "true")
    val query =
      s"""
         | SELECT sum($tbl1.col1), $tbl2.col2 from $tbl1 join $tbl2
         | on $tbl1.col2=$tbl2.col2 group by $tbl2.col2
         |""".stripMargin
    assertPreagg(query, checkLaForLeftJoin = false, checkLaForRightJoin = false,
      additionalConfs = conf)
  }

  test("check LA node is not present if one of the side is broadcastable using hints") {
    val query =
      s"""
         | SELECT /*+ BROADCAST ($tbl1) */ sum($tbl1.col1), $tbl2.col2 from $tbl1 join $tbl2
         | on $tbl1.col2=$tbl2.col2 group by $tbl2.col2
         |""".stripMargin
    assertPreagg(query, checkLaForLeftJoin = false, checkLaForRightJoin = false)
  }

  Seq(1, 100).foreach { ratioThreshold =>
    test(s"check the LA node if ratio threshold doesn't meet (ratioThreshold=$ratioThreshold)") {
      val conf = Map(
        SQLConf.PREAGGREGATION_CBO_ENABLED.key -> "true",
        SQLConf.PREAGGREGATION_CBO_SHUFFLE_JOIN_PUSHDOWN_THRESHOLD.key -> ratioThreshold.toString)
      val query =
        s"""
           | SELECT sum($tbl1.col1), $tbl2.col2 from $tbl1 join $tbl2
           | on $tbl1.col2=$tbl2.col2 group by $tbl2.col2
           |""".stripMargin
      if (ratioThreshold == 100) {
        assertPreagg(query, checkLaForLeftJoin = false, checkLaForRightJoin = false,
          additionalConfs = conf)
      } else {
        assertPreagg(query, additionalConfs = conf)
      }
    }
  }

  Seq(true, false).foreach { cboEnabled =>
    Seq(true, false).foreach { tableStatsExists =>
      test(s"test with stats = $tableStatsExists and cboEnabled = $cboEnabled") {
        val conf = Map(
          SQLConf.CBO_ENABLED.key -> cboEnabled.toString,
          SQLConf.PREAGGREGATION_CBO_ENABLED.key -> "true",
          SQLConf.PREAGGREGATION_CBO_SHUFFLE_JOIN_PUSHDOWN_THRESHOLD.key -> "1")
        val (tab1, tab2) = if (tableStatsExists) {
          (tbl1, tbl2)
        } else {
          (tblWithNoStats1, tblWithNoStats2)
        }
        val query =
          s"""
             | SELECT sum($tab1.col1), $tab2.col2 from $tab1 join $tab2 on
             | $tab1.col2=$tab2.col2 group by $tab2.col2
             |""".stripMargin
        if (cboEnabled && tableStatsExists) {
          assertPreagg(query, additionalConfs = conf)
        } else {
          assertPreagg(query, checkLaForLeftJoin = false, checkLaForRightJoin = false,
            additionalConfs = conf)
        }
      }
    }
  }

  /**
   * ------------------------------------------------------------------------------
   * Following tests are actually triggering the preaggregation optimization.
   * ------------------------------------------------------------------------------
   */
  Seq(true, false).foreach { preAggEnabled =>
    test(s"check for local aggregate node when preAggregation enabled = $preAggEnabled") {
      val conf = Map(SQLConf.PREAGGREGATION_ENABLED.key -> preAggEnabled.toString)
      val query =
        s"""
           | SELECT sum($tbl1.col1), $tbl2.col2 from $tbl1 join $tbl2
           | on $tbl1.col2=$tbl2.col2 group by $tbl2.col2
           |""".stripMargin
      if (preAggEnabled) {
        val optimizedPlan = assertPreagg(query)
        // tbl1 will have LA with group by only on Join key
        // tbl2 will have LA with group by on both Join key (tbl2.col2) and
        //     Aggregates grouping key (tbl2.col2).
        //     Since both are same - so we should have only 1 grouping key in tbl2's LA
        getAllLaNodesFromPlan(optimizedPlan).foreach { la =>
          assert(la.groupingExpressions.size == 1)
        }
      } else {
        assertPreagg(query, checkLaForLeftJoin = false, checkLaForRightJoin = false,
          additionalConfs = conf)
      }
    }
  }

  Seq("count", "sum", "max", "min").foreach { aggregateFunction =>
    test(s"preaggregation should triggered with $aggregateFunction") {
      val query =
        s"""
           | SELECT $aggregateFunction($tbl1.col1), $tbl2.col2
           | from $tbl1 join $tbl2 on $tbl1.col2=$tbl2.col2 group by $tbl2.col2
           |""".stripMargin
      assertPreagg(query)
    }
  }

  Seq("decimal", "float", "double", "integer").foreach { aggregationType =>
    Seq("count", "sum", "max", "min").foreach { aggregateFunction =>
      test(s"preaggregation should triggered with $aggregateFunction and type $aggregationType") {

        val colId = aggregationType match {
          case dataType if dataType == "decimal" => "col3"
          case dataType if dataType == "float" => "col2"
          case dataType if dataType == "double" => "col5"
          case _ => "col4"
        }

        val query =
          s"""
             | SELECT $aggregateFunction($tblWithTypes1.$colId), $tblWithTypes2.col2
             | from $tblWithTypes1 join $tblWithTypes2
             | on $tblWithTypes1.col2=$tblWithTypes2.col2
             | group by $tblWithTypes2.col2
             |""".stripMargin
        assertPreagg(query)
      }
    }
  }

  test("preaggregate with mutiple grouping keys") {
    val query =
      s"""
         | SELECT sum($tbl1.col1), $tbl2.col2 from $tbl1 join $tbl2
         | on $tbl1.col2=$tbl2.col2 group by $tbl2.col2, 2*$tbl1.col3
         |""".stripMargin
    assertPreagg(query)
  }

  test("preaggregate with multiple aggregate expressions") {
    val query =
      s"""
         | SELECT sum($tbl1.col1), min($tbl1.col1), count($tbl2.col2) from $tbl1 join $tbl2
         | on $tbl1.col2=$tbl2.col2 group by $tbl2.col3
         |""".stripMargin
    assertPreagg(query)
  }

  test("preaggregate with splittable complicated aggregate expressions") {
    val query =
      s"""
         | SELECT sum($tbl1.col1 + $tbl1.col3),  max($tbl2.col1*2) from $tbl1 join $tbl2
         | on $tbl1.col2=$tbl2.col2 group by $tbl2.col3
         |""".stripMargin
    assertPreagg(query)
  }

  test("preaggregate with complicated grouping expressions with single attribute reference") {
    val query =
      s"""
         | SELECT sum($tbl1.col1),  max($tbl2.col1*2) from $tbl1 join $tbl2
         | on $tbl1.col2=$tbl2.col2 group by $tbl2.col3 / 2 + 2 * $tbl2.col3, $tbl1.col2
         |""".stripMargin
    assertPreagg(query)
  }

  test("preaggregate with complicated join conditions") {
    val query =
      s"""
         | SELECT sum($tbl1.col1), min($tbl1.col1), count($tbl2.col2) from $tbl1 join $tbl2
         | on $tbl1.col2 + 2 = 2 * $tbl2.col2 group by $tbl2.col3
         |""".stripMargin
    assertPreagg(query)
  }

  test("aliasing in aggregate") {
    val query =
      s"""
         | SELECT sum(col3), col_sum1 from (SELECT sum($tbl1.col1) as col_sum1,
         | $tbl2.col3 as col3 from $tbl1 join $tbl2 on $tbl1.col2 + 2 = 2 * $tbl2.col2
         | group by $tbl2.col3) group by col_sum1
         |""".stripMargin
    assertPreagg(query)
  }

  test("aggregate expression is coming from both sides of join") {
    val query =
      s"""
         | SELECT sum($tbl1.col1), max($tbl2.col1), $tbl2.col3 from $tbl1 join $tbl2
         | on $tbl1.col2 = $tbl2.col2 group by $tbl2.col3
         |""".stripMargin
    assertPreagg(query)
  }

  test("grouping expression is coming from both sides of join") {
    val query =
      s"""
         | SELECT sum($tbl1.col1), $tbl2.col3 from $tbl1 join $tbl2
         | on $tbl1.col2 = $tbl2.col2 group by $tbl2.col3, $tbl1.col3
         |""".stripMargin
    assertPreagg(query)
  }

  test("aliases between join and aggregate") {
    val query =
      s"""
         | SELECT sum(alias1.col1) as agg1, alias2.col3 as agg2
         | from $tbl1 as alias1 join $tbl2 as alias2
         | on alias1.col2 = alias2.col2 group by alias2.col3, alias1.col3
         |""".stripMargin
    assertPreagg(query)
  }

  test("multiple level local aggregate push down below join and retain at all levels") {
    val query =
      s"""
         | SELECT max($tblWithTypes2.col2), $tbl2.col2  from ($tbl1 join $tbl2 on
         | $tbl1.col2=$tbl2.col2) join ($tblWithTypes1 join $tblWithTypes2 on
         | $tblWithTypes1.col2=$tblWithTypes2.col2) on $tblWithTypes1.col4=$tbl1.col1
         | group by $tblWithTypes2.col1, $tbl2.col2
         |""".stripMargin
    assertPreagg(query)
  }

  test("with alias multiple level local aggregate push down below join and retain at all levels") {
    val query =
      s"""
         | SELECT max(tab_type1.col3)*2 as agg1, sum(tab1.col3) as agg2 from
         | ($tbl1 as tab1 join $tbl2 as tab2 on tab1.col1=tab2.col1) join
         | ($tblWithTypes1 as tab_type1 join $tblWithTypes2 as tab_type2 on
         | tab_type1.col4=tab_type2.col4) on
         | tab_type1.col4=tab1.col1 group by tab_type2.col2, tab2.col2
         |""".stripMargin
    assertPreagg(query)
  }

  /**
   * Following tests will trigger preaggregation optimization and push through one side of
   * join
   */
  test("preaggregate with multiple aggregate expressions with one side pushdown") {
    val conf = Map(
      SQLConf.PREAGGREGATION_CBO_ENABLED.key -> "true",
      SQLConf.PREAGGREGATION_CBO_SHUFFLE_JOIN_PUSHDOWN_THRESHOLD.key -> "1")
    val query =
      s"""
         |  SELECT sum($tbl1.col1), min($tbl1.col1), count($tbl2.col2) from $tbl1 join $tbl2
         |  on $tbl1.col2=$tbl2.col2 group by $tbl2.col3
         |""".stripMargin
    // leftratio: 5 and rightRatio: 0. So LA node will push down to only left side
    assertPreagg(query, conf, checkLaForRightJoin = false)
  }

  test("preaggregate with mutiple grouping keys with one side pushdown") {
    val conf = Map(
      SQLConf.PREAGGREGATION_CBO_ENABLED.key -> "true",
      SQLConf.PREAGGREGATION_CBO_SHUFFLE_JOIN_PUSHDOWN_THRESHOLD.key -> "5")
    val query =
      s"""
         |  SELECT sum($tbl1.col1), $tbl2.col2 from $tbl1 join $tbl2
         |  on $tbl1.col2=$tbl2.col2 group by $tbl2.col2, 2*$tbl1.col3
         |""".stripMargin
    // leftratio: 0 and rightRatio: 6. So LA node will pushdown to only right side
    assertPreagg(query, conf, checkLaForLeftJoin = false)
  }

  test("preaggregate with splittable complicated aggregate expressions with one side pushdown") {
    val conf = Map(
      SQLConf.PREAGGREGATION_CBO_ENABLED.key -> "true",
      SQLConf.PREAGGREGATION_CBO_SHUFFLE_JOIN_PUSHDOWN_THRESHOLD.key -> "1")
    val query =
      s"""
         | SELECT sum($tbl1.col1 + $tbl1.col3),  max($tbl2.col1*2) from $tbl1 join $tbl2
         | on $tbl1.col2=$tbl2.col2 group by $tbl2.col3
         |""".stripMargin

    assertPreagg(query, conf, checkLaForRightJoin = false)
  }

  Seq("min", "count", "sum").foreach{ agExp =>
    test(s"agg $agExp with one side pushdown when it belongs to non-pushdown side") {
      val conf = Map(
        SQLConf.PREAGGREGATION_CBO_ENABLED.key -> "true",
        SQLConf.PREAGGREGATION_CBO_SHUFFLE_JOIN_PUSHDOWN_THRESHOLD.key -> "5")
      val query =
        s"""
           |  SELECT $agExp($tbl1.col1), $tbl2.col2 from $tbl1 join $tbl2
           |  on $tbl1.col2=$tbl2.col2 group by $tbl2.col2, 2*$tbl1.col3
           |""".stripMargin
      // leftratio < ratioThreshold . So LA node will pushdown to only right side.
      // left side has aggregate expression.
      assertPreagg(query, conf, checkLaForLeftJoin = false)
    }

    test(s"agg $agExp with one side pushdown when it belongs to pushdown side") {
      val conf = Map(
        SQLConf.PREAGGREGATION_CBO_ENABLED.key -> "true",
        SQLConf.PREAGGREGATION_CBO_SHUFFLE_JOIN_PUSHDOWN_THRESHOLD.key -> "5")
      val query =
        s"""
           |  SELECT $agExp($tbl1.col1), 2*$tbl2.col3 from $tbl1 join $tbl2
           |  on $tbl1.col2=$tbl2.col2 group by 2*$tbl2.col3, $tbl1.col2
           |""".stripMargin
      // rightRatio < ratioThreshold . So LA node will pushdown to only left side
      // left side has aggregate expression.
      assertPreagg(query, conf, checkLaForRightJoin = false)
    }
  }
  
  test("retain LA should handle attributes for one side pushdown") {
    val conf = Map(
      SQLConf.PREAGGREGATION_CBO_ENABLED.key -> "true",
      SQLConf.PREAGGREGATION_CBO_SHUFFLE_JOIN_PUSHDOWN_THRESHOLD.key -> "1")

    // Following query will have only right side push down for both the joins,
    // as the stats for right LA = 1 and stats for left LA = 0.
    val query =
      s"""
         | select min($tblWithTypes1.col2), 2*$tbl1.col1, 3*$tbl2.col2, 4*$tblWithTypes1.col4
         | from $tblWithTypes1 join
         | ($tbl1 join $tbl2 on $tbl1.col1 = $tbl2.col3)
         | on $tblWithTypes1.col4 = $tbl2.col1
         | group by 2*$tbl1.col1, 3*$tbl2.col2, 4*$tblWithTypes1.col4
         |
         |""".stripMargin
    val optimizedPlan = assertPreagg(query, additionalConfs = conf, checkLaForLeftJoin = false)
    assert(isProjectRetainedBetweenLAAndJoin(optimizedPlan))
  }

  /**
   * Following tests are for cases when empty input comes to Aggregate node
   */
  Seq("min", "count", "sum").foreach { agExp =>
    test(s"agg $agExp with one side pushdown when no incoming data") {
      val query = s"""
                     |  SELECT $agExp($tbl1.col1), $agExp($tbl2.col2)
                     |  from $tbl1 join $tbl2 on $tbl1.col2=$tbl2.col2
                     |  where $tbl1.col1 = -1 and $tbl2.col1 = -1
                     |""".stripMargin
      val conf1 = Map(
        TESTING_PUSHDOWN_LEFT_CONF -> "true",
        TESTING_PUSHDOWN_RIGHT_CONF -> "false")
      assertPreagg(query, conf1, checkLaForRightJoin = false)

      val conf2 = Map(
        TESTING_PUSHDOWN_LEFT_CONF -> "false",
        TESTING_PUSHDOWN_RIGHT_CONF -> "true")
      assertPreagg(query, conf2, checkLaForLeftJoin = false)
    }

    test(s"agg $agExp with both side pushdown when no incoming data") {
      val query = s"""
                     |  SELECT $agExp($tbl1.col1), $agExp($tbl2.col2)
                     |  from $tbl1 join $tbl2 on $tbl1.col2=$tbl2.col2
                     |  where $tbl1.col1 = -1 and $tbl2.col1 = -1
                     |""".stripMargin

      assertPreagg(query)
    }

    test(s"agg $agExp with both side pushdown when no incoming data " +
      s"for specific key") {
      val query = s"""
                     |  SELECT $agExp($tbl1.col1), $agExp($tbl2.col2)
                     |  from $tbl1 join $tbl2 on $tbl1.col3=$tbl2.col3
                     |  where $tbl2.col1 % 10 != 1
                     |  group by $tbl1.col3""".stripMargin

      // $tbl2.col1 % 10 != 1 is same as $tbl1.col3 != 1
      // So the pushdown LA on left side will get data of tbl1.col3 = 1
      // But the join will eliminate such rows and those rows won't be given as input to Aggregate
      assertPreagg(query)
    }
  }

  test("local aggregate optimization in single subquery expressions should not apply twice") {
    val query =
      s"""
         | SELECT * from $tbl1 where $tbl1.col3 > (select min($tbl1.col3) from $tbl1, $tbl2
         | where $tbl1.col1 = $tbl2.col1)
         |""".stripMargin
    assertPreagg(query)
  }

  test("local aggregate optimization in multiple subquery expressions should not apply twice") {
    val query =
      s"""
         | SELECT * from $tbl1 where $tbl1.col3 > (select min($tbl1.col3) from $tbl1, $tbl2
         | where $tbl1.col1 = $tbl2.col1 and $tbl1.col2 > (select min($tblWithTypes1.col4)
         | from $tblWithTypes1, $tblWithTypes2 where $tblWithTypes1.col4=$tblWithTypes2.col4)
         | )
         |""".stripMargin
    assertPreagg(query)
  }

  test("project is added below la when la is retained") {
    val query =
      s"""
         | SELECT sum($tblWithTypes1.col3), $tbl1.col3, $tbl2.col1 from $tbl1 join $tbl2
         | on $tbl1.col1=$tbl2.col1 join $tblWithTypes1 on $tbl1.col2 = $tblWithTypes1.col4
         | group by $tbl2.col1, $tbl1.col3
         |""".stripMargin
    val optimizedPlan = assertPreagg(query)
    assert(isProjectRetainedBetweenLAAndJoin(optimizedPlan))
  }

  test(s"check for pushdown below project node which has aliased output") {
    val query =
      s"""
         | SELECT apple, sum(ball) from
         | (select $tbl1.col1 as ball, $tbl2.col2 as apple from $tbl1 join $tbl2
         | on $tbl1.col2=$tbl2.col2) group by apple
         | """.stripMargin
    assertPreagg(query)
  }

  test(s"check for pushdown below multiple aggregate nodes") {
    val query =
      s"""
         |SELECT max(bat) from (select sum(ball) as bat, apple from (
         | select $tbl1.col1 as ball, $tbl2.col2 as apple from $tbl1 join $tbl2
         | on $tbl1.col2=$tbl2.col2) group by apple)
         | """.stripMargin
    assertPreagg(query)
  }

  test(s"check for pushdown below project node with complex grouping expression" +
    s" with grouping key aliased") {
    val query =
      s"""
         | SELECT 2*apple, max(subresult.col1) from (select $tbl1.col1,
         | 2*$tbl2.col2 as apple from $tbl1 join $tbl2
         | on $tbl1.col2=$tbl2.col2)subresult group by 2*apple
         |""".stripMargin
    assertPreagg(query)
  }

  test(s"check for pushdown below project node when only aggregate key is in output") {
    val query =
      s"""
         | SELECT max(ball) from (select $tbl1.col1 as ball,
         | $tbl2.col2 as apple from $tbl1 join $tbl2
         | on $tbl1.col2=$tbl2.col2) group by apple
         |""".stripMargin
    assertPreagg(query)
  }

  test(s"check for pushdown below project node when count has multiple params") {
    val query =
      s"""
         | SELECT count(subresult.col3, ball) from (select $tbl1.col1 as ball, $tbl1.col3,
         | $tbl2.col2 as apple from $tbl1 join $tbl2
         | on $tbl1.col2=$tbl2.col2)subresult group by apple
         |""".stripMargin
    assertPreagg(query)
  }

  test(s"check for pushdown below project with aliased output " +
    s"and aggregate is done on multiple columns") {
    val query =
      s"""
         | SELECT max(ball+bat) from (select $tbl1.col1 as ball, $tbl1.col3 as bat,
         | $tbl2.col2 as apple from $tbl1 join $tbl2
         | on $tbl1.col2=$tbl2.col2) group by apple
         | """.stripMargin
    assertPreagg(query)
  }

  test(s"check for pushdown below project with aliased and casted output") {
    val query =
      s"""
         | SELECT subresult.col2, max(ball) from (select cast($tbl1.col1 + $tbl1.col3
         | as double) ball, $tbl2.col2 from $tbl1 join $tbl2
         | on $tbl1.col2=$tbl2.col2)subresult group by subresult.col2
         |""".stripMargin
    assertPreagg(query)
  }

  test(s"check for pushdown below project with casted aliased output " +
    s"and aggregate is done on multiple columns") {
    val query =
      s"""
         | SELECT max(ball+cast(bat as double)) from (select
         | cast($tbl1.col1 as double) ball, cast($tbl1.col3 as float) bat,
         | $tbl2.col2 as apple from $tbl1 join $tbl2
         | on $tbl1.col2=$tbl2.col2) group by apple
         |""".stripMargin
    assertPreagg(query)
  }

  private def assertPreagg(
      query: String,
      additionalConfs: Map[String, String] = Map.empty,
      checkLaForLeftJoin: Boolean = true,
      checkLaForRightJoin: Boolean = true,
      checkAnswer: Boolean = true): LogicalPlan = {
    val confs = Map(
      SQLConf.CBO_ENABLED.key -> "true",
      SQLConf.PREAGGREGATION_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.PREAGGREGATION_CBO_ENABLED.key -> "false") ++ additionalConfs

    var optimizePlan: LogicalPlan = null
    withSQLConf(confs.toSeq: _*) {
      optimizePlan = sql(query).queryExecution.optimizedPlan

      if (checkLaForLeftJoin && checkLaForRightJoin) {
        assert(validateLocalAggregate(optimizePlan))
        assert(isPushThroughJoin(optimizePlan))
      } else if (checkLaForLeftJoin) {
        assert(!isPushThroughJoin(optimizePlan))
        assert(isPushThroughJoinOneSide(optimizePlan))
        assert(isPushThroughJoinLeftSide(optimizePlan))
      } else if (checkLaForRightJoin) {
        assert(!isPushThroughJoin(optimizePlan))
        assert(isPushThroughJoinOneSide(optimizePlan))
        assert(isPushThroughJoinRightSide(optimizePlan))
      } else {
        assert(!validateLocalAggregate(optimizePlan))
      }

      if (checkAnswer) {
        val actualResult = sql(query).queryExecution.executedPlan
        val expectedResult = executeQueryWithoutPreaggregation(query)
        compareAnswers(actualResult, expectedResult)
      }
    }
    optimizePlan
  }

  private def getAllLaNodesFromPlan(plan: LogicalPlan) = {
   plan.collectInPlanAndSubqueries{
      case la@LocalAggregate(_, _, _) => la
    }
  }

  private def validateLocalAggregate(optimizedPlan: LogicalPlan): Boolean = {
    val allLaNodes = getAllLaNodesFromPlan(optimizedPlan)
    allLaNodes.nonEmpty && !allLaNodes.exists(la => la.child.isInstanceOf[LocalAggregate])
  }

  private def isPushThroughJoin(optimizedPlan: LogicalPlan): Boolean = {
    val joins = optimizedPlan.collect {
      case j@Join(_, _, _, _) => j
    }
    joins.forall(join => isChildNodeLocalAggregate(join))
  }

  private def isChildNodeLocalAggregate(plan: LogicalPlan): Boolean = {
    plan.children.forall(_.isInstanceOf[LocalAggregate])
  }

  private def isPushThroughJoinOneSide(optimizedPlan: LogicalPlan): Boolean = {
    (isPushThroughJoinLeftSide(optimizedPlan)
      && !isPushThroughJoinRightSide(optimizedPlan)) || (!isPushThroughJoinLeftSide(optimizedPlan)
      && isPushThroughJoinRightSide(optimizedPlan))
  }

  private def isPushThroughJoinLeftSide(optimizedPlan: LogicalPlan): Boolean = {
    val joins = optimizedPlan.collect {
      case j@Join(_, _, _, _) => j
    }
    joins.exists(join => (join.left.isInstanceOf[LocalAggregate]
      && !join.right.isInstanceOf[LocalAggregate]))
  }

  private def isPushThroughJoinRightSide(optimizedPlan: LogicalPlan): Boolean = {
    val joins = optimizedPlan.collect {
      case j@Join(_, _, _, _) => j
    }
    joins.exists(join => (!join.left.isInstanceOf[LocalAggregate]
      && join.right.isInstanceOf[LocalAggregate]))
  }

  private def isProjectRetainedBetweenLAAndJoin(optimizedPlan: LogicalPlan): Boolean = {
    val las = optimizedPlan.collect {
      case la@LocalAggregate(_, _, _) if (la.child.isInstanceOf[Project]
        && la.child.asInstanceOf[Project].child.isInstanceOf[Join]) => la
    }
    las.nonEmpty
  }

  private def executeQueryWithoutPreaggregation(query: String): SparkPlan = {
    var results: SparkPlan = null
    withSQLConf(SQLConf.PREAGGREGATION_ENABLED.key -> "false") {
      results = sql(query).queryExecution.executedPlan
    }
    results
  }

  /**
   * Compares the results by executing the plans in QueryExecution object
   */
  private def compareAnswers(
      actualResult: SparkPlan,
      expectedResult: SparkPlan,
      sort: Boolean = false): Unit = {
    assert(actualResult.executeCollectPublic().nonEmpty)
    SQLTestUtils.compareAnswers(actualResult.executeCollectPublic(),
      expectedResult.executeCollectPublic(), sort).map { errorMessage =>
      val newErrorMessage = errorMessage
        .replaceFirst("Expected Answer - ([0-9]+)", "Without preaggregation - $1")
        .replaceFirst("Actual Answer - ([0-9]+)", "With preaggregation - $1")
      fail(
        s"""
           |Preaggregation disabled results do not match results with preaggregation
           | enabled results:
           |$newErrorMessage
           |""".stripMargin)
    }
  }
}
