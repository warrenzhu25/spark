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

package org.apache.spark.sql.catalyst.optimizer.ms.preaggregation

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeMap, AttributeReference, Expression, ExprId, Literal}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.optimizer.ms.preaggregation.PushdownLocalAggregate._
import org.apache.spark.sql.catalyst.plans.{FullOuter, Inner, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.catalyst.statsEstimation.{StatsEstimationTestBase, StatsTestPlan}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataTypes, IntegerType}

class PushdownLocalAggregateSuite extends PlanTest with StatsEstimationTestBase {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("PreAggregation", Once, PushdownLocalAggregate) :: Nil
  }

  def attr(colName: String, id: Int): AttributeReference = AttributeReference(
    colName, IntegerType)(ExprId(id))

  private val columnInfo: AttributeMap[ColumnStat] = AttributeMap(Seq(
    attr("t1.key", 0) -> rangeColumnStat(2, 0),
    attr("t1.value", 1) -> rangeColumnStat(10, 0),
    attr("t2.key", 2) -> rangeColumnStat(2, 0),
    attr("t2.value", 3) -> rangeColumnStat(10, 0),
    attr("t3.key", 4) -> rangeColumnStat(5, 0),
    attr("t3.value", 5) -> rangeColumnStat(5, 0)
  ))

  private val nameToAttr: Map[String, Attribute] = columnInfo.map(kv => kv._1.name -> kv._1)
  private val nameToColInfo: Map[String, (Attribute, ColumnStat)] =
    columnInfo.map(kv => kv._1.name -> kv)

  // Table t1/t2: big table with two columns
  private val t1 = StatsTestPlan(
    outputList = Seq("t1.key", "t1.value").map(nameToAttr),
    rowCount = 1000,
    // size = rows * (overhead + column length)
    size = Some(1000 * (8 + 4 + 4)),
    attributeStats = AttributeMap(Seq("t1.key", "t1.value").map(nameToColInfo)))

  private val t2 = StatsTestPlan(
    outputList = Seq("t2.key", "t2.value").map(nameToAttr),
    rowCount = 2000,
    size = Some(2000 * (8 + 4 + 4)),
    attributeStats = AttributeMap(Seq("t2.key", "t2.value").map(nameToColInfo)))

  // Table t3: small table with two columns
  private val t3 = StatsTestPlan(
    outputList = Seq("t3.key", "t3.value").map(nameToAttr),
    rowCount = 20,
    size = Some(20 * (8 + 4)),
    attributeStats = AttributeMap(Seq("t3.key", "t3.value").map(nameToColInfo)))

  private def checkLocalAggregatePushThroughJoin(plan: LogicalPlan): Boolean = {
    val joins = plan.collect {
      case j@Join(_, _, _, _, _) => j
    }
    joins.forall(isChildNodeLocalAggregate(_))
  }

  private def isChildNodeLocalAggregate(plan: LogicalPlan): Boolean = {
    plan.children.forall(_.isInstanceOf[LocalAggregate])
  }

  private def checkLocalAggregateNodeExists(plan: LogicalPlan): Boolean = {
    plan.collectFirst {
      case la@LocalAggregate(_, _, _) => la
    }.nonEmpty
  }

  private def isPushThroughJoinOneSide(optimizedPlan: LogicalPlan): Boolean = {
    (isPushThroughJoinLeftSide(optimizedPlan)
      && !isPushThroughJoinRightSide(optimizedPlan)) || (!isPushThroughJoinLeftSide(optimizedPlan)
      && isPushThroughJoinRightSide(optimizedPlan))
  }

  private def isPushThroughJoinLeftSide(optimizedPlan: LogicalPlan): Boolean = {
    val joins = optimizedPlan.collect {
      case j@Join(_, _, _, _, _) => j
    }
    joins.exists(join => (join.left.isInstanceOf[LocalAggregate]
      && !join.right.isInstanceOf[LocalAggregate]))
  }

  private def isPushThroughJoinRightSide(optimizedPlan: LogicalPlan): Boolean = {
    val joins = optimizedPlan.collect {
      case j@Join(_, _, _, _, _) => j
    }
    joins.exists(join => (!join.left.isInstanceOf[LocalAggregate]
      && join.right.isInstanceOf[LocalAggregate]))
  }

  /**
   * -----------------------------------------------------------------------------------
   * Sanity tests - End to end testing
   * -----------------------------------------------------------------------------------
   */
  Seq(true, false).foreach { preAggEnabled =>
    test(s"check preaggregation node added for preAggEnabled = $preAggEnabled") {
      withSQLConf(
        SQLConf.PREAGGREGATION_ENABLED.key -> preAggEnabled.toString,
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {

        val query = t1.join(t2, Inner, Some(nameToAttr("t1.key") === nameToAttr("t2.key")))
          .groupBy(nameToAttr("t1.key"))(sum(nameToAttr("t2.value"))).analyze

        val optimized = Optimize.execute(query)

        if (preAggEnabled) {
          assert(checkLocalAggregateNodeExists(optimized))
          assert(checkLocalAggregatePushThroughJoin(optimized))
        } else {
          assert(!checkLocalAggregateNodeExists(optimized))
        }
      }
    }
  }

  test("check local aggregate push through join") {
    withSQLConf(
      SQLConf.PREAGGREGATION_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {

      val query = t1.join(t2, Inner, Some(nameToAttr("t1.key") === nameToAttr("t2.key")))
        .groupBy(nameToAttr("t1.key"))(sum(nameToAttr("t2.value"))).analyze

      val optimized = Optimize.execute(query)
      assert(checkLocalAggregateNodeExists(optimized))
      assert(checkLocalAggregatePushThroughJoin(optimized))
    }
  }

  test("check pre aggregation triggered for count") {
    withSQLConf(
      SQLConf.PREAGGREGATION_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {

      val query = t1.join(t2, Inner, Some(nameToAttr("t1.key") === nameToAttr("t2.key")))
        .groupBy(nameToAttr("t1.key"))(count(nameToAttr("t2.value"))).analyze

      val optimized = Optimize.execute(query)
      assert(checkLocalAggregateNodeExists(optimized))
      assert(checkLocalAggregatePushThroughJoin(optimized))
    }
  }

  test("check pre aggregation not triggered for distinct") {
    withSQLConf(
      SQLConf.PREAGGREGATION_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {

      val query = t1.join(t2, Inner, Some(nameToAttr("t1.key") === nameToAttr("t2.key")))
        .groupBy(nameToAttr("t1.key"))(countDistinct(nameToAttr("t2.value"))).analyze

      val optimized = Optimize.execute(query)
      assert(!checkLocalAggregateNodeExists(optimized))
    }
  }

  test("check pre aggregation triggered for multiple levels of join") {
    withSQLConf(
      SQLConf.PREAGGREGATION_CBO_ENABLED.key -> "false",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {

      val query = t1.join(t2, Inner, Some(nameToAttr("t1.key") === nameToAttr("t2.key")))
        .join(t3, Inner, Some(nameToAttr("t3.key") === nameToAttr("t2.key")))
        .groupBy(nameToAttr("t1.key"), nameToAttr("t3.value"))(sum(nameToAttr("t2.value"))).analyze

      val optimized = Optimize.execute(query)
      assert(checkLocalAggregateNodeExists(optimized))
      assert(checkLocalAggregatePushThroughJoin(optimized))
    }
  }

  test("check pre aggregation triggered for multiple levels of join push one side") {
    withSQLConf(
      SQLConf.PREAGGREGATION_CBO_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {

      val query = t1.join(t3, Inner, Some(nameToAttr("t1.key") === nameToAttr("t3.key")))
        .join(t2, Inner, Some(nameToAttr("t3.key") === nameToAttr("t2.key")))
        .groupBy(nameToAttr("t1.key"), nameToAttr("t3.value"))(
          sum(nameToAttr("t2.value")), count(nameToAttr("t3.value"))
          , sum(nameToAttr("t3.value")), max(nameToAttr("t1.value"))).analyze

      val optimized = Optimize.execute(query)
      assert(checkLocalAggregateNodeExists(optimized))
      assert(isPushThroughJoinOneSide(optimized))
      val aggregate = normalizeExprIds(optimized).asInstanceOf[Aggregate]
      assert(aggregate.child.isInstanceOf[Project])
      val topJoin = aggregate.child.asInstanceOf[Project].child.asInstanceOf[Join]
      // Stats: left: 100 right: 571 => Pushdown to both the sides
      assert(topJoin.left.isInstanceOf[LocalAggregate])
      assert(topJoin.right.isInstanceOf[LocalAggregate])
      val leftJoin = topJoin.left.asInstanceOf[LocalAggregate].child.asInstanceOf[Join]
      // Stats  left: 250 right: 0 => Pushdown only left side
      assert(leftJoin.left.isInstanceOf[LocalAggregate])
      assert(!leftJoin.right.isInstanceOf[LocalAggregate])
    }
  }

  /**
   * -----------------------------------------------------------------------------------
   * Functional tests
   * -----------------------------------------------------------------------------------
   */
  test("pushdownAggregateEnabled") {
    withSQLConf(SQLConf.PREAGGREGATION_ENABLED.key -> "false") {
      assert(!pushdownAggregateEnabled)
    }
  }

  test("cboBasedPreaggregationEnabled") {
    withSQLConf(SQLConf.PREAGGREGATION_CBO_ENABLED.key -> "false") {
      assert(!cboBasedPreaggregationEnabled)
    }
  }

  test("preaggregationCboShuffleJoinThreshold") {
    withSQLConf(SQLConf.PREAGGREGATION_CBO_SHUFFLE_JOIN_PUSHDOWN_THRESHOLD.key -> "100") {
      assert(preaggregationCboShuffleJoinThreshold === 100)
    }
  }

  test("validate aggregate expression is non deterministic") {
    val aggregate = t1.groupBy(nameToAttr("t1.key"))(sum(nameToAttr("t2.key")) * rand(1))
      .analyze.asInstanceOf[Aggregate]
    assert(!validateAggregateIsSplittable(aggregate))
  }

  test("validate group by expression is non deterministic") {
    val aggregate = t1.groupBy(nameToAttr("t1.key") * rand(1))(sum(nameToAttr("t2.key")))
      .analyze.asInstanceOf[Aggregate]
    assert(!validateAggregateIsSplittable(aggregate))
  }

  test("validate aggregation function distinct") {
    val aggregate = t1.groupBy(nameToAttr("t1.key"))(countDistinct(nameToAttr("t2.key")))
      .analyze.asInstanceOf[Aggregate]
    assert(!validateAggregateIsSplittable(aggregate))
  }

  test("validate grouping keys has multiple grouping reference attributes") {
    val aggregate = t1.groupBy(nameToAttr("t1.key") * nameToAttr("t2.key"))(
      sum(nameToAttr("t2.value"))).analyze.asInstanceOf[Aggregate]
    assert(!validateAggregateIsSplittable(aggregate))
  }

  Seq("max", "min", "sum", "count").foreach { aggFunction =>
    test(s"validate aggregate is splittable for  $aggFunction") {
      val aggregate = t1.groupBy(nameToAttr("t1.key"))(aggExpn(nameToAttr("t2.key"), aggFunction))
        .analyze.asInstanceOf[Aggregate]
      assert(validateAggregateIsSplittable(aggregate))
    }
  }

  Seq("max", "min", "sum", "count").foreach { aggFunction =>
    test(s"splitAggregateToAggregateAndLocalAggregate for $aggFunction") {
      val aggregate = t1.join(t2, Inner, Some(nameToAttr("t1.key") === nameToAttr("t2.key")))
        .groupBy(nameToAttr("t1.key"))(aggExpn(nameToAttr("t2.value"), aggFunction))
        .analyze.asInstanceOf[Aggregate]

      val newAggregateOption = splitAggregateToAggregateAndLocalAggregate(aggregate)
      assert(newAggregateOption.isDefined)

      val newAggregate = normalizeExprIds(newAggregateOption.get).asInstanceOf[Aggregate]
      assert(checkLocalAggregateNodeExists(newAggregate))

      val localAggregate = newAggregate.child.asInstanceOf[LocalAggregate]
      assert(localAggregate.groupingExpressions == newAggregate.groupingExpressions)

      assert(newAggregate.child.children.forall(_.isInstanceOf[Join]))
      assert(newAggregate.groupingExpressions === aggregate.groupingExpressions)

      if (aggFunction == "count") {
        assert(newAggregate.aggregateExpressions.map(_.expr)
          .toString.contains(s"sum($aggFunction(t2.value"))
      } else if (aggFunction == "sum") {
        assert(newAggregate.aggregateExpressions.map(_.expr)
          .toString.contains(s"$aggFunction($aggFunction(cast(t2.value"))
      } else {
        assert(newAggregate.aggregateExpressions.map(_.expr)
          .toString.contains(s"$aggFunction($aggFunction(t2.value"))
      }
    }
  }

  test("push down local aggregate through join: non inner join") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      val aggregate = t1.join(t2, FullOuter, Some(nameToAttr("t1.key") === nameToAttr("t2.key")))
        .groupBy(nameToAttr("t1.key"))(sum(nameToAttr("t2.value"))).analyze.asInstanceOf[Aggregate]
      val newAggregate =
        splitAggregateToAggregateAndLocalAggregate(aggregate).get
      val planAfterPushdownOption = pushDownLocalAggregateThroughJoin(
        newAggregate.child.asInstanceOf[LocalAggregate], retain = false)
      assert(planAfterPushdownOption.isEmpty)
    }
  }

  test("push down local aggregate through join: broadcast join") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "100000") {
      val aggregate = t1.join(t2, Inner, Some(nameToAttr("t1.key") === nameToAttr("t2.key")))
        .groupBy(nameToAttr("t1.key"))(sum(nameToAttr("t2.value"))).analyze.asInstanceOf[Aggregate]
      val newAggregate =
        splitAggregateToAggregateAndLocalAggregate(aggregate).get
      val planAfterPushdownOption = pushDownLocalAggregateThroughJoin(
        newAggregate.child.asInstanceOf[LocalAggregate], retain = false)
      assert(planAfterPushdownOption.isEmpty)
    }
  }

  test("push down local aggregate through join with broadcast hints") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      val aggregate = t1.join(t2.hint("broadcast"), Inner, Some(nameToAttr("t1.key") === nameToAttr("t2.key")))
        .groupBy(nameToAttr("t1.key"))(sum(nameToAttr("t2.value"))).analyze.asInstanceOf[Aggregate]
      val newAggregate =
        splitAggregateToAggregateAndLocalAggregate(aggregate).get
      val planAfterPushdownOption = pushDownLocalAggregateThroughJoin(
        newAggregate.child.asInstanceOf[LocalAggregate], retain = false)
      assert(planAfterPushdownOption.isEmpty)
    }
  }

  test("push down local aggregate through join: stats not satisfied") {
    withSQLConf(
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.PREAGGREGATION_CBO_SHUFFLE_JOIN_PUSHDOWN_THRESHOLD.key -> "1000") {
      val aggregate = t1.join(t2, Inner, Some(nameToAttr("t1.key") === nameToAttr("t2.key")))
        .groupBy(nameToAttr("t1.key"))(sum(nameToAttr("t2.value")))
        .analyze.asInstanceOf[Aggregate]
      val newAggregate =
        splitAggregateToAggregateAndLocalAggregate(aggregate).get
      val planAfterPushdownOption = pushDownLocalAggregateThroughJoin(
        newAggregate.child.asInstanceOf[LocalAggregate], retain = false)
      assert(planAfterPushdownOption.isEmpty)
    }
  }

  test("push down local aggregate through join: complicated join condition") {
    withSQLConf(
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.PREAGGREGATION_CBO_ENABLED.key -> "false") {
      val aggregate = t1.join(t2, Inner,
        Some(nameToAttr("t1.key") * 2 === nameToAttr("t2.key") + nameToAttr("t2.value")))
        .groupBy(nameToAttr("t1.key"))(sum(nameToAttr("t2.value")))
        .analyze.asInstanceOf[Aggregate]
      val newAggregate =
        splitAggregateToAggregateAndLocalAggregate(aggregate).get
      val planAfterPushdownOption = pushDownLocalAggregateThroughJoin(
        newAggregate.child.asInstanceOf[LocalAggregate], retain = false)
      assert(planAfterPushdownOption.isDefined)
      assert(checkLocalAggregatePushThroughJoin(planAfterPushdownOption.get))
    }
  }

  test("getAllAggregateExpressions") {
    val aggExpressions = getAllAggregateExpressions(Seq(sum(nameToAttr("t1.key")) * 2
      , sum(nameToAttr("t1.key")) / count(nameToAttr("t2.key")), nameToAttr("t3.key"))).map {
      normalizeExpressionId(_)
    }
    val expected = Seq(
      sum(nameToAttr("t1.key")),
      sum(nameToAttr("t1.key")),
      count(nameToAttr("t2.key"))).map(normalizeExpressionId)
    assert(aggExpressions.size === 3)
    assert(aggExpressions === expected)
  }

  test("createAliasesForPushedDownLocalAggregate") {
    val aggExpressions = getAllAggregateExpressions(
      Seq(sum(nameToAttr("t1.key")) * 2, sum(nameToAttr("t2.key"))
        / count(nameToAttr("t2.value"))))
    val aliasMap = createAliasesForPushedDownLocalAggregate(aggExpressions)
    assert(aliasMap.size === 3)
    assert(aggExpressions.forall(aliasMap.get(_).isDefined))
  }

  test("aggregateFunctionNeedsCountFromOtherSide") {
    var aggExpressions = getAllAggregateExpressions(
      Seq(avg(nameToAttr("t1.key")) * 2, min(nameToAttr("t2.key")) / max(nameToAttr("t2.value"))))
    assert(!aggExpressions.forall(aggregateFunctionNeedsCountFromOtherSide))
    aggExpressions = getAllAggregateExpressions(
      Seq(sum("t1.key") * 2, count("t2.key") / sum("t2.value")))
    assert(aggExpressions.forall(aggregateFunctionNeedsCountFromOtherSide))
  }

  test("getNamedGroupingExpressions") {
    val namedGroupingExpressions = getNamedGroupingExpressions(
      Seq(nameToAttr("t1.key"), "xyz", nameToAttr("t1.key") * 2)).map {
       normalizeExpressionId(_)
    }
    val expected = Seq(
      nameToAttr("t1.key"),
      Literal("xyz").as("groupExp1"),
      (nameToAttr("t1.key") * 2).as("groupExp2")).map(normalizeExpressionId)
    assert(namedGroupingExpressions.size === 3)
    assert(namedGroupingExpressions === expected)
  }

  test("castIfNeeded") {
    var castExpression = castIfNeeded(nameToAttr("t1.value"), DataTypes.IntegerType)
    assert(nameToAttr("t1.value").dataType === DataTypes.IntegerType)
    assert(castExpression.dataType === DataTypes.IntegerType)
    castExpression = castIfNeeded(nameToAttr("t1.value"), DataTypes.DoubleType)
    assert(castExpression.dataType === DataTypes.DoubleType)
  }

  test("rewriteUpperAggregateExpressionAfterPushdown") {
    val aggregate = t1.join(t2, Inner, Some(nameToAttr("t1.key") === nameToAttr("t2.key")))
      .groupBy(nameToAttr("t1.key"))(count(nameToAttr("t2.value")) * 2)
      .analyze.asInstanceOf[Aggregate]
    val newAggregate = splitAggregateToAggregateAndLocalAggregate(aggregate).get

    assert(normalizeExprIds(newAggregate).asInstanceOf[Aggregate].aggregateExpressions.head
      .expr.toString == "(coalesce(sum(count(t2.value#3)#0L), 0) * cast(2 as bigint))" +
      " AS (count(t2.value#3) * 2)#0L")
  }

  test("canBroadcast") {
    withSQLConf(
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      val join = t1.join(t2, Inner, Some(nameToAttr("t1.key") * 2 === nameToAttr("t2.key"))).analyze
      assert(!canBroadcast(Inner, join.asInstanceOf[Join].left, join.asInstanceOf[Join].right, join.asInstanceOf[Join].hint))
    }
  }

  test("isDeterministic") {
    val aggregate = t1.join(t2, FullOuter, Some(nameToAttr("t1.key") === nameToAttr("t2.key")))
      .groupBy(nameToAttr("t1.key"))(sum(nameToAttr("t2.value")) * rand(1))
      .analyze.asInstanceOf[Aggregate]
    assert(!isDeterministic(aggregate))
  }

  Seq("min", "max", "count", "sum", "avg", "distinct").foreach { aggFn =>
    test(s"hasValidAggregateFunctions: aggFunction: $aggFn") {
      val aggregate = t1.join(t2, FullOuter, Some(nameToAttr("t1.key") === nameToAttr("t2.key")))
        .groupBy(nameToAttr("t1.key"))(aggExpn(nameToAttr("t2.value")
          , aggFn)).analyze.asInstanceOf[Aggregate]
      if (aggFn == "avg") {
        assert(!hasValidAggregateFunctions(aggregate))
      } else {
        assert(hasValidAggregateFunctions(aggregate))
      }
    }
  }

  test("expressionPushableToPlan") {
    val aggregate = t1.join(t2, Inner, Some(nameToAttr("t1.key") === nameToAttr("t2.key")))
      .select("t1.key", "t2.value")
      .groupBy(nameToAttr("t1.key") * nameToAttr("t1.value"))(sum(nameToAttr("t2.value")))
      .analyze.asInstanceOf[Aggregate]
    assert(!aggregate.groupingExpressions.forall {
      expressionPushableToPlan(_, aggregate.child)
    })
  }

  test("pushDownLocalAggregate not happened due filter operator") {
    val aggregate = t1.join(t2, Inner, Some(nameToAttr("t1.key") === nameToAttr("t2.key")))
      .where(nameToAttr("t1.key") == nameToAttr("t2.key"))
      .groupBy(nameToAttr("t1.key"))(sum(nameToAttr("t2.value")))
      .analyze.asInstanceOf[Aggregate]
    val newAggregate = splitAggregateToAggregateAndLocalAggregate(aggregate)

    assert(pushDownLocalAggregate(newAggregate.get.child.asInstanceOf[LocalAggregate],
      true).isEmpty)
  }

  test("processLocalAggregate not happened due to filter operator") {
    val aggregate = t1.join(t2, Inner, Some(nameToAttr("t1.key") === nameToAttr("t2.key")))
      .where(nameToAttr("t1.key") == nameToAttr("t2.key"))
      .groupBy(nameToAttr("t1.key"))(sum(nameToAttr("t2.value")))
      .analyze.asInstanceOf[Aggregate]
    val localAggregate = splitAggregateToAggregateAndLocalAggregate(aggregate).get
      .child.asInstanceOf[LocalAggregate]

    assert(processLocalAggregate(localAggregate) === localAggregate)
  }

  Seq(100, 1000).foreach { ratioThreshold =>
    test(s"processLocalAggregate for reduction ratio threshold $ratioThreshold") {
      withSQLConf(
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.PREAGGREGATION_CBO_SHUFFLE_JOIN_PUSHDOWN_THRESHOLD.key
          -> ratioThreshold.toString) {
        val aggregate = t1.join(t2, Inner, Some(nameToAttr("t1.key") === nameToAttr("t2.key")))
          .groupBy(nameToAttr("t1.key"))(sum(nameToAttr("t2.value")))
          .analyze.asInstanceOf[Aggregate]
        val localAggregate = splitAggregateToAggregateAndLocalAggregate(aggregate).get.child
          .asInstanceOf[LocalAggregate]

        if (ratioThreshold == 1000) {
          assert(processLocalAggregate(localAggregate) === localAggregate)
        } else {
          assert(processLocalAggregate(localAggregate) !== localAggregate)
        }
      }
    }
  }

  test("processAggregate not splittable") {
    val aggregate = t1.join(t2, Inner, Some(nameToAttr("t1.key") === nameToAttr("t2.key")))
      .groupBy(nameToAttr("t1.key") * nameToAttr("t2.key"))(countDistinct(nameToAttr("t2.value")))
      .analyze.asInstanceOf[Aggregate]
    assert(processAggregate(aggregate) === aggregate)
  }

  Seq(100, 1000).foreach { ratioThreshold =>
    test(s"processAggregate pushdown validation for ratio threshold $ratioThreshold") {
      withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.PREAGGREGATION_CBO_SHUFFLE_JOIN_PUSHDOWN_THRESHOLD.key
          -> ratioThreshold.toString) {
        val aggregate = t1.join(t2, Inner, Some(nameToAttr("t1.key") === nameToAttr("t2.key")))
          .groupBy(nameToAttr("t1.key"))(sum(nameToAttr("t2.value")))
          .analyze.asInstanceOf[Aggregate]

        if (ratioThreshold == 1000) {
          assert(processAggregate(aggregate) === aggregate)
        } else {
          assert(processAggregate(aggregate) !== aggregate)
        }
      }
    }
  }

  test("push down local aggregate through project with aliases") {
    withSQLConf(
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.PREAGGREGATION_CBO_ENABLED.key -> "false") {
      val aggregateKey = nameToAttr("t2.value").as("ball").as("bat")
      val groupKey = nameToAttr("t1.key").as("apple").as("orange")

      val aggregate = t1.join(t2, Inner, Some(nameToAttr("t1.key") === nameToAttr("t2.key")))
        .select(aggregateKey, groupKey).
        groupBy(groupKey.toAttribute)(sum(aggregateKey.toAttribute)).
        analyze.asInstanceOf[Aggregate]
      val newAggregate =
        splitAggregateToAggregateAndLocalAggregate(aggregate).get

      val planAfterPushdownOption = pushDownLocalAggregateThroughProject(
        newAggregate.child.asInstanceOf[LocalAggregate], false)
      assert(planAfterPushdownOption.isDefined)
      val plan = planAfterPushdownOption.get
      assert(checkLocalAggregatePushThroughJoin(plan))
      val joinNode = plan.children.head.asInstanceOf[Join]
      val groupingExpression = joinNode.left.asInstanceOf[LocalAggregate]
        .groupingExpressions.filter(_.isInstanceOf[Alias])
      val aliasNames = groupingExpression.map(_.name)
      assert(aliasNames == Seq("orange"))
      assert(groupingExpression.head.asInstanceOf[Alias].child
        .asInstanceOf[Attribute].name == "t1.key")
    }
  }

  private def aggExpn(expression: Expression, aggFunction: String): Expression = {
    aggFunction match {
      case _ if aggFunction == "sum" => sum(expression)
      case _ if aggFunction == "min" => min(expression)
      case _ if aggFunction == "max" => max(expression)
      case _ if aggFunction == "count" => count(expression)
      case _ if aggFunction == "avg" => avg(expression)
      case _ if aggFunction == "distinct" => countDistinct(expression)
    }
  }

  private def normalizeExpressionId(exp: Expression): Expression = {
    exp match {
      case a: Alias => Alias(a.child, a.name)(ExprId(0))
      case a: AggregateExpression => a.copy(resultId = ExprId(0))
      case a: AttributeReference => AttributeReference(a.name, a.dataType)(ExprId(0))
      case _ => exp
    }
  }
}
