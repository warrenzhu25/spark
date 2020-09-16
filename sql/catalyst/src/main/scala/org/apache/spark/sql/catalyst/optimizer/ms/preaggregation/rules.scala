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

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.optimizer.ColumnPruning
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.DataType

object PushdownLocalAggregate extends Rule[LogicalPlan] with Logging {

  // Configs to control feature
  private[preaggregation] def pushdownAggregateEnabled =
    SQLConf.get.preaggregationEnabled
  private[preaggregation] def cboBasedPreaggregationEnabled =
    SQLConf.get.cboBasedPreaggregationEnabled
  private[preaggregation] def preaggregationCboShuffleJoinThreshold =
    SQLConf.get.preaggregationCboShuffleJoinThreshold

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!pushdownAggregateEnabled) {
      return plan
    }

    /**
     * This is to prevent multiple LA optimization on the same plan,
     * especially in the scenario of subqueries in expressions.
     */
    if (isPlanAlreadyPreAggregationOptimized(plan)) {
      return plan
    }

    val newPlan = plan transformDown {
      case agg: Aggregate =>
        val processedAgg = processAggregate(agg)
        if (processedAgg != agg) {
          logDebug("pushdown Agg done")
        }
        processedAgg
      case localAgg: LocalAggregate =>
        val processedLocalAgg = processLocalAggregate(localAgg)
        if (processedLocalAgg != localAgg) {
          logDebug("pushdown LocalAgg done")
        }
        processedLocalAgg
    }
    newPlan
  }

  private def isPlanAlreadyPreAggregationOptimized(plan: LogicalPlan): Boolean = {
    plan.find(_.isInstanceOf[LocalAggregate]).nonEmpty
  }

  /**
   * This method takes an Aggregate and pushes a LocalAggregate down below Projects and Joins
   * If it couldn't successfully pushdown the LocalAggregate, then the original Aggregate is
   * returned.
   * Input:    Aggregate
   *              |
   *             Plan
   * Output:   Aggregate (new)
   *              |
   *         ChildPlanContainingLocalAggregate
   */
  private[preaggregation] def processAggregate(aggregate: Aggregate): Aggregate = {
    val aggregateWithLocalAggregate = splitAggregateToAggregateAndLocalAggregate(aggregate)
    aggregateWithLocalAggregate match {
      case Some(newAggregate@Aggregate(_, _, localAgg: LocalAggregate)) =>
        val pushedDownLocalAggregate = pushDownLocalAggregate(localAgg, retain = false)
        if (pushedDownLocalAggregate.isEmpty) {
          aggregate
        } else {
          newAggregate.copy(child = pushedDownLocalAggregate.get)
        }
      case _ =>
        aggregate
    }
  }

  /**
   * This method takes a LocalAggregate and pushes it down below Projects and Joins
   * If it couldn't successfully pushdown the LocalAggregate, then the original LocalAggregate is
   * returned.
   * Input:  LocalAggregate
   *           |
   *          Plan
   * Output:   LocalAggregate (new)
   *              |
   *         ChildPlanContainingLocalAggregate
   */
  private[preaggregation] def processLocalAggregate(
      localAggregate: LocalAggregate): LogicalPlan = {
    val pushedDownLocalAggregate = pushDownLocalAggregate(localAggregate, retain = true)
    if (pushedDownLocalAggregate.isEmpty) {
      return localAggregate
    }
    pushedDownLocalAggregate.get
  }

  /**
   * This method takes an Aggregate and breaks it into Aggregate -> LocalAggregate -> child
   * if possible
   * Input:  Aggregate                  Output:   Aggregate-New
   *             |                                  |
   *            Child                             LocalAggregate-New
   *                                                 |
   *                                                 child
   * Ex- Aggregate(groupingExpressions = [col1, col2, col3],
   *               aggregateExpressions = [max(col4) as ans1, count(col5) as ans2])
   *               |
   *               child
   * will become
   *     Aggregate(groupingExpressions = [col1, col2, col3],
   *               aggregateExpressions = [max(partial_col4) as ans1, sum(partial_col5) as ans2])
   *               |
   * localAggregate(groupingExpressions = [col1, col2, col3],
   *               aggregateExpressions = [max(col4) as partial_col4, count(col5) as partial_col5])
   *               |
   *               child
   */
  private[preaggregation] def splitAggregateToAggregateAndLocalAggregate(
      aggregate: Aggregate): Option[Aggregate] = {
    if (!validateAggregateIsSplittable(aggregate)) {
      return None
    }

    val groupingExpressions = aggregate.groupingExpressions
    val aggregateExpressions = aggregate.aggregateExpressions

    val namedGroupingExpressions = getNamedGroupingExpressions(groupingExpressions)
    val groupingExprsToNamedExprsMap = groupingExpressions.zip(namedGroupingExpressions).toMap

    val realAggExprs = getAllAggregateExpressions(aggregateExpressions)
    val realAggExprsToAliasMap = createAliasesForPushedDownLocalAggregate(realAggExprs)
    val realAggExprsAliased = realAggExprs.map(realAggExprsToAliasMap)

    val groupingExpressionsForLocalAggregate = namedGroupingExpressions
    val aggregateExpressionsForLocalAggregate = realAggExprsAliased
    val newLocalAggregate = LocalAggregate(
      groupingExpressionsForLocalAggregate,
      aggregateExpressionsForLocalAggregate,
      aggregate.child)

    // create result Map
    val agExpToResultMap = realAggExprsToAliasMap.map {
      case (ag, alias) => (ag, alias.toAttribute)
    }
    val groupingExpressionsForAggregate = namedGroupingExpressions.map(_.toAttribute)
    val aggregateExpressionsForAggregate = aggregateExpressions.map { exp =>
      val normalizedExpr = rewriteUpperAggregateExpressionAfterPushdown(exp, agExpToResultMap)
      normalizedExpr.transformUp {
        case e: Expression =>
          groupingExprsToNamedExprsMap.get(e).map(_.toAttribute).getOrElse(e)
      }.asInstanceOf[NamedExpression]
    }

    val newAggWithLocalAgg = Aggregate(
      groupingExpressionsForAggregate,
      aggregateExpressionsForAggregate,
      newLocalAggregate)
    Some(newAggWithLocalAgg)
  }

  /**
   * This method takes a LocalAggregate and tries to push it down recursively until some
   * benefit is seen.
   * If it doesn't see benefit anywhere or if it finds operator from where
   * pushdown is not handled (Ex - Filter), then returns None
   */
  private[preaggregation] def pushDownLocalAggregate(
      localAggregate: LocalAggregate,
      retain: Boolean): Option[LogicalPlan] = {
    localAggregate match {
      case la@LocalAggregate(_, _, _: Project) =>
        pushDownLocalAggregateThroughProject(la, retain)
      case la@LocalAggregate(_, _, _: Join) =>
        pushDownLocalAggregateThroughJoin(la, retain)
      case _ =>
        None
    }
  }

  /**
   * Pushdown LocalAggregate through Project
   */
  private[preaggregation] def pushDownLocalAggregateThroughProject(
      localAggregate: LocalAggregate,
      retain: Boolean = true): Option[LogicalPlan] = {

    if (!localAggregate.child.isInstanceOf[Project]) {
      return None
    }

    val project = localAggregate.child.asInstanceOf[Project]
    val attributeSeq = project.projectList.filter(_.isInstanceOf[Alias]).map { p =>
      (p.toAttribute, p.asInstanceOf[Alias])
    }

    if (attributeSeq.isEmpty) {
      pushDownLocalAggregate(localAggregate.copy(child = project.child), retain)
    } else {
      val attributeMap: AttributeMap[Alias] = AttributeMap(attributeSeq)
      val groupingExpressions = localAggregate.groupingExpressions.map {
        _.transformUp {
          case attribute: Attribute => attributeMap.get(attribute).getOrElse(attribute)
        }.asInstanceOf[NamedExpression]
      }

      val aggregateExpression = localAggregate.otherAggregateExpressions.map {
        _.transformUp {
          case attribute: Attribute => attributeMap.get(attribute)
            .map(_.child).getOrElse(attribute)
        }.asInstanceOf[NamedExpression]
      }

      pushDownLocalAggregate(localAggregate.copy(child = project.child,
        groupingExpressions = groupingExpressions,
        otherAggregateExpressions = aggregateExpression), retain)
    }
  }

  /**
   * Pushdown LocalAggregate through Join
   */
  private[preaggregation] def pushDownLocalAggregateThroughJoin(
      localAggregate: LocalAggregate,
      retain: Boolean = true): Option[LogicalPlan] = {
    if (!localAggregate.child.isInstanceOf[Join]) {
      return None
    }
    val join = localAggregate.child.asInstanceOf[Join]

    // Currently we support only Inner Join
    if (join.joinType != Inner) {
      return None
    }

    // Currently we don't do Aggregate pushdown via broadcast join
    val isBroadcast = canBroadcast(join.right) || canBroadcast(join.left)
    if (isBroadcast) {
      logDebug("Not pushing down as one of the side is broadcastable")
      return None
    }

    // Partition grouping keys into left and right side
    val (groupingExpressionsLeft, remainingGroupingExpressions) =
      localAggregate.groupingExpressions.partition(expressionPushableToPlan(_, join.left))
    val (groupingExpressionsRight, groupingExpressionsBelongingToBothSide) =
      remainingGroupingExpressions.partition(expressionPushableToPlan(_, join.right))
    if (groupingExpressionsBelongingToBothSide.nonEmpty) {
      return None
    }

    // Partition aggregate expressions into left and right side
    val (aggregateExpressionsLeft, remainingAggregateExpressions) =
      localAggregate.otherAggregateExpressions.partition(expressionPushableToPlan(_, join.left))
    val (aggregateExpressionsRight, aggregateExpressionsBelongingToBothSide) =
      remainingAggregateExpressions.partition(expressionPushableToPlan(_, join.right))
    if (aggregateExpressionsBelongingToBothSide.nonEmpty) {
      return None
    }

    join match {
      case ExtractEquiJoinKeys(Inner, leftKeys, rightKeys, None, left, right, hint) =>
        val realAggExprsLeft = getAllAggregateExpressions(aggregateExpressionsLeft)
        val realAggExprsRight = getAllAggregateExpressions(aggregateExpressionsRight)

        val needCountLeft = realAggExprsRight.exists(aggregateFunctionNeedsCountFromOtherSide)
        val needCountRight = realAggExprsLeft.exists(aggregateFunctionNeedsCountFromOtherSide)

        val leftAliasAggregateCount = if (needCountLeft) {
          Some(Alias(Sum(Literal(1)).toAggregateExpression(), "partialCountLeft")())
        } else {
          None
        }

        val rightAliasAggregateCount = if (needCountRight) {
          Some(Alias(Sum(Literal(1)).toAggregateExpression(), "partialCountRight")())
        } else {
          None
        }

        val (newLeftLocalAggregate, leftJoinKeysNamedExprMap, realAggExprsToAliasMapLeft) =
          createLocalAggregateBelowJoin(realAggExprsLeft, leftKeys, left,
            groupingExpressionsLeft, leftAliasAggregateCount)

        val (newRightLocalAggregate, rightJoinKeysNamedExprMap, realAggExprsToAliasMapRight) =
          createLocalAggregateBelowJoin(realAggExprsRight, rightKeys, right,
            groupingExpressionsRight, rightAliasAggregateCount)

        val newJoinInfo = getNewJoinAfterLAPushdown(
          join,
          newLeftLocalAggregate,
          newRightLocalAggregate,
          leftJoinKeysNamedExprMap,
          rightJoinKeysNamedExprMap)

        val (newJoin, shouldPushThroughLeft, shouldPushThroughRight) = {
          newJoinInfo match {
            case (None, _, _) =>
              return None
            case (Some(newJoin), shouldPushThroughLeft, shouldPushThroughRight) =>
              (newJoin, shouldPushThroughLeft, shouldPushThroughRight)
          }
        }

        val (aggregateToProjectMapForPushDown, aggregateToProjectMapForNonPushDown) =
          getAggregateToProjectMap(
            realAggExprsLeft,
            realAggExprsRight,
            realAggExprsToAliasMapLeft,
            realAggExprsToAliasMapRight,
            shouldPushThroughLeft,
            shouldPushThroughRight,
            leftAliasAggregateCount.map(_.toAttribute),
            rightAliasAggregateCount.map(_.toAttribute))

        val groupingExprsAfterJoin = getGroupingExpression(
          groupingExpressionsLeft,
          groupingExpressionsRight,
          shouldPushThroughLeft,
          shouldPushThroughRight)

        if (retain) {
          val newOtherAggregateExpressions = localAggregate.otherAggregateExpressions.map { exp =>
            rewriteUpperAggregateExpressionAfterPushdown(exp,
              aggregateToProjectMapForPushDown ++ aggregateToProjectMapForNonPushDown)
              .asInstanceOf[NamedExpression]
          }
          val newLA = LocalAggregate(groupingExprsAfterJoin, newOtherAggregateExpressions, newJoin)
          // TODO: In future, we can call ColumnPruning rule in Preaggregation batch to take
          //  care of project insertion instead of checking and inserting here
          Some(newLA.copy(child = ColumnPruning.prunedChild(newLA.child, newLA.references)))
        } else {
          val projectList = getProjectListAfterPushdownBelowJoin(
            groupingExprsAfterJoin,
            aggregateExpressionsLeft,
            aggregateExpressionsRight,
            shouldPushThroughLeft,
            shouldPushThroughRight,
            aggregateToProjectMapForPushDown,
            aggregateToProjectMapForNonPushDown)
          Some(Project(projectList, newJoin))
        }
      case _ =>
        logDebug("Not pushing down as join condition is not simple equi-join " +
          s"condition: ${join.condition}")
        None
    }
  }

  /**
   * This method returns the new grouping Expressions after(above) the Join after LA is
   * pushed down through Join. If the LA is pushdown to one side this will return attribute
   * of the grouping expression to that side, else will return the grouping expression
   * itself to the side.
   *
   * Eg:   LA[t1.col1 as gpExp1, t2.col2 as gpExp2] []
   *         |
   *         Join
   *
   *         to
   *
   *         Project[gpExp1, t2.col2 as gpExp2]
   *                    Join
   *         /                      \
   *       /                         \
   *     LA[t1.col1 as gpExp1]       t2
   *     |
   *     t1
   *
   * This method is called from pushDownLocalAggregateThroughJoin.
   */
  private def getGroupingExpression(
      groupingExpressionsLeft: Seq[NamedExpression],
      groupingExpressionsRight: Seq[NamedExpression],
      shouldPushThroughLeft: Boolean,
      shouldPushThroughRight: Boolean): Seq[NamedExpression] = {
    if (shouldPushThroughLeft && shouldPushThroughRight) {
      groupingExpressionsLeft ++ groupingExpressionsRight map (_.toAttribute)
    } else if (shouldPushThroughLeft) {
      groupingExpressionsLeft.map(_.toAttribute) ++ groupingExpressionsRight
    } else {
      groupingExpressionsLeft ++ groupingExpressionsRight.map(_.toAttribute)
    }
  }

  /**
   * This method is to get project list after LA pushdown. It mainly handles
   * the aggregateExpressions to project for pushdown and non pushdown cases.
   *
   * This method is called from pushDownLocalAggregateThroughJoin.
   */
  private def getProjectListAfterPushdownBelowJoin(
      groupingExpressionsToProject: Seq[NamedExpression],
      aggregateExpressionsLeft: Seq[NamedExpression],
      aggregateExpressionsRight: Seq[NamedExpression],
      shouldPushThroughLeft: Boolean,
      shouldPushThroughRight: Boolean,
      aggregateToProjectMapForPushDown: Map[AggregateExpression, Expression],
      aggregateToProjectMapForNonPushDown: Map[AggregateExpression, Expression])
  : Seq[NamedExpression] = {

    def updateProjectListFromAggregation(
        aggExpression: Seq[NamedExpression],
        aggToProjectMap: Map[AggregateExpression, Expression]) = {
      aggExpression.map {
        _.transformUp {
          case agExp: AggregateExpression =>
            aggToProjectMap.getOrElse(agExp, agExp)
        }.asInstanceOf[NamedExpression]
      }
    }

    val pushdownAggregateExpressionsToProject = updateProjectListFromAggregation(
      if (shouldPushThroughLeft && shouldPushThroughRight) {
        aggregateExpressionsLeft ++ aggregateExpressionsRight
      } else if (shouldPushThroughLeft) {
        aggregateExpressionsLeft
      } else {
        aggregateExpressionsRight
      }, aggregateToProjectMapForPushDown)

    val nonPushdownAggregateExpressionsToProject = updateProjectListFromAggregation(
      if (!shouldPushThroughRight) {
        aggregateExpressionsRight
      } else if (!shouldPushThroughLeft) {
        aggregateExpressionsLeft
      } else {
        Seq.empty
      }, aggregateToProjectMapForNonPushDown)

    groupingExpressionsToProject ++  pushdownAggregateExpressionsToProject ++
      nonPushdownAggregateExpressionsToProject
  }

  private def createLocalAggregateBelowJoin(
      realAggExprs: Seq[AggregateExpression],
      joinKeys: Seq[Expression],
      joinSide: LogicalPlan,
      groupingExpressions: Seq[NamedExpression],
      aliasAggregateCount: Option[Alias]) = {

    val joinKeysNamedExprMap = joinKeys.zip(getNamedGroupingExpressions(joinKeys)).toMap
    val realAggExprsToAliasMap = createAliasesForPushedDownLocalAggregate(realAggExprs)

    // Create new grouping keys for localAggregate using the relevant
    // grouping keys and the join keys belonging to this side
    // Here we take distinct as grouping expression might be same as join key. So in such
    // case we should have that attribute only once in the join key
    // TODO: Handle more complicates cases here. Ex - groupingKey - 2*col1, join key - 2*col1
    // In such case, ideally the pushed down LA should have only 2*col1 once as grouping key
    val laGroupingKeys = (groupingExpressions ++ joinKeys.map(joinKeysNamedExprMap(_))).distinct

    // Create new aggregate expressions for localAggregate using the previous
    // aggregate expressions
    val laAggregateExprs = realAggExprs.map(realAggExprsToAliasMap) ++ aliasAggregateCount

    // Create new LA which will act as children of Join
    val newLaAfterPushdown = LocalAggregate(laGroupingKeys, laAggregateExprs, joinSide)

    (newLaAfterPushdown, joinKeysNamedExprMap, realAggExprsToAliasMap)
  }

  private def getNewJoinAfterLAPushdown(
      join: Join,
      newLeftLocalAggregate: LocalAggregate,
      newRightLocalAggregate: LocalAggregate,
      leftJoinKeysNamedExprMap: Map[Expression, NamedExpression],
      rightJoinKeysNamedExprMap: Map[Expression, NamedExpression])
  : (Option[Join], Boolean, Boolean) = {

    var shouldPushThroughLeft = true
    var shouldPushThroughRight = true

    if (cboBasedPreaggregationEnabled) {
      val leftRatio = newLeftLocalAggregate.reductionRatioAssumingAggregateStats
      val rightRatio = newRightLocalAggregate.reductionRatioAssumingAggregateStats
      val ratioThreshold = BigInt(preaggregationCboShuffleJoinThreshold)
      val ratioStr = s"[ leftRatio: $leftRatio, rightRatio: $rightRatio, " +
        s"threshold: $ratioThreshold ]"
      if (leftRatio < ratioThreshold && rightRatio < ratioThreshold) {
        logInfo(s"Not pushing down to any side as stats criteria not met $ratioStr")
        return (None, false, false)
      }
      if (leftRatio < ratioThreshold) {
        logInfo(s"Not pushing down to left side as stats criteria not met $ratioStr")
        shouldPushThroughLeft = false
      }

      if (rightRatio < ratioThreshold) {
        logInfo(s"Not pushing down to right side as stats criteria not met $ratioStr")
        shouldPushThroughRight = false
      }
    }
    // Create new Join condition
    // Previously join condition might be x*2 = y*3. Now x*2, y*3 are namedexpression
    // in the child plan. So we have to rewrite joinCondition using the same
    val joinCondition = join.condition match {
      case None => None
      case Some(cond) =>
        val newCond = cond.transformUp {
          case e: Expression =>
            if (shouldPushThroughLeft && shouldPushThroughRight) {
              leftJoinKeysNamedExprMap.get(e).map(_.toAttribute).getOrElse(
                rightJoinKeysNamedExprMap.get(e).map(_.toAttribute).getOrElse(e))
            } else if (shouldPushThroughLeft) {
              leftJoinKeysNamedExprMap.get(e).map(_.toAttribute).getOrElse(e)
            } else {
              rightJoinKeysNamedExprMap.get(e).map(_.toAttribute).getOrElse(e)
            }
        }
        Some(newCond)
    }

    val joinHint = join.hint

    val newJoin = if (shouldPushThroughLeft && shouldPushThroughRight) {
      Join(newLeftLocalAggregate, newRightLocalAggregate, Inner, joinCondition, joinHint)
    } else if (shouldPushThroughLeft) {
      Join(newLeftLocalAggregate, join.right, Inner, joinCondition, joinHint)
    } else {
      Join(join.left, newRightLocalAggregate, Inner, joinCondition, joinHint)
    }
    (Some(newJoin), shouldPushThroughLeft, shouldPushThroughRight)
  }

  private def getAggregateToProjectMap(
      realAggExprsLeft: Seq[AggregateExpression],
      realAggExprsRight: Seq[AggregateExpression],
      realAggExprsToAliasMapLeft: Map[AggregateExpression, Alias],
      realAggExprsToAliasMapRight: Map[AggregateExpression, Alias],
      shouldPushThroughLeft: Boolean,
      shouldPushThroughRight: Boolean,
      leftAggregateCountAttribute: Option[Attribute],
      rightAggregateCountAttribute: Option[Attribute])
  : (Map[AggregateExpression, Expression], Map[AggregateExpression, Expression]) = {

    def getPushdownAggToProjectMap(
        realAggExprs: Seq[AggregateExpression],
        realAggExprsToAliasMap: Map[AggregateExpression, Alias],
        countFromOtherSide: Option[Attribute]) = {
      realAggExprs.map { agExp =>
        agExp -> resultOfAggExpAfterJoinForPushdown(agExp,
          realAggExprsToAliasMap(agExp),
          countFromOtherSide)
      }.toMap
    }

    def getNonPushdownAggToProjectMap(
        realAggExprs: Seq[AggregateExpression],
        countFromOtherSide: Option[Attribute]) = {
      realAggExprs.map { agExp =>
        agExp -> resultOfAggExpAfterJoinForNonPushdown(agExp,
          countFromOtherSide)
      }.toMap
    }

    val countOnRight = if (shouldPushThroughRight) {
      rightAggregateCountAttribute
    } else {
      None
    }
    val countOnLeft = if (shouldPushThroughLeft) {
      leftAggregateCountAttribute
    } else {
      None
    }

    val aggregateToProjectMapForPushDown = {
      {
        if (shouldPushThroughLeft) {
          getPushdownAggToProjectMap(realAggExprsLeft, realAggExprsToAliasMapLeft, countOnRight)
        } else {
          Map.empty[AggregateExpression, Expression]
        }
      } ++ {
        if (shouldPushThroughRight) {
          getPushdownAggToProjectMap(realAggExprsRight, realAggExprsToAliasMapRight, countOnLeft)
        } else {
          Map.empty[AggregateExpression, Expression]
        }
      }
    }

    val aggregateToProjectMapForNonPushDown = {
      {
        if (!shouldPushThroughLeft) {
          getNonPushdownAggToProjectMap(realAggExprsLeft, rightAggregateCountAttribute)
        } else {
          Map.empty[AggregateExpression, Expression]
        }
      } ++ {
        if (!shouldPushThroughRight) {
          getNonPushdownAggToProjectMap(realAggExprsRight, leftAggregateCountAttribute)
        } else {
          Map.empty[AggregateExpression, Expression]
        }
      }
    }
    (aggregateToProjectMapForPushDown, aggregateToProjectMapForNonPushDown)
  }

  private def resultOfAggExpAfterJoinForPushdown(
      aggExp: AggregateExpression,
      pushedDownAlias: Alias,
      countFromOtherSide: Option[Attribute]): Expression = {
    val result = aggExp.aggregateFunction match {
      case _: Sum | _: Count if countFromOtherSide.isDefined =>
        val right = pushedDownAlias.toAttribute
        val left = castIfNeeded(countFromOtherSide.get, right.dataType)
        Multiply(left, right)
      case _ =>
        pushedDownAlias.toAttribute
    }
    result
  }

  /**
   * This method is mainly for creating project list after join for AggregateExpressions
   * which are not pushed down below join
   * This will return child for aggregate expression function other than sum/count.
   * For sum this will return partial_count_otherside * col
   * For count this will return partial_count_side
   */
  private def resultOfAggExpAfterJoinForNonPushdown(
      aggExp: AggregateExpression,
      countFromOtherSide: Option[Attribute]): Expression = {
    // Ideally this check should never fail as pushDownLocalAggregateThroughJoin makes sure
    // that countFromOtherSide is non empty when aggExp is Sum/Count
    if (aggregateFunctionNeedsCountFromOtherSide(aggExp) && countFromOtherSide.isEmpty) {
      throw new SparkException("Count from other side should be present")
    }
    val result = aggExp.aggregateFunction match {
      case sum: Sum =>
        val partialCount = castIfNeeded(countFromOtherSide.get, sum.child.dataType)
        Multiply(partialCount, sum.child)
      case _: Count =>
        countFromOtherSide.get
      case other =>
        other.children.head
    }
    result
  }

  // whether the references in exp belongs to output of side
  private[preaggregation] def expressionPushableToPlan(
      exp: Expression,
      side: LogicalPlan): Boolean = {
    exp.references.subsetOf(side.outputSet)
  }

  /**
   * checks whether all the aggregateExpressions of Aggregate have Aggregate functions
   * which are pushable. Currently wre only support Sum, Count, min, Max
   */
  private[preaggregation] def hasValidAggregateFunctions(aggregate: Aggregate): Boolean = {
    aggregate.aggregateExpressions.forall { exp =>
      exp.collect {
        case aggExp: AggregateExpression => aggExp.aggregateFunction
      }.forall {
        case _: Sum | _: Count | _: Max | _: Min => true
        case _ => false
      }
    }
  }

  /**
   * Checks whether all the aggregate and grouping expressions are deterministic.
   */
  private[preaggregation] def isDeterministic(aggregate: Aggregate): Boolean = {
    aggregate.groupingExpressions.forall(_.deterministic) &&
      aggregate.aggregateExpressions.forall(_.deterministic)
  }

  /**
   * Checks whether an Aggregate is splittable into Aggregate+LocalAggregate
   * - checks that all aggregate functions are supported by preaggregation
   * - checks that plan is not a streaming plan
   * - checks that all expressions inside aggregate are deterministic
   * - checks that all grouping Expressions points to single reference
   * - checks that all aggregate functions are splittable Ex - Sum/Min/Max/Count
   * - checks that none of the Aggregate Expression has Distinct or GroupedAggPandasUDF
   */
  private[preaggregation] def validateAggregateIsSplittable(aggregate: Aggregate): Boolean = {
    if (aggregate.isStreaming || !isDeterministic(aggregate)
      || !hasValidAggregateFunctions(aggregate)) {
      return false
    }
    val groupingExpReferences = aggregate.groupingExpressions.map(_.references)
    val groupingExpAreSingleAttributes = groupingExpReferences.forall(_.size == 1)
    if (!groupingExpAreSingleAttributes) {
      return false
    }

    val unsupportedAggregateExpressionsPresent = aggregate.aggregateExpressions.exists { expr =>
      expr.collect {
        case exp: AggregateExpression if exp.isDistinct => exp
        case udf: PythonUDF if PythonUDF.isGroupedAggPandasUDF(udf) =>
          // Aggregate Expression might have grouped Pandas UDF which are handled in a very
          // different way as compared to normal Aggregate. They are handles using
          // [[AggregateInPandasExec]] class.
          udf
      }.nonEmpty
    }
    !unsupportedAggregateExpressionsPresent
  }

  // checks whether the sizeInBytes of a plan is within autoBroadcastJoinThreshold
  private def canBroadcastBySize(plan: LogicalPlan): Boolean = {
    plan.stats.sizeInBytes >= 0 &&
      plan.stats.sizeInBytes <= SQLConf.get.autoBroadcastJoinThreshold
  }

  // checks whether broadcast hint is present in stats of plan
  private def canBroadcastByHints(plan: LogicalPlan): Boolean = {
     plan.stats.hints.broadcast
  }

  // checks if the plan can be broadcasted using hints or size
  private[preaggregation] def canBroadcast(plan: LogicalPlan): Boolean = {
    canBroadcastByHints(plan) || canBroadcastBySize(plan)
  }

  /**
   * Rewrites all the AggregateExpressions present in expression assuming a pushdown
   * has happened.
   * Suppose an expression was:   2*Count(x#2)
   * This will be converted to 2*Sum(partial_x#43)
   * where partial_x#43 is the result of pushing down the Count below
   */
  private[preaggregation] def rewriteUpperAggregateExpressionAfterPushdown(
      expression: Expression,
      inputToUpperAggregateExpressionMap: Map[AggregateExpression, Expression]): Expression = {
    expression.transformUp {
      case ag: AggregateExpression if inputToUpperAggregateExpressionMap.contains(ag) =>
        val resultExprFromUnderlyingLocalAggregate = inputToUpperAggregateExpressionMap(ag)
        val newAgFunction = ag.aggregateFunction match {
          case _: Count =>
            Sum(resultExprFromUnderlyingLocalAggregate)
          case o =>
            o.withNewChildren(Seq(resultExprFromUnderlyingLocalAggregate))
              .asInstanceOf[AggregateFunction]
        }
        castIfNeeded(ag.copy(aggregateFunction = newAgFunction), ag.dataType)
    }
  }

  // Casts an expression if it doesn't has the requiredDataType
  private[preaggregation] def castIfNeeded(
      exp: Expression,
      requiredDataType: DataType): Expression = {
    if (!exp.dataType.sameType(requiredDataType)) {
      Cast(exp, requiredDataType)
    } else {
      exp
    }
  }

  private[preaggregation] def getNamedGroupingExpressions(
      groupingExpressions: Seq[Expression]): Seq[NamedExpression] = {
    groupingExpressions.zipWithIndex.map { case (exp, index) =>
      exp match {
        case exp: NamedExpression => exp
        case exp: Expression => Alias(exp, s"groupExp$index")()
      }
    }
  }

  /**
   * Aliases a given Seq of aggregateExpressions and returns a map from
   * aggregate Expression to new Alias
   */
  private[preaggregation] def createAliasesForPushedDownLocalAggregate(
      aggregateExpressions: Seq[AggregateExpression]): Map[AggregateExpression, Alias] = {
    aggregateExpressions.map { aggExp =>
      aggExp -> Alias(aggExp, aggExp.resultAttribute.name)()
    }.toMap
  }

  /**
   * Returns whether the given aggregate expression needs count for
   * reduction after pushdown through join
   */
  private[preaggregation] def aggregateFunctionNeedsCountFromOtherSide(
      agExp: AggregateExpression): Boolean = {
    agExp.aggregateFunction match {
      case _: Sum | _: Count => true
      case _ => false
    }
  }

  /**
   * Returns all the aggregate expressions from a given list of expressions
   * Ex - Input = Seq( 2*Sum(x), Sum(y)/Count(z) )
   * Output - Sum(x), Sum(y), Count(z)
   */
  private[preaggregation] def getAllAggregateExpressions(
      exps: Seq[Expression]): Seq[AggregateExpression] = {
    exps.flatMap { exp =>
      exp.collect { case ag: AggregateExpression => ag }
    }
  }
}
