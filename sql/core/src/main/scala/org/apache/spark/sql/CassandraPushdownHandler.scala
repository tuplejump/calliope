/*
 * Licensed to Tuplejump Software Pvt. Ltd. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Tuplejump Software Pvt. Ltd. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.spark.sql

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.expressions._

object CassandraPushdownHandler extends PushdownHandler {
  private[sql] def getPushdownFilters(filters: Seq[Expression], partitionKeys: List[String], clusteringKeys: List[String], allIndexes: List[String]): PushdownFilters = {
    val indexes: List[String] = allIndexes diff partitionKeys

    //Get the defined equal filters
    val equals = filters.map(mapEqualsToColumnNames).filter(_.isDefined).map(_.get)

    val equalFilters: Seq[String] = equals.map(_._1)
    //Get the clustering keys in the equal list
    val equalsOnClusteringKeys = clusteringKeys.intersect(equalFilters).toList

    val clusteringKeysToUse: List[String] = getClusterKeysToUse(clusteringKeys, equalsOnClusteringKeys)

    val equalIndexes: List[String] = equalFilters.intersect(indexes).toList

    val gtlts = filters.map(mapGtLtToColumnNames).filter(_.isDefined).map(_.get)

    val gtltFilters = gtlts.map(_._1)

    //Since we can use NON EQ indexes only if we have atleast one eq index check for that
    val nonEqIndexes: List[String] = if (equalIndexes.isEmpty) {
      List.empty[String]
    } else {
      gtltFilters.intersect(indexes).toList
    }

    // If a gtlt query is on clustering key immediately following the eq keys we can use that
    val nextClusteringKey = if (clusteringKeys.length > clusteringKeysToUse.length) Some(clusteringKeys(clusteringKeysToUse.length)) else None

    val gtltClusteringKey: List[String] = nextClusteringKey match {
      case Some(nck) => if (gtltFilters.contains(nck)) {
        gtltFilters.filter(_ == nck).toList
      } else {
        List.empty[String]
      }
      case None => List.empty[String]
    }

    var possibleFilters = (equals ++ gtlts)

    val pushdownExpr = (clusteringKeysToUse ++ gtltClusteringKey ++ equalIndexes ++ nonEqIndexes).distinct.flatMap {
      col =>
        possibleFilters.filter(_._1 == col)
    }.map(_._2)

    val retainExpr = filters.toList diff pushdownExpr

    PushdownFilters(pushdownExpr, retainExpr)
  }

  private def getClusterKeysToUse(clusteringKeys: List[String], filteredClusteringKeys: List[String], index: Int = 0): List[String] = {
    if (filteredClusteringKeys.isEmpty) {
      List.empty[String]
    } else {
      if (filteredClusteringKeys(0) != clusteringKeys(index)) {
        List.empty[String]
      } else {
        List(filteredClusteringKeys(0)) ::: getClusterKeysToUse(clusteringKeys, filteredClusteringKeys.tail, index + 1)
      }
    }
  }
}

case class PushdownFilters(filtersToPushdown: Seq[Expression], filtersToRetain: Seq[Expression])


object StargatePushdownHandler extends PushdownHandler with Logging {
  private[sql] def getPushdownFilters(filters: Seq[Expression]): PushdownFilters = {
    logInfo("Using STARGATE filter")
    val (pushdown, retain) = filters.partition(isSupportedQuery)

    logInfo(s"PUSHDOWN: $pushdown")

    PushdownFilters(pushdown, retain)
  }

  private val isSupportedQuery: Expression => Boolean = {
    case p: Expression if isSupportedSimpleQuery(p) => true
    case p: Expression if isSupportedComplexQuery(p) => true
    case _ => false
  }

  private val isSupportedComplexQuery: Expression => Boolean = {
    case p@Or(left: Expression, right: Expression) =>
      (isSupportedSimpleQuery(right) || isSupportedComplexQuery(right)) &&
        (isSupportedSimpleQuery(left) || isSupportedComplexQuery(left))

    case p@And(left: Expression, right: Expression) =>
      (isSupportedSimpleQuery(right) || isSupportedComplexQuery(right)) &&
        (isSupportedSimpleQuery(left) || isSupportedComplexQuery(left))

    case p@Not(left: Expression) =>
      isSupportedSimpleQuery(left) || isSupportedComplexQuery(left)

    case _ => false
  }

  private val isSupportedSimpleQuery: Expression => Boolean = {
    case p@EqualTo(left: NamedExpression, right: Literal) => true
    case p@EqualTo(Cast(left: NamedExpression, _), right: Literal) => true
    case p@LessThan(left: NamedExpression, right: Literal) => true
    case p@LessThan(Cast(left: NamedExpression, _), right: Literal) => true
    case p@LessThanOrEqual(left: NamedExpression, right: Literal) => true
    case p@LessThanOrEqual(Cast(left: NamedExpression, _), right: Literal) => true
    case p@GreaterThan(left: NamedExpression, right: Literal) => true
    case p@GreaterThan(Cast(left: NamedExpression, _), right: Literal) => true
    case p@GreaterThanOrEqual(left: NamedExpression, right: Literal) => true
    case p@GreaterThanOrEqual(Cast(left: NamedExpression, _), right: Literal) => true
    case p@In(left: NamedExpression, right: Seq[Literal@unchecked]) => true
    case _ => false
  }
}


private[sql] trait PushdownHandler {
  protected[sql] def mapEqualsToColumnNames: Expression => Option[(String, Expression)] = {
    case p@EqualTo(left: NamedExpression, right: Literal) => Some(left.name -> p)
    case p@EqualTo(Cast(left: NamedExpression, _), right: Literal) => Some(left.name -> p)
    case _ => None
  }

  protected[sql] def mapGtLtToColumnNames: Expression => Option[(String, Expression)] = {
    case p@LessThan(left: NamedExpression, right: Literal) => Some(left.name -> p)
    case p@LessThan(Cast(left: NamedExpression, _), right: Literal) => Some(left.name -> p)
    case p@LessThanOrEqual(left: NamedExpression, right: Literal) => Some(left.name -> p)
    case p@LessThanOrEqual(Cast(left: NamedExpression, _), right: Literal) => Some(left.name -> p)
    case p@GreaterThan(left: NamedExpression, right: Literal) => Some(left.name -> p)
    case p@GreaterThan(Cast(left: NamedExpression, _), right: Literal) => Some(left.name -> p)
    case p@GreaterThanOrEqual(left: NamedExpression, right: Literal) => Some(left.name -> p)
    case p@GreaterThanOrEqual(Cast(left: NamedExpression, _), right: Literal) => Some(left.name -> p)
    case _ => None
  }
}
