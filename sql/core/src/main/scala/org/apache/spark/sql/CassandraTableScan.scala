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

import com.datastax.driver.core.{Row => CassandraRow}
import com.tuplejump.calliope.CasBuilder
import com.tuplejump.calliope.Implicits._
import com.tuplejump.calliope.stargate.JsonMapping.{BooleanCondition, Condition, MatchCondition, RangeCondition}
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.execution.LeafNode

case class CassandraTableScan(
                               // note: output cannot be transient, see
                               // https://issues.apache.org/jira/browse/SPARK-1367
                               output: Seq[Attribute],
                               relation: CassandraRelation,
                               filters: Seq[Expression]) extends LeafNode with Logging {

  override def execute(): RDD[Row] = {

    implicit val cassandraRow2sparkRow: CassandraRow => Row = {
      row =>
        new GenericRow(CassandraSparkDataConvertor.build(row, output))
    }

    val queryToUse: String = relation.stargateIndex match {
      case Some(idxColumn) => buildStargateQuery(idxColumn)
      case None => buildCassandraQuery
    }

    logInfo(s"Generated CQL: $queryToUse")

    val cas = CasBuilder.native
      .withColumnFamilyAndQuery(relation.keyspace, relation.table, queryToUse)
      .onHost(relation.host)
      .onPort(relation.rpcPort)
      .onNativePort(relation.nativePort)
      .mergeRangesInMultiRangeSplit(256)

    sqlContext.sparkContext.nativeCassandra[Row](cas)
  }

  private def buildStargateQuery(idxColumn: String): String = {
    val baseQuery: String = buildBaseQuery

    val queryToUse = if (filters.length <= 0) {
      baseQuery
    } else {
      val simplePredicates = filters.map(predicateToCondition)

      val condition = BooleanCondition(must = simplePredicates.toList, should = List.empty[Condition], not = List.empty[Condition])

      //println(condition.toJson.to)

      val stargateQuery = condition.toJson.toString()

      s"$baseQuery AND $idxColumn = '{ filter: $stargateQuery }'"
    } + " ALLOW FILTERING"

    logInfo(s"Querying with: $queryToUse")
    queryToUse
  }

  private val predicateToCondition: Expression => Condition = {
    case p@EqualTo(left: NamedExpression, right: Literal) =>
      MatchCondition(field = left.name, value = right.value)

    case p@EqualTo(Cast(left: NamedExpression, _), right: Literal) =>
      MatchCondition(field = left.name, value = right.value)

    case p@LessThan(left: NamedExpression, right: Literal) =>
      RangeCondition(field = left.name, upper = Some(right.value), lower = None)

    case p@LessThan(Cast(left: NamedExpression, _), right: Literal) =>
      RangeCondition(field = left.name, upper = Some(right.value), lower = None)

    case p@GreaterThan(left: NamedExpression, right: Literal) =>
      RangeCondition(field = left.name, lower = Some(right.value), upper = None)

    case p@GreaterThan(Cast(left: NamedExpression, _), right: Literal) =>
      RangeCondition(field = left.name, lower = Some(right.value), upper = None)

    case p@LessThanOrEqual(left: NamedExpression, right: Literal) =>
      RangeCondition(field = left.name, upper = Some(right.value), lower = None, includeUpper = true)

    case p@LessThanOrEqual(Cast(left: NamedExpression, _), right: Literal) =>
      RangeCondition(field = left.name, upper = Some(right.value), lower = None, includeUpper = true)

    case p@GreaterThanOrEqual(left: NamedExpression, right: Literal) =>
      RangeCondition(field = left.name, lower = Some(right.value), upper = None, includeLower = true)

    case p@GreaterThanOrEqual(Cast(left: NamedExpression, _), right: Literal) =>
      RangeCondition(field = left.name, lower = Some(right.value), upper = None, includeLower = true)

    case p@Or(left: Expression, right: Expression) =>
      BooleanCondition(must = List.empty[Condition],
        should = List(predicateToCondition(left), predicateToCondition(right)),
        not = List.empty[Condition])

    case p@And(left: Expression, right: Expression) =>
      BooleanCondition(should = List.empty[Condition],
        must = List(predicateToCondition(left), predicateToCondition(right)),
        not = List.empty[Condition])

    case p@Not(left: Expression) =>
      BooleanCondition(must = List.empty[Condition],
        should = List.empty[Condition],
        not = List(predicateToCondition(left)))

    case p@In(left: NamedExpression, right: Seq[Literal @unchecked]) =>
      BooleanCondition(should = right.map(r => MatchCondition(field = left.name, value = r.value)).toList,
        must = List.empty[Condition],
        not = List.empty[Condition])
  }

  private def isGtLt: Expression => Boolean = {
    case p: GreaterThan => true
    case p: LessThan => true
    case p: LessThanOrEqual => true
    case p: GreaterThanOrEqual => true
    case _ => false
  }

  private def buildCassandraQuery: String = {
    val baseQuery: String = buildBaseQuery

    val queryToUse = if (filters.length <= 0) {
      baseQuery
    } else {
      val filterString = filters.map {
        case p@EqualTo(left: NamedExpression, right: Literal) => Some(buildQueryString("=", left, right))
        case p@EqualTo(Cast(left: NamedExpression, _), right: Literal) => Some(buildQueryString("=", left, right))
        case p@LessThan(left: NamedExpression, right: Literal) => Some(buildQueryString("<", left, right))
        case p@LessThan(Cast(left: NamedExpression, _), right: Literal) => Some(buildQueryString("<", left, right))
        case p@GreaterThan(left: NamedExpression, right: Literal) => Some(buildQueryString(">", left, right))
        case p@GreaterThan(Cast(left: NamedExpression, _), right: Literal) => Some(buildQueryString(">", left, right))
        case p@LessThanOrEqual(left: NamedExpression, right: Literal) => Some(buildQueryString("<=", left, right))
        case p@LessThanOrEqual(Cast(left: NamedExpression, _), right: Literal) => Some(buildQueryString("<=", left, right))
        case p@GreaterThanOrEqual(left: NamedExpression, right: Literal) => Some(buildQueryString(">=", left, right))
        case p@GreaterThanOrEqual(Cast(left: NamedExpression, _), right: Literal) => Some(buildQueryString(">=", left, right))
      }.filter(_.isDefined).map(_.get).mkString(" AND ")

      s"$baseQuery AND $filterString"
    } + " ALLOW FILTERING"
    queryToUse
  }

  private def buildBaseQuery: String = {
    val projection = if (output.isEmpty) "*"
    else {
      (output.map(_.name).toList ++ relation.partitionKeys).distinct.mkString(",")
    }

    val keyString: String = relation.partitionKeys.mkString(",")

    val baseQuery = s"SELECT ${projection} FROM ${relation.keyspace}.${relation.table} WHERE token($keyString) > ? AND token($keyString) < ?"
    baseQuery
  }

  private def buildQueryString(comparatorSign: String, expr: NamedExpression, literal: Literal): String = {
    literal.dataType match {
      case BooleanType =>
        s"${expr.name} ${comparatorSign} ${literal.value.asInstanceOf[Boolean]}"
      case IntegerType =>
        s"${expr.name} ${comparatorSign} ${literal.value.asInstanceOf[Integer]}"
      case LongType =>
        s"${expr.name} ${comparatorSign} ${literal.value.asInstanceOf[Long]}"
      case DoubleType =>
        s"${expr.name} ${comparatorSign} ${literal.value.asInstanceOf[Double]}"
      case FloatType =>
        s"${expr.name} ${comparatorSign} ${literal.value.asInstanceOf[Float]}"
      case StringType =>
        s"${expr.name} ${comparatorSign} '${literal.value.asInstanceOf[String]}'"
      case _ =>
        s"${expr.name} ${comparatorSign} '${literal.value.asInstanceOf[String]}'"
    }
  }
}
