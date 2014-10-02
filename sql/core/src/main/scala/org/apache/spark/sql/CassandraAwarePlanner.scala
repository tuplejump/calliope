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

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan

protected[sql] trait CassandraAwarePlanner {
  self: SQLContext#SparkPlanner =>

  object CassandraOperations extends Strategy {
    override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case PhysicalOperation(projectList, filters: Seq[Expression], relation: CassandraRelation) =>
        val pushdownFilters = relation.pushdownPredicates(filters)
        val scan: (Seq[Attribute]) => SparkPlan = CassandraTableScan(_, relation, pushdownFilters.filtersToPushdown)
        pruneFilterProject(projectList, filters, { f => pushdownFilters.filtersToRetain}, scan) :: Nil

      case SaveToCassandra(host, nativePort, rpcPort, keyspace, table, username, password, logicalPlan) =>
        val relation = CassandraRelation(host, nativePort, rpcPort, keyspace, table, sqlContext,
          username, password, false, Some(sparkContext.hadoopConfiguration))
        WriteToCassandra(relation, planLater(logicalPlan)) :: Nil

      case logical.InsertIntoTable(relation: CassandraRelation, partition, child, overwrite) =>
        WriteToCassandra(relation, planLater(child)) :: Nil

      case ops =>
        logInfo(s"Cassandra Operations doesn't handle $ops")
        Nil
    }

    def selectFilters(relation: CassandraRelation)(condition: Expression => Boolean): (Seq[Expression]) => Seq[Expression] = {
      filters =>
        filters.filter(condition)
    }
  }
}
