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

package org.apache.spark.sql.hive

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.GeneratePredicate
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{OutputFaker, SparkPlan}

import scala.collection.JavaConversions._

protected[sql] trait CassandraStrategies {
  self: SQLContext#SparkPlanner =>

  val hiveContext: CassandraAwareHiveContext

  /**
   * :: Experimental ::
   * Finds table scans that would use the Hive SerDe and replaces them with our own native Cassandra
   * table scan operator.
   *
   *
   * TODO: This code is based on ParquetConversion. Original comment from there: "Much of this logic is
   * duplicated in HiveTableScan.  Ideally we would do some refactoring but since this is after the
   * code freeze for 1.1 all logic is here to minimize disruption."
   *
   * Other issues:
   * - Much of this logic assumes case insensitive resolution.
   */
  @Experimental
  object CassandraConversion extends Strategy {

    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case PhysicalOperation(projectList, predicates, relation: MetastoreRelation)
        if relation.tableDesc.getSerdeClassName.contains("Cassandra") =>

        // We are going to throw the predicates and projection back at the whole optimization
        // sequence so lets unresolve all the attributes, allowing them to be rebound to the
        // matching cassandra attributes.
        val unresolvedOtherPredicates = predicates.map(_ transform {
          case a: AttributeReference => UnresolvedAttribute(a.name)
        }).reduceOption(And).getOrElse(Literal(true))

        val unresolvedProjection = projectList.map(_ transform {
          case a: AttributeReference => UnresolvedAttribute(a.name)
        })

          hiveContext
            .cassandraTable(relation.databaseName, relation.tableName)
            .where(unresolvedOtherPredicates)
            .select(unresolvedProjection: _*)
            .queryExecution
            .executedPlan :: Nil

      case _ => Nil
    }
  }

}
