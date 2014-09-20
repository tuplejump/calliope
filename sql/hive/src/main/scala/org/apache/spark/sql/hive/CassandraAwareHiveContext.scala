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

import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.analysis.{Catalog, OverrideCatalog}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, LowerCaseSchema}
import org.apache.spark.sql.{CassandraAwarePlanner, CassandraAwareSQLContextFunctions, CassandraCatalog, SQLContext}

class CassandraAwareHiveContext(sc: SparkContext) extends HiveContext(sc) with CassandraAwareSQLContextFunctions {
  self =>

  @transient
  override protected[sql] lazy val catalog = new HiveMetastoreCatalog(this) with HiveCassandraCatalog {
    override def lookupRelation(
                                 databaseName: Option[String],
                                 tableName: String,
                                 alias: Option[String] = None): LogicalPlan = {

      LowerCaseSchema(super.lookupRelation(databaseName, tableName, alias))
    }

    override protected val context: SQLContext with CassandraAwareSQLContextFunctions = self
  }

  @transient
  override val hivePlanner = new SparkPlanner with HiveStrategies with CassandraStrategies with CassandraAwarePlanner {
    val hiveContext = self

    override val strategies: Seq[Strategy] = Seq(
      CommandStrategy(self),
      //CassandraCommandStrategy(self),
      HiveCommandStrategy(self),
      TakeOrdered,
      CassandraOperations,
      ParquetOperations,
      InMemoryScans,
      CassandraConversion, // Must be before HiveTableScans
      ParquetConversion, // Must be before HiveTableScans
      HiveTableScans,
      DataSinks,
      Scripts,
      HashAggregation,
      LeftSemiJoin,
      HashJoin,
      BasicOperators,
      CartesianProduct,
      BroadcastNestedLoopJoin
    )
  }

  @transient
  override protected[sql] val planner = hivePlanner

  trait HiveCassandraCatalog extends OverrideCatalog with CassandraCatalog
}
