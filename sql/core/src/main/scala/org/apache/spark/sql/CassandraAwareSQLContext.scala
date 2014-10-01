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

import com.datastax.driver.core.{DataType => CassandraDataType, TableMetadata, KeyspaceMetadata}
import com.tuplejump.calliope.sql.{StargateOptimizer, CassandraAwareSQLContextFunctions, CassandraProperties}
import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.analysis.{Catalog, SimpleCatalog}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan

import scala.collection.JavaConversions._

class CassandraAwareSQLContext(sc: SparkContext) extends SQLContext(sc) with CassandraAwareSQLContextFunctions {
  self =>

  override protected[sql] lazy val catalog: Catalog = new SimpleCatalog(true) with CassandraCatalog {
    override protected val context: SQLContext with CassandraAwareSQLContextFunctions = self
  }

  override protected[sql] val planner: SparkPlanner = new SparkPlanner with CassandraAwarePlanner {
    override val strategies: Seq[Strategy] =
      CommandStrategy(self) ::
        TakeOrdered ::
        HashAggregation ::
        LeftSemiJoin ::
        HashJoin ::
        InMemoryScans ::
        CassandraOperations ::
        ParquetOperations ::
        BasicOperators ::
        CartesianProduct ::
        BroadcastNestedLoopJoin :: Nil
  }
}
