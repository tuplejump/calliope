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

import com.datastax.driver.core.{DataType => CassandraDataType}
import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan

import scala.util.{Failure, Success, Try}

class CassandraAwareSQLContext(sc: SparkContext) extends SQLContext(sc) with CassandraAwareSQLContextFunctions {
  self =>
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

trait CassandraAwareSQLContextFunctions {
  self: SQLContext =>

  final val cassandraHostKey = "spark.cassandra.connection.host"

  final val cassandraNativePortKey = "spark.cassandra.connection.native.port"

  final val cassandraRpcPortKey = "spark.cassandra.connection.rpc.port"

  private val cassandraHost: String = Try(sparkContext.getConf.get(cassandraHostKey)) match {
    case Success(host) => host
    case Failure(ex) => "127.0.0.1"
  }

  private val cassandraNativePort: String = Try(sparkContext.getConf.get(cassandraNativePortKey)) match {
    case Success(port) => port
    case Failure(ex) => "9042"
  }

  private val cassandraRpcPort: String = Try(sparkContext.getConf.get(cassandraRpcPortKey)) match {
    case Success(port) => port
    case Failure(ex) => "9160"
  }

  def cassandraTable(keyspace: String, table: String): SchemaRDD = cassandraTable(cassandraHost, cassandraNativePort, keyspace, table)

  def cassandraTable(keyspace: String, table: String, mayUseStargate: Boolean): SchemaRDD = cassandraTable(cassandraHost, cassandraNativePort, keyspace, table, mayUseStargate)

  def cassandraTable(host: String, port: String, keyspace: String, table: String, mayUseStargate: Boolean = false): SchemaRDD = {
    //Cassaandra Thrift port is not used in this case
    new SchemaRDD(this, CassandraRelation(host, port, cassandraRpcPort, keyspace, table, Some(sparkContext.hadoopConfiguration), mayUseStargate))
  }
}

protected[sql] trait CassandraAwarePlanner {
  self: SQLContext#SparkPlanner =>

  object CassandraOperations extends Strategy {
    override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case PhysicalOperation(projectList, filters: Seq[Expression], relation: CassandraRelation) =>

        val pushdownFilters = relation.pushdownPredicates(filters)

        val scan: (Seq[Attribute]) => SparkPlan = CassandraTableScan(_, relation, pushdownFilters.filtersToPushdown)

        pruneFilterProject(projectList, filters, { f => pushdownFilters.filtersToRetain}, scan) :: Nil

      case SaveToCassandra(host, nativePort, rpcPort, keyspace, table, logicalPlan) =>
        val relation = CassandraRelation(host, nativePort, rpcPort, keyspace, table)
        WriteToCassandra(relation, planLater(logicalPlan)) :: Nil

      case _ => Nil
    }

    def selectFilters(relation: CassandraRelation)(condition: Expression => Boolean): (Seq[Expression]) => Seq[Expression] = {
      filters =>
        filters.filter(condition)
    }
  }

}

