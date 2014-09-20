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
import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.analysis.{Catalog, SimpleCatalog}
import org.apache.spark.sql.catalyst.expressions._
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

object CalliopeSqlSettings {
  final val enableStargateKey: String = "calliope.stargate.enable"

  final val cassandraHostKey = "spark.cassandra.connection.host"

  final val cassandraNativePortKey = "spark.cassandra.connection.native.port"

  final val cassandraRpcPortKey = "spark.cassandra.connection.rpc.port"

  final val cassandraUsernameKey = "spark.cassandra.auth.username"

  final val casssandraPasswordKey = "spark.cassandra.auth.password"

  final val loadCassandraTablesKey = "spark.cassandra.auto.load.tables"
}

case class CassandraProperties(sparkContext: SparkContext) {
  val cassandraHost: String = sparkContext.getConf.get(CalliopeSqlSettings.cassandraHostKey, "127.0.0.1")

  val cassandraNativePort: String = sparkContext.getConf.get(CalliopeSqlSettings.cassandraNativePortKey, "9042")

  val cassandraRpcPort: String = sparkContext.getConf.get(CalliopeSqlSettings.cassandraRpcPortKey, "9160")

  val loadCassandraTables = sparkContext.getConf.getBoolean(CalliopeSqlSettings.loadCassandraTablesKey, false)

  val cassandraUsername = sparkContext.getConf.getOption(CalliopeSqlSettings.cassandraUsernameKey)

  val cassandraPassword = sparkContext.getConf.getOption(CalliopeSqlSettings.casssandraPasswordKey)

  val mayUseStargate = sparkContext.getConf.getBoolean(CalliopeSqlSettings.enableStargateKey, false)
}

trait CassandraAwareSQLContextFunctions {
  self: SQLContext =>

  val cassandraProperties = CassandraProperties(sparkContext)

  import cassandraProperties._

  /**
   * Create an SchemaRDD for the mentioned Cassandra Table using configured host and port
   * @param keyspace Keyspace to connect to
   * @param table Table to connect to
   * @return
   */
  def cassandraTable(keyspace: String, table: String): SchemaRDD = {
    cassandraTable(cassandraHost, cassandraNativePort, keyspace, table, false)
  }

  /**
   * Create an SchemaRDD for the mentioned Cassandra Table using configured host and port
   * @param keyspace Keyspace to connect to
   * @param table Table to connect to
   * @param mayUseStargate Should this SchemaRDD use Stargate for applying predicates
   * @return
   */
  def cassandraTable(keyspace: String, table: String, mayUseStargate: Boolean): SchemaRDD = {
    cassandraTable(cassandraHost, cassandraNativePort, keyspace, table, mayUseStargate)
  }

  /**
   * Create an SchemaRDD for the mentioned Cassandra Table
   * @param host Initial node in the cassandra cluster to connect to
   * @param port The Cassandra Native transport port
   * @param keyspace Keyspace to connect to
   * @param table Table to connect to
   * @param mayUseStargate Should this SchemaRDD use Stargate for applying predicates
   * @return
   */
  def cassandraTable(host: String, port: String, keyspace: String, table: String,
                     mayUseStargate: Boolean): SchemaRDD = {
    cassandraTable(host, port, keyspace, table, cassandraUsername, cassandraPassword, mayUseStargate)
  }

  /**
   * Create an SchemaRDD for the mentioned Cassandra Table
   * @param host Initial node in the cassandra cluster to connect to
   * @param port The Cassandra Native transport port
   * @param keyspace Keyspace to connect to
   * @param table Table to connect to
   * @param username Username of the user with access to Cassandra cluster
   * @param password Password of the user to connect to Cassandra
   * @param mayUseStargate Should this SchemaRDD use Stargate for applying predicates
   * @return
   */
  def cassandraTable(host: String, port: String, keyspace: String, table: String,
                     username: String, password: String,
                     mayUseStargate: Boolean): SchemaRDD = {
    cassandraTable(host, port, keyspace, table, Some(username), Some(password), mayUseStargate)
  }

  /*
   * Create an SchemaRDD for the mentioned Cassandra Table
   * @param host
   * @param port
   * @param keyspace
   * @param table
   * @param username
   * @param password
   * @param mayUseStargate
   * @return
   */
  def cassandraTable(host: String, port: String, keyspace: String, table: String,
                     username: Option[String], password: Option[String],
                     mayUseStargate: Boolean): SchemaRDD = {

    //Cassandra Thrift port is not used in this case
    new SchemaRDD(this,
      CassandraRelation(host,
        port,
        cassandraRpcPort,
        keyspace,
        table,
        self,
        username,
        password,
        mayUseStargate,
        Some(sparkContext.hadoopConfiguration)))
  }

  /**
   * Register all the Cassandra keyspace and tables with SparkSQL
   * @param host Host to initiate connection with
   * @param port Native Cassandra transport port
   * @param username Username of the user with access to Cassandra cluster
   * @param password Password of the user to connect to Cassandra
   * @param mayUseStargate Should we be using stargate index for data filtering
   */
  def allCassandraTables(host: String = cassandraHost, port: String = cassandraNativePort,
                         username: Option[String] = cassandraUsername, password: Option[String] = cassandraPassword,
                         mayUseStargate: Boolean = false) {

    val meta = CassandraSchemaHelper.getCassandraMetadata(host, port, username, password)
    meta.getKeyspaces.foreach {
      case keyspace if (!keyspace.getName.startsWith("system")) =>
        keyspace.getTables.foreach {
          table =>
            val ksName: String = keyspace.getName
            val tableName: String = table.getName
            val casRdd = cassandraTable(host, port, ksName, tableName, username, password, mayUseStargate)

            self.catalog.unregisterTable(None, s"$ksName.$tableName")
            self.catalog.registerTable(None, s"$ksName.$tableName", casRdd.logicalPlan)

            logInfo(s"Registered C* table: $ksName.$tableName")
        }
      case _ => Nil
    }
  }

  if (loadCassandraTables) {
    allCassandraTables()
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

protected[sql] trait CassandraCatalog extends Catalog {
  protected def context: SQLContext with CassandraAwareSQLContextFunctions

  abstract override def lookupRelation(databaseName: Option[String], tableName: String, alias: Option[String]): LogicalPlan = {
    val cassandraProperties = CassandraProperties(context.sparkContext)

    import cassandraProperties._

    databaseName match {
      case Some(dbname) =>
        val metadata = CassandraSchemaHelper.getCassandraMetadata(cassandraHost, cassandraNativePort, cassandraUsername, cassandraPassword)
        metadata.getKeyspace(dbname) match {
          case ksmeta: KeyspaceMetadata =>
            ksmeta.getTable(tableName) match {
              case tableMeta: TableMetadata =>
                context.cassandraTable(cassandraHost, cassandraNativePort, dbname, tableName, cassandraUsername, cassandraPassword, mayUseStargate).baseLogicalPlan
              case null =>
                super.lookupRelation(databaseName, tableName, alias)
            }
          case null =>
            super.lookupRelation(databaseName, tableName, alias)
        }
      case None =>
        //We cannot fetch a table without the keyspace name in cassandra
        super.lookupRelation(databaseName, tableName, alias)
    }
  }
}