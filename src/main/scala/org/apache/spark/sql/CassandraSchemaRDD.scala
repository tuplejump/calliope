package org.apache.spark.sql

import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.SparkLogicalPlan

import scala.util.{Failure, Success, Try}

case class CassandraSchemaRDD(@transient _sqlContext: SQLContext,
                              @transient _baseLogicalPlan: LogicalPlan) extends SchemaRDD(_sqlContext, _baseLogicalPlan) {


  final val cassandraHostKey = "spark.cassandra.connection.host"

  final val cassandraNativePortKey = "spark.cassandra.connection.native.port"

  final val cassandraRpcPortKey = "spark.cassandra.connection.rpc.port"

  private val cassandraHost: String = Try(sqlContext.sparkContext.getConf.get(cassandraHostKey)) match {
    case Success(host) => host
    case Failure(ex) => "127.0.0.1"
  }


  private val cassandraNativePort: String = Try(sqlContext.sparkContext.getConf.get(cassandraNativePortKey)) match {
    case Success(port) => port
    case Failure(ex) => "9042"
  }

  private val cassandraRpcPort: String = Try(sqlContext.sparkContext.getConf.get(cassandraRpcPortKey)) match {
    case Success(port) => port
    case Failure(ex) => "9160"
  }

  @transient override protected[spark] val logicalPlan: LogicalPlan = baseLogicalPlan match {
    case _: SaveToCassandra =>
      queryExecution.toRdd
      SparkLogicalPlan(queryExecution.executedPlan)
    // For various commands (like DDL) and queries with side effects, we force query optimization to
    // happen right away to let these side effects take place eagerly.
    case _: Command | _: InsertIntoTable | _: InsertIntoCreatedTable | _: WriteToFile =>
      queryExecution.toRdd
      SparkLogicalPlan(queryExecution.executedPlan)
    case _ =>
      baseLogicalPlan
  }

  def this(schemaRdd: SchemaRDD) = this(schemaRdd.sqlContext, schemaRdd.baseLogicalPlan)

  def saveToCassandra(keyspace: String, table: String): Unit = {
    saveToCassandra(cassandraHost, cassandraNativePort, cassandraRpcPort, keyspace, table)
  }

  def saveToCassandra(host: String, nativePort: String, rpcPort: String, keyspace: String, table: String): Unit = {
    sqlContext.executePlan(SaveToCassandra(host, nativePort, rpcPort, keyspace, table, logicalPlan)).toRdd
  }
}

case class SaveToCassandra(host: String, nativePort: String, rpcPort: String, keyspace: String, table: String, child: LogicalPlan) extends UnaryNode {
  override def references = Set.empty

  override def output = child.output
}

