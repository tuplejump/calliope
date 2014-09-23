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

import com.tuplejump.calliope.sql.CalliopeSqlSettings
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.SparkLogicalPlan

class CassandraSchemaRDD(@transient _sqlContext: SQLContext,
                         @transient _baseLogicalPlan: LogicalPlan) extends SchemaRDD(_sqlContext, _baseLogicalPlan) {

  private val cassandraHost: String = sqlContext.sparkContext.getConf.get(CalliopeSqlSettings.cassandraHostKey, "127.0.0.1")

  private val cassandraNativePort: String = sqlContext.sparkContext.getConf.get(CalliopeSqlSettings.cassandraNativePortKey, "9042")

  private val cassandraRpcPort: String = sqlContext.sparkContext.getConf.get(CalliopeSqlSettings.cassandraRpcPortKey, "9160")

  private val cassandraUsername = sparkContext.getConf.getOption(CalliopeSqlSettings.cassandraUsernameKey)

  private val cassandraPassword = sparkContext.getConf.getOption(CalliopeSqlSettings.casssandraPasswordKey)

  @transient override protected[spark] val logicalPlan: LogicalPlan = baseLogicalPlan match {
    case _: SaveToCassandra =>
      queryExecution.toRdd
      SparkLogicalPlan(queryExecution.executedPlan)(sqlContext)
    // For various commands (like DDL) and queries with side effects, we force query optimization to
    // happen right away to let these side effects take place eagerly.
    case _: Command | _: InsertIntoTable | _: InsertIntoCreatedTable | _: WriteToFile =>
      queryExecution.toRdd
      SparkLogicalPlan(queryExecution.executedPlan)(sqlContext)
    case _ =>
      baseLogicalPlan
  }

  def this(schemaRdd: SchemaRDD) = this(schemaRdd.sqlContext, schemaRdd.baseLogicalPlan)

  /**
   *
   * @param keyspace
   * @param table
   */
  def saveToCassandra(keyspace: String, table: String): Unit = {
    saveToCassandra(cassandraHost, cassandraNativePort, cassandraRpcPort, keyspace, table)
  }

  /**
   *
   * @param host
   * @param nativePort
   * @param rpcPort
   * @param keyspace
   * @param table
   */
  def saveToCassandra(host: String, nativePort: String, rpcPort: String, keyspace: String, table: String): Unit = {
    sqlContext.executePlan(SaveToCassandra(host, nativePort, rpcPort, keyspace, table,
      cassandraUsername, cassandraPassword, logicalPlan)).toRdd
  }

  /**
   *
   * @param host
   * @param nativePort
   * @param rpcPort
   * @param keyspace
   * @param table
   * @param username
   * @param password
   */
  def saveToCassandra(host: String, nativePort: String, rpcPort: String, keyspace: String, table: String, username: String, password: String): Unit = {
    sqlContext.executePlan(SaveToCassandra(host, nativePort, rpcPort, keyspace, table,
      Some(username), Some(password), logicalPlan)).toRdd
  }
}

case class SaveToCassandra(host: String, nativePort: String, rpcPort: String,
                           keyspace: String, table: String, username: Option[String], password: Option[String],
                           child: LogicalPlan) extends UnaryNode {

  override def output = child.output
}

