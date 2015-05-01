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

package com.tuplejump.calliope.sql

import org.apache.spark.sql.{CassandraRelation, SchemaRDD, SQLContext}
import scala.collection.JavaConversions._


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

            self.catalog.unregisterTable(Seq(s"$ksName.$tableName"))
            casRdd.registerTempTable(s"$ksName.$tableName")

            logInfo(s"Registered C* table: $ksName.$tableName")
        }
      case _ => Nil
    }
  }

  if (loadCassandraTables) {
    allCassandraTables()
  }
}
