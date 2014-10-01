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

import org.apache.hadoop.conf.Configuration
import org.apache.spark.annotation.Experimental
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{OutputFaker, SparkPlan}
import org.apache.spark.sql.hive.execution.NativeCommand

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



object CassandraConfConstants {
  val CASSANDRA_HOST = "cassandra.host";
  // initialHost
  val CASSANDRA_RPC_PORT = "cassandra.port.rpc";
  // rcpPort
  val CASSANDRA_NATIVE_PORT = "cassandra.port.native";
  // rcpPort
  val CASSANDRA_KEYSPACE_NAME = "cassandra.ks.name";
  // keyspace
  val CASSANDRA_KEYSPACE_REPFACTOR = "cassandra.ks.repfactor";
  //keyspace replication factor
  val CASSANDRA_CF_NAME = "cassandra.cf.name";
  // column family
  val CASSANDRA_USERNAME = "cassandra.username"
  val CASSANDRA_PASSWORD = "cassandra.password"

  val defaultHost = "127.0.0.1"
  val defaultRpcPort = "9160"
  val defaultNativePort = "9042"
}

case class CassandraProperties(properties: Map[String, String], configuration: Configuration) {
  lazy val cassandraHost = properties.get(CassandraConfConstants.CASSANDRA_HOST) match {
    case Some(host) => host
    case None =>
      if (configuration != null) {
        configuration.get(CassandraConfConstants.CASSANDRA_HOST, CassandraConfConstants.defaultHost)
      } else {
        CassandraConfConstants.defaultHost
      }
  }

  lazy val cassandraRpcPort = properties.get(CassandraConfConstants.CASSANDRA_NATIVE_PORT) match {
    case Some(rpcPort) => rpcPort
    case None =>
      if (configuration != null) {
        configuration.get(CassandraConfConstants.CASSANDRA_RPC_PORT, CassandraConfConstants.defaultRpcPort)
      } else {
        CassandraConfConstants.defaultRpcPort
      }
  }

  lazy val cassandraNativePort = properties.get(CassandraConfConstants.CASSANDRA_NATIVE_PORT) match {
    case Some(nativePort) => nativePort
    case None =>
      if (configuration != null) {
        configuration.get(CassandraConfConstants.CASSANDRA_NATIVE_PORT, CassandraConfConstants.defaultNativePort)
      } else {
        CassandraConfConstants.defaultNativePort
      }
  }

  lazy val cassandraUsername: Option[String] = properties.get(CassandraConfConstants.CASSANDRA_USERNAME) match {
    case Some(user) => Some(user)
    case None =>
      if (configuration != null) {
        val user = configuration.get(CassandraConfConstants.CASSANDRA_USERNAME, "")
        if (user.length == 0) {
          None
        } else {
          Some(user)
        }
      } else {
        None
      }
  }

  lazy val cassandraPassword: Option[String] = properties.get(CassandraConfConstants.CASSANDRA_PASSWORD) match {
    case Some(password) => Some(password)
    case None =>
      if (configuration != null) {
        val password = configuration.get(CassandraConfConstants.CASSANDRA_PASSWORD, "")
        if (password.length == 0) {
          None
        } else {
          Some(password)
        }
      } else {
        None
      }
  }
}

