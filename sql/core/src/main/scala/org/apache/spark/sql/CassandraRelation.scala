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

import com.datastax.driver.core.{Cluster, KeyspaceMetadata, Metadata, TableMetadata, DataType => CassanndraDataType}
import com.tuplejump.calliope.sql.{CalliopeSqlSettings, CassandraSchemaHelper}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.LeafNode

import scala.collection.JavaConversions._

case class CassandraRelation(host: String, nativePort: String,
                             rpcPort: String,
                             keyspace: String,
                             table: String,
                             @transient sqlContext: SQLContext,
                             cassandraUsername: Option[String] = None,
                             cassandraPassword: Option[String] = None,
                             mayUseStartgate: Boolean = false,
                             @transient conf: Option[Configuration] = None)
  extends LeafNode with MultiInstanceRelation {

  @transient private[sql] val cassandraSchema: TableMetadata =
    CassandraSchemaHelper.getCassandraTableSchema(host, nativePort, keyspace, table, cassandraUsername, cassandraPassword)

  assert(cassandraSchema != null, s"Invalid Keyspace [$keyspace] or Table [$table] ")

  private[sql] val partitionKeys: List[String] = cassandraSchema.getPartitionKey.map(_.getName).toList

  private[sql] val clusteringKeys: List[String] = cassandraSchema.getClusteringColumns.map(_.getName).toList

  private[sql] val columns: Map[String, SerCassandraDataType] = cassandraSchema.getColumns.map{
    c => c.getName -> SerCassandraDataType.fromDataType(c.getType)
  }.toMap

  private val indexes: List[String] = cassandraSchema.getColumns.filter(_.getIndex != null).map(_.getName).toList

  override def newInstance() =
    new CassandraRelation(host,
      nativePort,
      rpcPort,
      keyspace,
      table,
      sqlContext,
      cassandraUsername,
      cassandraPassword,
      mayUseStartgate,
      conf).asInstanceOf[this.type]

  override val output: Seq[Attribute] = CassandraTypeConverter.convertToAttributes(cassandraSchema)

  private val isStargatePermitted = mayUseStartgate || (conf match {
    case Some(c) =>
      c.get(CalliopeSqlSettings.enableStargateKey) == "true" || c.get(s"calliope.stargate.$keyspace.$table.enable") == "true"
    case None => false
  })

  private[sql] val stargateIndex: Option[String] = if (isStargatePermitted) {
    cassandraSchema.getColumns.filter(_.getIndex != null).map(_.getIndex).collectFirst {
      case idx if (idx.isCustomIndex && idx.getIndexClassName == "com.tuplejump.stargate.RowIndex") =>
        idx.getIndexedColumn.getName
    }
  } else {
    None
  }

  def pushdownPredicates(filters: Seq[Expression]): PushdownFilters = {
    stargateIndex match {
      case Some(idxColumn) => StargatePushdownHandler.getPushdownFilters(filters)
      case None => CassandraPushdownHandler.getPushdownFilters(filters, partitionKeys, clusteringKeys, indexes)
    }
  }

  //TODO: Find better way of getting estimated result sizes from Cassandra
  override lazy val statistics: Statistics =
    Statistics(sizeInBytes = sqlContext.defaultSizeInBytes)
}
