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
import com.tuplejump.calliope.Implicits._
import com.tuplejump.calliope.Types.CQLRowMap
import org.apache.hadoop.mapreduce.SparkHadoopMapReduceUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{SparkPlan, UnaryNode}

case class WriteToCassandra(relation: CassandraRelation, child: SparkPlan) extends UnaryNode with SparkHadoopMapReduceUtil {


  private val columnMap: Map[String, Int] = child.output.zipWithIndex.map { case (a, i) => a.name -> i}.toMap

  override def execute(): RDD[Row] = {
    val childRdd = child.execute()

    val keyColumns = relation.partitionKeys ++ relation.clusteringKeys
    val columnsWithTypes: List[(String, SerCassandraDataType)] = relation.columns.filter(me => columnMap.contains(me._1)).toList
    val valueColumns = columnsWithTypes.map(_._1) diff keyColumns

    implicit val sparkRow2casRowMap: Row => CQLRowMap = {
      row =>
        columnsWithTypes.view.map {
          case (colName, sdtype) =>
            colName -> CassandraSparkDataConvertor.serializeValue(row(columnMap(colName)), sdtype)
        }.toMap
    }

    childRdd.saveToCas(relation.host, relation.rpcPort, relation.keyspace, relation.table, keyColumns, valueColumns)

    childRdd
  }

  override def output: Seq[Attribute] = child.output

  override def otherCopyArgs = sqlContext :: Nil

}
