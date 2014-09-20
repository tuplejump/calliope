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

package com.tuplejump.calliope

import com.tuplejump.calliope.Implicits._
import com.tuplejump.calliope.Types._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

/**
 * This class provides functions to save data from DStream to Cassandra
 * @param self
 * @tparam U
 */
class CassandraDStreamFunction[U](self: DStream[U]) {

  /**
   * Save incoming RDD to cassandra using configuration from the Cql3CasBuilder provided.
   * @param cas
   * @param keyMarshaller
   * @param rowMarshaller
   */
  def saveToCas(cas: Cql3CasBuilder)(implicit keyMarshaller: U => CQLRowKeyMap, rowMarshaller: U => CQLRowValues) {
    self.foreachRDD {
      rdd: RDD[U] => rdd.cql3SaveToCassandra(cas)
    }
  }

  /**
   * Save incoming RDD to the keyspace and columnfamily mentioned using the specified host as the seed node.
   * @param host
   * @param port
   * @param keyspace
   * @param columnFamily
   * @param keyCols
   * @param valueCols
   * @param marshaller
   * @return
   */
  def saveToCas(host: String, port: String, keyspace: String, columnFamily: String, keyCols: List[CQLKeyColumnName], valueCols: List[CQLColumnName])
               (implicit marshaller: U => CQLRowMap) {
    self.foreachRDD {
      rdd: RDD[U] => rdd.saveToCas(host, port, keyspace, columnFamily, keyCols, valueCols)
    }
  }


  /**
   * Save incoming RDD to the keyspace and columnfamily mentioned using the localhost as the seed node.
   * @param keyspace
   * @param columnFamily
   * @param keyCols
   * @param valueCols
   * @param marshaller
   * @return
   */
  def saveToCas(keyspace: String, columnFamily: String, keyCols: List[CQLKeyColumnName], valueCols: List[CQLColumnName])
               (implicit marshaller: U => CQLRowMap) {
    self.foreachRDD {
      rdd: RDD[U] => rdd.saveToCas(keyspace, columnFamily, keyCols, valueCols)
    }
  }
}
