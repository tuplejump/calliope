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

package com.tuplejump.calliope.native

import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SerializableWritable, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import com.datastax.driver.core.Row
import com.tuplejump.calliope.{NativeCasBuilder, CasBuilder}
import scala.annotation.implicitNotFound

class NativeCassandraSparkContext(self: SparkContext) {

  /**
   *
   * @param host
   * @param port
   * @param keyspace
   * @param columnFamily
   * @param unmarshaller
   * @param tm
   * @tparam T
   * @return
   */
  @implicitNotFound(
    "No transformer found for Row => ${T}. You must have an implicit method defined of type Row => ${T}"
  )
  def nativeCassandra[T](host: String, port: String, keyspace: String, columnFamily: String, partitionColumns: List[String])
                        (implicit unmarshaller: Row => T,
                         tm: Manifest[T]): RDD[T] = {
    val cas = CasBuilder.native.withColumnFamilyAndKeyColumns(keyspace, columnFamily).onHost(host).onPort(port)
    this.nativeCassandra[T](cas)
  }

  /**
   *
   * @param keyspace
   * @param columnFamily
   * @param unmarshaller
   * @param tm
   * @tparam T
   * @return
   */
  @implicitNotFound(
    "No transformer found for Row => ${T}. You must have an implicit method defined of type Row => ${T}"
  )
  def nativeCassandra[T](keyspace: String, columnFamily: String, partitionColumns: List[String])
                        (implicit unmarshaller: Row => T, tm: Manifest[T]): RDD[T] = {
    val cas = CasBuilder.native.withColumnFamilyAndKeyColumns(keyspace, columnFamily, partitionColumns: _*)
    nativeCassandra[T](cas)(unmarshaller, tm)
  }


  /**
   *
   * @param cas
   * @param unmarshaller
   * @param tm
   * @tparam T
   * @return
   */
  @implicitNotFound(
    "No transformer found for Row => ${T}. You must have an implicit method defined of type Row => ${T}"
  )
  def nativeCassandra[T](cas: NativeCasBuilder)(implicit unmarshaller: Row => T, tm: Manifest[T]): RDD[T] = {
    val confBroadcast = self.broadcast(new SerializableWritable(cas.configuration))
    new NativeCassandraRDD[T](self, confBroadcast, unmarshaller)
  }

}
