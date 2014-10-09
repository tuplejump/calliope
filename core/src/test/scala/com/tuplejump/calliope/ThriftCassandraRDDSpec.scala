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

import org.scalatest.{BeforeAndAfterAll, FunSpec}
import org.scalatest.matchers.{MustMatchers, ShouldMatchers}
import java.nio.ByteBuffer
import com.tuplejump.calliope.utils.RichByteBuffer
import RichByteBuffer._
import org.apache.spark.SparkContext

import Implicits._
import com.tuplejump.calliope.Types.{CQLRowMap, CQLRowKeyMap, ThriftRowMap, ThriftRowKey}

/**
 * To run this test you need a Cassandra cluster up and running
 * and run the data-script.cli in it to create the data.
 *
 */
class ThriftCassandraRDDSpec extends FunSpec with BeforeAndAfterAll with ShouldMatchers with MustMatchers {

  val CASSANDRA_NODE_COUNT = 3
  val CASSANDRA_NODE_LOCATIONS = List("127.0.0.1", "127.0.0.2", "127.0.0.3")
  val TEST_KEYSPACE = "casSparkTest"
  val TEST_INPUT_COLUMN_FAMILY = "Words"


  info("Describes the functionality provided by the Cassandra RDD")

  val sc = new SparkContext("local[4]", "castest")

  describe("Thrift Cassandra RDD") {

    it("should be able to build and process RDD[K,V]") {
      val cas = CasBuilder.thrift.withColumnFamily(TEST_KEYSPACE, TEST_INPUT_COLUMN_FAMILY)

      val casrdd = sc.thriftCassandra[String, Map[String, String]](cas)
      //This is same as calling,
      //val casrdd = sc.cassandra[String, Map[String, String]](TEST_KEYSPACE, TEST_INPUT_COLUMN_FAMILY)

      val result = casrdd.collect().toMap

      val resultKeys = result.keys

      resultKeys must be(Set("3musk001", "thelostworld001", "3musk003", "3musk002", "thelostworld002"))

    }


    it("should be able to select certain columns from Cassandra to build RDD") {
      val cas = CasBuilder.thrift.withColumnFamily(TEST_KEYSPACE, TEST_INPUT_COLUMN_FAMILY).columns("book")

      val casrdd = sc.thriftCassandra[String, Map[String, String]](cas)
      //This is same as calling,
      //val casrdd = sc.cassandra[String, Map[String, String]](TEST_KEYSPACE, TEST_INPUT_COLUMN_FAMILY)

      val result = casrdd.take(1).head

      val resultValues = result._2.keySet

      resultValues.size must be(1)
    }


  }

  override def afterAll() {
    sc.stop()
  }
}

private object ThriftCRDDTransformers {

  import RichByteBuffer._

  implicit def row2String(key: ThriftRowKey, row: ThriftRowMap): List[String] = {
    row.keys.toList
  }

  implicit def cql3Row2Mapss(keys: CQLRowKeyMap, values: CQLRowMap): (Map[String, String], Map[String, String]) = {
    (keys, values)
  }
}