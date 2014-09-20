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
import org.apache.spark.SparkContext

import com.tuplejump.calliope.Implicits._
import com.tuplejump.calliope.macros.NativeRowReader
import scala.language.implicitConversions
import com.datastax.driver.core.Row

/**
 * To run this test you need a Cassandra cluster up and running
 * and run the cql3test.cql in it to create the data.
 *
 */
class NativeCassandraRDDSpec extends FunSpec with BeforeAndAfterAll with ShouldMatchers with MustMatchers {

  val CASSANDRA_NODE_COUNT = 3
  val CASSANDRA_NODE_LOCATIONS = List("127.0.0.1", "127.0.0.2", "127.0.0.3")
  val TEST_KEYSPACE = "casSparkTest"
  val TEST_INPUT_COLUMN_FAMILY = "Words"

  info("Describes the functionality provided by the Cassandra RDD")

  val sc = new SparkContext("spark://127.0.0.1:7077", "nattest")
  sc.addJar("lib_managed/jars/org.apache.cassandra/cassandra-all/cassandra-all-2.0.9.jar")
  sc.addJar("lib_managed/jars/org.apache.cassandra/cassandra-thrift/cassandra-thrift-2.0.9.jar")
  sc.addJar("lib_managed/jars/org.apache.thrift/libthrift/libthrift-0.9.1.jar")
  sc.addJar("lib_managed/bundles/com.datastax.cassandra/cassandra-driver-core/cassandra-driver-core-2.0.4.jar")
  sc.addJar("target/scala-2.10/calliope_2.10-1.1.0-CTP-U1-SNAPSHOT.jar")
  //val sc = new SparkContext("local[1]", "nattest")

  describe("Native Cassandra RDD") {
    it("should be able to build and process RDD[U]") {
      /* val transformer = NativeRowReader.columnListMapper[NativeEmployee]("deptid", "empid", "first_name", "last_name")
      import transformer._ */

      implicit val transformer: Row => NativeEmployee = {
        r =>
          NativeEmployee(r.getInt("deptid"), r.getInt("empid"), r.getString("first_name"), r.getString("last_name"))
      }

      val cas = CasBuilder.native.withColumnFamilyAndKeyColumns("cql3_test", "emp_read_test", "deptid")


      val casrdd = sc.nativeCassandra[NativeEmployee](cas)

      casrdd.count() must equal(5)
    }

    /* it("should be able to build and process RDD[U] with multi range splits") {
      val transformer = NativeRowReader.columnListMapper[NativeEmployee]("deptid", "empid", "first_name", "last_name")
      import transformer._

      val cas = CasBuilder.native.withColumnFamilyAndKeyColumns("cql3_test", "emp_read_test", "deptid").mergeRangesInMultiRangeSplit(256)


      val casrdd = sc.nativeCassandra[NativeEmployee](cas)

      casrdd.count() must equal(5)
    }


    it("should be able to query selected columns") {

      implicit val Row2NameTuple: Row => (String, String) = { row: Row => (row.getString("first_name"), row.getString("last_name"))}

      val columnFamily: String = "emp_read_test"

      val token = "token(deptid)"
      val inputCql: String = s"select deptid, first_name, last_name from $columnFamily where $token > ? and $token <? allow filtering"

      val cas = CasBuilder.native.withColumnFamilyAndQuery("cql3_test", columnFamily, inputCql) //.mergeRangesInMultiRangeSplit(256)

      val casrdd = sc.nativeCassandra[(String, String)](cas)

      val result = casrdd.collect().toList

      result must have length (5)
      result should contain(("jack", "carpenter"))
      result should contain(("john", "grumpy"))

    }

    it("should be able to use secodary indexes") {
      val transformer = NativeRowReader.columnListMapper[NativeEmployee]("deptid", "empid", "first_name", "last_name")

      import transformer._

      val columnFamily = "emp_read_test"

      val token = "token(deptid)"

      val query = s"select * from $columnFamily where $token > ? and $token <? and first_name = 'john' allow filtering"
      val cas = CasBuilder.native.withColumnFamilyAndQuery("cql3_test", "emp_read_test", query) //.mergeRangesInMultiRangeSplit(256)

      val casrdd = sc.nativeCassandra[NativeEmployee](cas)

      val result = casrdd.collect().toList

      result must have length 1


      result should contain(NativeEmployee(20, 106, "john", "grumpy"))
      result should not contain (NativeEmployee(20, 105, "jack", "carpenter"))
    } */


  }

  override def afterAll() {
    sc.stop()
  }
}

case class NativeEmployee(deptId: Int, empId: Int, firstName: String, lastName: String)