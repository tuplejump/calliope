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
import com.tuplejump.calliope.Types.{CQLRowMap, CQLRowKeyMap, ThriftRowMap, ThriftRowKey}
import com.tuplejump.calliope.macros.CqlRowReader

import scala.language.implicitConversions
/**
 * To run this test you need a Cassandra cluster up and running
 * and run the data-script.cli in it to create the data.
 *
 */
class Cql3CassandraRDDSpec extends FunSpec with BeforeAndAfterAll with ShouldMatchers with MustMatchers {

  val CASSANDRA_NODE_COUNT = 3
  val CASSANDRA_NODE_LOCATIONS = List("127.0.0.1", "127.0.0.2", "127.0.0.3")
  val TEST_KEYSPACE = "casSparkTest"
  val TEST_INPUT_COLUMN_FAMILY = "Words"


  info("Describes the functionality provided by the Cassandra RDD")

  //val sc = new SparkContext("spark://127.0.0.1:7077", "nattest")
  val sc = new SparkContext("local[1]", "castest")

  describe("Cql3 Cassandra RDD") {
    it("should be able to build and process RDD[U]") {

      val transformer = CqlRowReader.columnListMapper[Employee]("deptid", "empid", "first_name", "last_name")

      import transformer._

      val cas = CasBuilder.cql3.withColumnFamily("cql3_test", "emp_read_test")

      val casrdd = sc.cql3Cassandra[Employee](cas)

      val result = casrdd.collect().toList

      result must have length (5)
      result should contain(Employee(20, 105, "jack", "carpenter"))
      result should contain(Employee(20, 106, "john", "grumpy"))
    }


    it("should be able to query selected columns") {
      import Cql3CRDDTransformers._

      val cas = CasBuilder.cql3.withColumnFamily("cql3_test", "emp_read_test").columns("first_name", "last_name")

      val casrdd = sc.cql3Cassandra[(String, String)](cas)

      val result = casrdd.collect().toList

      result must have length (5)
      result should contain(("jack", "carpenter"))
      result should contain(("john", "grumpy"))

    }

    it("should be able to use secodary indexes") {
      import Cql3CRDDTransformers._

      val cas = CasBuilder.cql3.withColumnFamily("cql3_test", "emp_read_test").where("first_name = 'john'")

      val casrdd = sc.cql3Cassandra[Employee](cas)

      val result = casrdd.collect().toList

      result must have length (1)
      result should contain(Employee(20, 106, "john", "grumpy"))

      result should not contain (Employee(20, 105, "jack", "carpenter"))
    }


  }

  override def afterAll() {
    sc.stop()
  }
}

object Cql3CRDDTransformers {

  import com.tuplejump.calliope.utils.RichByteBuffer._

  implicit def row2String(key: ThriftRowKey, row: ThriftRowMap): List[String] = {
    row.keys.toList
  }

  implicit def cql3Row2Emp(keys: CQLRowKeyMap, values: CQLRowMap): Employee =
    Employee(keys.get("deptid").get, keys.get("empid").get, values.get("first_name").get, values.get("last_name").get)

  implicit def cql3Row2EmpName(keys: CQLRowKeyMap, values: CQLRowMap): (String, String) =
    (values.get("first_name").get, values.get("last_name").get)
}

case class Employee(deptId: Int, empId: Int, firstName: String, lastName: String)