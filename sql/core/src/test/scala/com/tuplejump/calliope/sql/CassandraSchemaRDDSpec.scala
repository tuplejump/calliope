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

import java.net.InetAddress
import java.sql.Timestamp
import java.util.{Date, UUID}

import org.apache.spark.SparkContext
import org.apache.spark.sql.CassandraAwareSQLContext
import org.scalatest.matchers.{MustMatchers, ShouldMatchers}
import org.scalatest.{BeforeAndAfterAll, FunSpec}

import com.tuplejump.calliope.Implicits._

class CassandraSchemaRDDSpec extends FunSpec with BeforeAndAfterAll with ShouldMatchers with MustMatchers {
  val TEST_KEYSPACE = "cql3_test"
  val TEST_READ_TABLE = "data_type_test"
  val TEST_WRITE_TABLE = "data_type_write_test"

  val sc = new SparkContext("local", "castest")
  val casContext = new CassandraAwareSQLContext(sc)

  import casContext._

  describe("Cassandra Schema RDD") {
    it("should persist a given SchemaRDD") {
      val schemaRdd = casContext.cassandraTable(TEST_KEYSPACE, TEST_READ_TABLE)

      schemaRdd.saveToCassandra(TEST_KEYSPACE, TEST_WRITE_TABLE)

      val readRdd = casContext.cassandraTable(TEST_KEYSPACE, TEST_WRITE_TABLE)

      readRdd.collect() should have size 4

      val records = List(
        UserRecord("b182f9cd-84ed-409a-ad59-8face7d675c4", "Arnold", "arnold@writersguild.com",
          36, 76.30f, true, "10.12.34.129", "A fighter", Set("San Fransisco", "New York").toSeq,
          Map("color" -> "fair", "hair" -> "salt and pepper"), List("MA", "MPhil").toSeq, 2350.45,
          "Some blob content".getBytes, new Timestamp(new Date().getTime), 23506, 23.45, 59.68),

        UserRecord("22e3d53b-ec0f-4717-bb39-ca98b32efd64", "Betty", "betty@writersguild.com",
          36, 76.30f, true, "10.12.34.130", "A fighter", Set("San Fransisco", "New York").toSeq,
          Map("color" -> "fair", "hair" -> "salt and pepper"), List("MA", "MPhil").toSeq, 2350.45,
          "Some blob content".getBytes, new Timestamp(new Date().getTime), 23506, 23.45, 59.68),

        UserRecord("117cbda4-ff6c-4942-9672-e295b31d9e16", "Cary", "cary@writersguild.com",
          36, 76.30f, true, "10.12.34.129", "A fighter", Set("San Fransisco", "New York").toSeq,
          Map("color" -> "fair", "hair" -> "salt and pepper"), List("MA", "MPhil").toSeq, 2350.45,
          "Some blob content".getBytes, new Timestamp(new Date().getTime), 23506, 23.45, 59.68)
      )

      val schemaRdd2 = sc.parallelize(records).toSchemaRDD

      schemaRdd2.saveToCassandra(TEST_KEYSPACE, TEST_WRITE_TABLE)

      val readRdd2 = casContext.cassandraTable(TEST_KEYSPACE, TEST_WRITE_TABLE)

      readRdd2.collect() should have size 7

    }
  }
}

case class UserRecord(user_id: String, name: String, email: String, age_in_years: Int, weight: Float,
                      is_married: Boolean, ipaddress: String, profile: String, places: Seq[String],
                      props: Map[String, String], degrees: Seq[String], salary: BigDecimal, some_blob: Array[Byte],
                      created_time: Timestamp, some_big_int: BigDecimal, latitude: Double, longitude: Double)
