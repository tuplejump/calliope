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

import org.apache.spark.SparkContext
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.{CassandraAwareSQLContext, CassandraTableScan, SchemaRDD}
import org.scalatest.matchers.{MustMatchers, ShouldMatchers}
import org.scalatest.{BeforeAndAfterAll, FunSpec}

class CassandraAwareSQLContextSpec extends FunSpec with BeforeAndAfterAll with ShouldMatchers with MustMatchers {

  private final val TEST_KEYSPACE = "cql3_test"
  private final val TEST_INPUT_COLUMN_FAMILY = "data_type_test"
  private final val TEST_CF_WITH_CLUSTERING_KEY = "emp_score_test"

  val sc = new SparkContext("local", "castest")

  val casContext = new CassandraAwareSQLContext(sc)
  import casContext._

  describe("Cassandra Query Context") {
    it("should create a SchemaRDD from Cassandra table and read the rows to it") {
      val ctable: SchemaRDD = casContext.cassandraTable(TEST_KEYSPACE, TEST_INPUT_COLUMN_FAMILY)
      val schemaString = """root
                           | |-- user_id: string (nullable = false)
                           | |-- age_in_years: integer (nullable = false)
                           | |-- created_time: timestamp (nullable = false)
                           | |-- degrees: array (nullable = false)
                           | |    |-- element: string (containsNull = true)
                           | |-- email: string (nullable = false)
                           | |-- ipaddress: string (nullable = false)
                           | |-- is_married: boolean (nullable = false)
                           | |-- latitude: double (nullable = false)
                           | |-- longitude: double (nullable = false)
                           | |-- name: string (nullable = false)
                           | |-- places: array (nullable = false)
                           | |    |-- element: string (containsNull = true)
                           | |-- profile: string (nullable = false)
                           | |-- props: map (nullable = false)
                           | |    |-- key: string
                           | |    |-- value: string (valueContainsNull = true)
                           | |-- salary: decimal (nullable = false)
                           | |-- some_big_int: decimal (nullable = false)
                           | |-- some_blob: binary (nullable = false)
                           | |-- weight: float (nullable = false)""".stripMargin
      ctable.printSchema()
      ctable.schemaString.trim should be(schemaString)
      val elist = ctable.collect().toList
      elist should have size (4)
    }

    it("should filter with 'where' on the SchemaRDD") {
      val filteredRdd: SchemaRDD = casContext.cassandraTable(TEST_KEYSPACE, TEST_INPUT_COLUMN_FAMILY).where('email like "j%@gmail.com")
      val elist = filteredRdd.collect()
      elist should have size (3)
    }

    it("should be able to register a cassandra table rdd as SparkSql Table") {
      val casRdd: SchemaRDD = casContext.cassandraTable(TEST_KEYSPACE, TEST_INPUT_COLUMN_FAMILY)
      casContext.registerRDDAsTable(casRdd, "people")
      val elist = casContext.sql("SELECT * FROM people WHERE email like 'j%@gmail.com'").collect()
      elist should have size (3)
    }

    it("should Cassandra Secondary indexes to filter the data when given a simple query") {
      val filteredRdd: SchemaRDD = casContext.cassandraTable(TEST_KEYSPACE, TEST_INPUT_COLUMN_FAMILY).where('email === "jill@gmail.com")
      val elist = filteredRdd.collect()
      elist should have size (1)
    }

    it("should use mix of Cassandra Secondary indexes and spark filter when needed") {
      val filteredRdd: SchemaRDD = casContext.cassandraTable(TEST_KEYSPACE, TEST_INPUT_COLUMN_FAMILY).where('name === "jack").where('age_in_years === 28)

      val plan: SparkPlan = filteredRdd.queryExecution.executedPlan
      plan.expressions should have size (1)
      plan.expressions.head.references.head.name should be("age_in_years")
      plan.children should have size (1)
      plan.children.head.getClass should be(classOf[CassandraTableScan])

      val pushedDown = plan.children.head.asInstanceOf[CassandraTableScan].filters
      pushedDown should have size (1)
      pushedDown.head.references.head.name should be("name")

      val elist = filteredRdd.collect()
      elist should have size (2)
    }

    it("should use multiple Cassandra Secondary indexes when possible") {
      val filteredRdd: SchemaRDD = casContext.cassandraTable(TEST_KEYSPACE, TEST_INPUT_COLUMN_FAMILY).where('name === "jack").where('weight === 63.45)

      val plan: SparkPlan = filteredRdd.queryExecution.executedPlan
      plan.getClass should be(classOf[CassandraTableScan])

      val pushedDown = plan.asInstanceOf[CassandraTableScan].filters
      pushedDown should have size (2)
      pushedDown.map(_.references.head.name) should equal(List("name", "weight"))

      val elist = filteredRdd.collect()
      elist should have size (2)
    }

    it("should use multiple Cassandra Secondary indexes with AND query in sql if possible") {
      val people2: SchemaRDD = casContext.cassandraTable(TEST_KEYSPACE, TEST_INPUT_COLUMN_FAMILY)
      people2.registerTempTable("people2")


      val filteredRdd = casContext.sql("SELECT * from people2 WHERE name = 'jack' AND weight = 63.45")
      val plan: SparkPlan = filteredRdd.queryExecution.executedPlan
      plan.getClass should be(classOf[CassandraTableScan])

      val pushedDown = plan.asInstanceOf[CassandraTableScan].filters
      pushedDown should have size (2)
      pushedDown.map(_.references.head.name) should equal(List("name", "weight"))

      val elist = filteredRdd.collect()
      elist should have size (2)
    }

    it("should not pushdown an OR query") {
      val people2: SchemaRDD = casContext.cassandraTable(TEST_KEYSPACE, TEST_INPUT_COLUMN_FAMILY)
      people2.registerTempTable("people2")
      val filteredRdd = casContext.sql("SELECT * from people2 WHERE name = 'jack' OR weight = 63.45")
      val plan: SparkPlan = filteredRdd.queryExecution.executedPlan
      plan.expressions should have size (1)
      plan.expressions.head.references.map(_.name) should be(List("name", "weight"))
      plan.children should have size (1)
      plan.children.head.getClass should be(classOf[CassandraTableScan])
      val pushedDown = plan.children.head.asInstanceOf[CassandraTableScan].filters
      pushedDown should have size (0)
      val elist = filteredRdd.collect()
      elist should have size (2)
    }

    it("should pushdown appropriate GT and LT queries when accompanied by an EQ query") {
      val people2: SchemaRDD = casContext.cassandraTable(TEST_KEYSPACE, TEST_INPUT_COLUMN_FAMILY)
      people2.registerTempTable("people3")
      val filteredRdd = casContext.sql("SELECT * from people3 WHERE name = 'jack' and age_in_years < 40 AND weight > 60.45")
      val plan: SparkPlan = filteredRdd.queryExecution.executedPlan
      plan.expressions should have size (1)
      plan.expressions.head.references.map(_.name) should be(List("age_in_years"))
      plan.children should have size (1)
      plan.children.head.getClass should be(classOf[CassandraTableScan])
      val pushedDown = plan.children.head.asInstanceOf[CassandraTableScan].filters
      pushedDown should have size (2)
      pushedDown.map(_.references.head.name) should equal(List("name", "weight"))
      val elist = filteredRdd.collect()
      elist should have size (2)
    }

    it("should not pushdown GT and LT queries when not accompanied by an EQ query") {
      val people2: SchemaRDD = casContext.cassandraTable(TEST_KEYSPACE, TEST_INPUT_COLUMN_FAMILY)
      people2.registerTempTable("people4")
      val filteredRdd = casContext.sql("SELECT * from people4 WHERE age_in_years < 40 AND weight > 60.45")
      val plan: SparkPlan = filteredRdd.queryExecution.executedPlan
      plan.expressions.head.references.map(_.name) should be(List("age_in_years", "weight"))
      plan.children should have size (1)
      plan.children.head.getClass should be(classOf[CassandraTableScan])
      val pushedDown = plan.children.head.asInstanceOf[CassandraTableScan].filters
      pushedDown should have size (0)
      val elist = filteredRdd.collect()
      elist should have size (4)
    }

    it("should pushdown an EQ on first clustering key column") {
      val empScore1: SchemaRDD = casContext.cassandraTable(TEST_KEYSPACE, TEST_CF_WITH_CLUSTERING_KEY)
      val devScores = empScore1.where('dept === "dev")
      val plan: SparkPlan = devScores.queryExecution.executedPlan
      plan.getClass should be(classOf[CassandraTableScan])
      val pushdown = plan.asInstanceOf[CassandraTableScan].filters
      pushdown.map(_.references.head.name) should equal(List("dept"))
      val devList = devScores.collect()
      devList should have size (6)
    }

    it("should not pushdown an EQ on second clustering key column if first is not filtered by an EQ") {
      val empScore1: SchemaRDD = casContext.cassandraTable(TEST_KEYSPACE, TEST_CF_WITH_CLUSTERING_KEY)
      val devScores = empScore1.where('year === 2001)
      val plan: SparkPlan = devScores.queryExecution.executedPlan
      plan.expressions.head.references.map(_.name) should be(List("year"))
      plan.children should have size (1)
      plan.children.head.getClass should be(classOf[CassandraTableScan])
      val pushdown = plan.children.head.asInstanceOf[CassandraTableScan].filters
      pushdown should have size (0)
      val devList = devScores.collect()
      devList should have size (6)
    }

    it("should pushdown EQ on clustering keys given when consequetive ones are given") {
      val empScore1: SchemaRDD = casContext.cassandraTable(TEST_KEYSPACE, TEST_CF_WITH_CLUSTERING_KEY)
      val devScores = empScore1.where('dept === "dev" && 'year === 2001)
      val plan: SparkPlan = devScores.queryExecution.executedPlan
      plan.getClass should be(classOf[CassandraTableScan])
      val pushdown = plan.asInstanceOf[CassandraTableScan].filters
      pushdown.map(_.references.head.name) should equal(List("dept", "year"))
      val devList = devScores.collect()
      devList should have size (3)
    }

    it("should pushdown EQ on clustering keys given when consequetive ones are given irrespective of ordering in query") {
      val empScore1: SchemaRDD = casContext.cassandraTable(TEST_KEYSPACE, TEST_CF_WITH_CLUSTERING_KEY)
      val devScores = empScore1.where('year === 2001 && 'dept === "dev")
      val plan: SparkPlan = devScores.queryExecution.executedPlan
      plan.getClass should be(classOf[CassandraTableScan])
      val pushdown = plan.asInstanceOf[CassandraTableScan].filters
      pushdown.map(_.references.head.name) should equal(List("dept", "year"))
      val devList = devScores.collect()
      devList should have size (3)
    }

    it("should pushdown EQ on partial clustering keys given when following ones are not given") {
      val empScore1: SchemaRDD = casContext.cassandraTable(TEST_KEYSPACE, TEST_CF_WITH_CLUSTERING_KEY)
      val devScores = empScore1.where('dept === "ict" && 'emp === "johnny@tuplejump.com")
      val plan: SparkPlan = devScores.queryExecution.executedPlan
      plan.expressions.head.references.map(_.name) should be(List("emp"))
      plan.children should have size (1)
      plan.children.head.getClass should be(classOf[CassandraTableScan])
      val pushdown = plan.children.head.asInstanceOf[CassandraTableScan].filters
      pushdown.map(_.references.head.name) should equal(List("dept"))
      val devList = devScores.collect()
      devList should have size (2)
    }

    it("should pushdown GTLT on a clustering key given when preceding ones filtered with EQ") {
      val empScore1: SchemaRDD = casContext.cassandraTable(TEST_KEYSPACE, TEST_CF_WITH_CLUSTERING_KEY)
      val devScores = empScore1.where('dept === "ict" && 'year > 2000 && 'emp === "johnny@tuplejump.com")
      val plan: SparkPlan = devScores.queryExecution.executedPlan
      plan.expressions.head.references.map(_.name) should be(List("emp"))
      plan.children should have size (1)
      plan.children.head.getClass should be(classOf[CassandraTableScan])
      val pushdown = plan.children.head.asInstanceOf[CassandraTableScan].filters
      pushdown.map(_.references.head.name) should equal(List("dept", "year"))
      val devList = devScores.collect()
      devList should have size (2)
    }

    it("should pushdown GTLT on a clustering key given when preceding ones filtered with EQ 2") {
      val empScore1: SchemaRDD = casContext.cassandraTable(TEST_KEYSPACE, TEST_CF_WITH_CLUSTERING_KEY)
      val devScores = empScore1.where('dept === "ict" && 'year === 2001 && 'emp > "i" && 'emp < "j~")
      val plan: SparkPlan = devScores.queryExecution.executedPlan
      plan.getClass should be(classOf[CassandraTableScan])
      val pushdown = plan.asInstanceOf[CassandraTableScan].filters
      pushdown.map(_.references.head.name) should equal(List("dept", "year", "emp", "emp"))
      val devList = devScores.collect()
      devList should have size (2)
    }

    it("should select only the requested columns") {
      val empScore2: SchemaRDD = casContext.cassandraTable(TEST_KEYSPACE, TEST_INPUT_COLUMN_FAMILY).select('user_id, 'age_in_years, 'is_married)
      val plan = empScore2.queryExecution.executedPlan
      println(plan)
      empScore2.printSchema()
      val schema = """root
                     | |-- user_id: string (nullable = false)
                     | |-- age_in_years: integer (nullable = false)
                     | |-- is_married: boolean (nullable = false)""".stripMargin
      empScore2.schemaString.trim should be(schema)
      empScore2.collect().foreach {
        r => r.size should be(3)
      }
    }
  }

  override def afterAll() {
    sc.stop()
  }
}
