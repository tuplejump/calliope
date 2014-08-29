package com.tuplejump.calliope.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.CassandraQueryContext
import org.scalatest.matchers.{MustMatchers, ShouldMatchers}
import org.scalatest.{BeforeAndAfterAll, FunSpec}

class StargateAwareContextSpec extends FunSpec with BeforeAndAfterAll with ShouldMatchers with MustMatchers {

  private final val TEST_KEYSPACE = "cql3_test"
  private final val TEST_INPUT_COLUMN_FAMILY = "stargate_test"

  val sc = new SparkContext("local", "castest")

  val casContext = new CassandraQueryContext(sc)
  import casContext._

  describe("Stargate aware context") {
    it("should pushdown supported predicates to stargate"){
      val table = casContext.cassandraTable(TEST_KEYSPACE, TEST_INPUT_COLUMN_FAMILY, mayUseStargate = true)
        .select('post_pagename, 'user_agent)
        .where('user_agent === "Chrome")

      val plan = table.queryExecution

      table.printSchema()
      
      println(s"PLAN: $plan")

      table.collect().foreach(println)
    }
  }
}
