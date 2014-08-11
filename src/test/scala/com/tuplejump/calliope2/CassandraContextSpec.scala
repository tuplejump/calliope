package com.tuplejump.calliope2

import org.apache.spark.SparkContext
import org.apache.spark.sql.{CassandraContext, SchemaRDD}
import org.scalatest.matchers.{MustMatchers, ShouldMatchers}
import org.scalatest.{BeforeAndAfterAll, FunSpec}

class CassandraContextSpec extends FunSpec with BeforeAndAfterAll with ShouldMatchers with MustMatchers {
  val TEST_KEYSPACE = "cql3_test"
  val TEST_INPUT_COLUMN_FAMILY = "data_type_test"

  val sc = new SparkContext("local", "castest")
  val casContext = new CassandraContext(sc)

  import casContext._

  describe("Cassandra SQL Context") {
    /* it("should create a SchemaRDD from Cassandra table and read the rows to it") {

      val ctable: SchemaRDD = casContext.cassandraTable(TEST_KEYSPACE, TEST_INPUT_COLUMN_FAMILY)


      val schemaString = """root
                           | |-- user_id: StringType
                           | |-- age_in_years: IntegerType
                           | |-- created_time: StringType
                           | |-- degrees: ArrayType[StringType]
                           | |-- email: StringType
                           | |-- ipaddress: StringType
                           | |-- is_married: BooleanType
                           | |-- latitude: DoubleType
                           | |-- longitude: DoubleType
                           | |-- name: StringType
                           | |-- places: ArrayType[StringType]
                           | |-- profile: StringType
                           | |-- props: MapType(StringType,StringType)
                           | |-- salary: DecimalType
                           | |-- some_big_int: DecimalType
                           | |-- some_blob: BinaryType
                           | |-- weight: FloatType
                           | """.stripMargin

      ctable.schemaString should be(schemaString)

      val elist = ctable.collect().toList


      elist should have size (4)
    } */

    /* it("should filter with 'where' on the SchemaRDD") {
      val filteredRdd: SchemaRDD = casContext.cassandraTable(TEST_KEYSPACE, TEST_INPUT_COLUMN_FAMILY).where('email like "j%@gmail.com")

      val elist = filteredRdd.collect()

      elist should have size (3)

    } */

    /* it("should be able to register a cassandra table rdd as SparkSql Table") {
      val casRdd: SchemaRDD = casContext.cassandraTable(TEST_KEYSPACE, TEST_INPUT_COLUMN_FAMILY)

      casContext.registerRDDAsTable(casRdd, "people")

      val elist = casContext.sql("SELECT * FROM people WHERE email like 'j%@gmail.com'").collect()

      elist should have size (3)
    } */

    it("should use Cassandra Clustering Key and Secondary indexes to filter the data if possible") {
      val filteredRdd: SchemaRDD = casContext.cassandraTable(TEST_KEYSPACE, TEST_INPUT_COLUMN_FAMILY).where('email === "jill@gmail.com")

      val elist = filteredRdd.collect()

      elist should have size (1)
    }
  }


  override def afterAll() {
    sc.stop()
  }
}
