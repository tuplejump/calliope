package com.tuplejump.calliope

import org.scalatest.{BeforeAndAfterAll, FunSpec}
import org.scalatest.matchers.{MustMatchers, ShouldMatchers}
import org.apache.spark.SparkContext
import java.nio.ByteBuffer
import java.util.UUID
import com.tuplejump.calliope.utils.RichByteBuffer
import RichByteBuffer._
import com.tuplejump.calliope.Implicits._
import CRDDFuncTransformers._
import Cql3CRDDTransformers._

class CassandraRDDFunctionsSpec extends FunSpec with BeforeAndAfterAll with ShouldMatchers with MustMatchers {

  val THRIFT_TEST_KEYSPACE = "casSparkTest"
  val THRIFT_TEST_OUTPUT_COLUMN_FAMILY = "TheLords"

  val CQL_TEST_KEYSPACE = "cql3_test"
  val CQL_TEST_OUTPUT_COLUMN_FAMILY = "emp_write_test"

  val sc = new SparkContext("local", "castest")

  describe("Cassandra RDD Function") {
    it("should allow persistence of any RDD to cassandra") {

      val data = List(
        ("Frodo", 24, "hobbit", "shire"),
        ("Samwise", 35, "hobbit", "shire"),
        ("Gandalf", 200, "wizard", "no one knows")
      )

      val rdd = sc.parallelize(data)

      val cas = CasBuilder.thrift.withColumnFamily(THRIFT_TEST_KEYSPACE, THRIFT_TEST_OUTPUT_COLUMN_FAMILY)

      rdd.thriftSaveToCassandra(cas)

      val casrdd = sc.thriftCassandra[String, (String, Int, String, String)](cas)

      val results = casrdd.map {
        case (k, v) => v
      }.collect()

      results.contains(("Frodo", 24, "hobbit", "shire")) must be(true)
    }

    it("should allow persistence using CQL") {

      val data = List(
        Employee(21, 110, "alan", "turing"),
        Employee(21, 111, "bjarne", "stroustrup"),
        Employee(22, 108, "charles", "babbage"),
        Employee(22, 102, "dennis", "ritchie")
      )

      val rdd = sc.parallelize(data)

      val cas = CasBuilder.cql3.withColumnFamily(CQL_TEST_KEYSPACE, CQL_TEST_OUTPUT_COLUMN_FAMILY)
        .saveWithQuery("UPDATE " + CQL_TEST_KEYSPACE + "." + CQL_TEST_OUTPUT_COLUMN_FAMILY +
        " set first_name = ?, last_name = ?")

      rdd.cql3SaveToCassandra(cas)

      val casrdd = sc.cql3Cassandra[Employee](cas)

      val result = casrdd.collect()

      result must have length (4)

      result should contain(Employee(21, 110, "alan", "turing"))

    }
  }

  override def afterAll() {
    sc.stop()
  }
}

private object CRDDFuncTransformers {

  import RichByteBuffer._

  implicit def rddToKey(x: (String, Int, String, String)): ByteBuffer = {
    UUID.nameUUIDFromBytes((x._1 + x._2 + x._3 + x._4).getBytes()).toString
  }

  implicit def lordsToColumns(x: (String, Int, String, String)): Map[ByteBuffer, ByteBuffer] = {
    Map[ByteBuffer, ByteBuffer](
      "name" -> x._1,
      "age" -> x._2,
      "tribe" -> x._3,
      "from" -> x._4
    )
  }

  implicit def columnsToLords(m: Map[ByteBuffer, ByteBuffer]): (String, Int, String, String) = {
    (m.getOrElse[ByteBuffer]("name", "NO_NAME"),
      m.getOrElse[ByteBuffer]("age", 0),
      m.getOrElse[ByteBuffer]("tribe", "NOT KNOWN"),
      m.getOrElse[ByteBuffer]("from", "a land far far away"))
  }

  implicit def EmployeeToKeys(e: Employee): Map[String, ByteBuffer] = {
    Map("deptid" -> e.deptId, "empid" -> e.empId)
  }

  implicit def EmployeeToVal(e: Employee): List[ByteBuffer] = {
    List(e.firstName, e.lastName)
  }

}
