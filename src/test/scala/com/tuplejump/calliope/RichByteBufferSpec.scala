package com.tuplejump.calliope

import java.nio.ByteBuffer
import org.apache.cassandra.utils.ByteBufferUtil
import org.scalatest.FunSpec
import org.scalatest.matchers.{MustMatchers, ShouldMatchers}
import com.tuplejump.calliope.utils.RichByteBuffer

class RichByteBufferSpec extends FunSpec with ShouldMatchers with MustMatchers {
  describe("RichByteBuffer") {

    import RichByteBuffer._

    it("should should add implicit conversion of ByteBuffer to String") {
      val b = ByteBufferUtil.bytes("Test")

      val s: String = b
      s.length must be(4) //Should come from test
      "Test".equalsIgnoreCase(s) must be(true)
    }

    it("should add implicit conversion of ByteBuffer to Int") {
      val b: ByteBuffer = ByteBufferUtil.bytes(100)

      val i: Int = b
      100 / i must be(1)
    }

    it("should add implicit conversion of ByteBuffer to Double") {
      val b: ByteBuffer = ByteBufferUtil.bytes(100d)

      val d: Double = b
      300d - d must be(200d)
    }

    it("should add implicit conversion of ByteBuffer to Long") {
      val b: ByteBuffer = ByteBufferUtil.bytes(100l)

      val l: Long = b
      300l - l must be(200l)
    }

    it("should add implicit conversion between ByteBuffer and Boolean") {
      val btrue: ByteBuffer = true
      val vtrue: Boolean = btrue
      true must be(vtrue)

      val bfalse: ByteBuffer = false
      val vfalse: Boolean = bfalse
      false must be(vfalse)
    }

    it("should add implicit conversion between ByteBuffer and UUID") {
      import java.util.UUID
      val uuid = UUID.randomUUID()
      val uuidByteBuffer: ByteBuffer = uuid

      val vuuid: UUID = uuidByteBuffer

      uuid must be(vuuid)
    }


    it("should ease the conversion of list to case class") {
      case class Person(name: String, age: Int)
      val l: List[ByteBuffer] = List("Joey", 10)

      def list2Person(list: List[ByteBuffer]) = Person(list(0), list(1)) //One line boiler plate

      val p = list2Person(l)

      p.isInstanceOf[Person] must be(true)

      p.name must be("Joey")
      p.age must be(10)
    }

    it("should ease the conversion to typed Tuple") {
      val l: List[ByteBuffer] = List("Joey", 10)

      def list2Tuple2(list: List[ByteBuffer]) = new Tuple2[String, Int](list(0), list(1)) //One line boiler plate

      val p = list2Tuple2(l)

      p.isInstanceOf[Tuple2[_, _]] must be(true)

      p._1 must be("Joey")
      p._2 must be(10)
    }

    it("should add implicit conversion of Set[String] to ByteBuffer and vise versa") {
      val origSs = Set("foo", "bar")

      val bb: ByteBuffer = origSs

      val newSs: Set[String] = bb

      newSs must equal(origSs)
    }

    it("should add implicit conversion of List[String] to ByteBuffer and vise versa") {
      val origLs = List("foo", "bar")

      val bb: ByteBuffer = origLs

      val newLs: List[String] = bb

      newLs must equal(origLs)
    }

    it("should add implicit conversion of non-empty Option[String] to ByteBuffer and vise versa") {
      val orig = Some("foo")

      val bb: ByteBuffer = orig

      val copy: Option[String] = bb

      copy must equal(orig)
    }

    it("should add implicit conversion of empty Option[String] to ByteBuffer and vise versa") {
      val orig: Option[String] = None

      val bb: ByteBuffer = orig

      bb must be(null)

      val copy: Option[String] = bb

      copy must be(None)
    }

    it("should add implicit conversion of non-empty Option[Double] to ByteBuffer and vise versa") {
      val orig = Some(1D)

      val bb: ByteBuffer = orig

      val copy: Option[Double] = bb

      copy must equal(orig)
    }

    it("should add implicit conversion of empty Option[Double] to ByteBuffer and vise versa") {
      val orig: Option[Double] = None

      val bb: ByteBuffer = orig

      bb must be(null)

      val copy: Option[Double] = bb

      copy must be(None)
    }
  }
}
