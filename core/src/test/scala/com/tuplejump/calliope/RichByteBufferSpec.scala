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

import java.nio.ByteBuffer
import org.apache.cassandra.utils.ByteBufferUtil
import org.scalatest.FunSpec
import org.scalatest.matchers.{MustMatchers, ShouldMatchers}
import com.tuplejump.calliope.utils.RichByteBuffer
import java.math.BigInteger
import org.joda.time.DateTime
import java.util.Date
import java.util.UUID
import java.net.InetAddress
import scala.reflect._


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

    def verifyNonEmptyOptionConversion[A](typeName: String, orig: A)(implicit s: Option[A] => ByteBuffer, d: ByteBuffer => Option[A]) = {
      it(s"should add implicit conversion of non-empty Option[${typeName}] to ByteBuffer and vise versa") {
        val bb: ByteBuffer = Some(orig)
        val copy: Option[A] = bb
        copy must equal(Some(orig))
      }
    }

    def verifyEmptyOptionConversion[A](typeName: String)(implicit s: Option[A] => ByteBuffer, d: ByteBuffer => Option[A]) =
      it(s"should add implicit conversion of empty Option[${typeName}] to ByteBuffer and vise versa") {
        val orig: Option[A] = None
        val bb: ByteBuffer = orig
        bb must be(null)
        val copy: Option[A] = bb
        copy must be(None)
      }

    def verifyOptionConversion[A](origValue: A)(implicit s: Option[A] => ByteBuffer, d: ByteBuffer => Option[A]) = {
      val typeName = origValue.getClass.getName

      verifyEmptyOptionConversion[A](typeName)
      verifyNonEmptyOptionConversion[A](typeName, origValue)
    }

    verifyOptionConversion[String]("foo")

    verifyOptionConversion[Boolean](true)

    verifyOptionConversion[Int](1)
    verifyOptionConversion[Long](1L)
    verifyOptionConversion[BigInteger](new BigInteger("1"))

    verifyOptionConversion[Float](1.1F)
    verifyOptionConversion[Double](1.1D)
    verifyOptionConversion[BigDecimal](BigDecimal(java.math.BigDecimal.ONE))

    verifyOptionConversion[Date](new Date())
    verifyOptionConversion[DateTime](new DateTime())

    verifyOptionConversion[UUID](UUID.randomUUID)

    verifyOptionConversion[InetAddress](InetAddress.getLocalHost)
  }
}
