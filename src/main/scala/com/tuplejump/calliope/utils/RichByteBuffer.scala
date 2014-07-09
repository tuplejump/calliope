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

package com.tuplejump.calliope.utils

import java.nio.ByteBuffer
import org.apache.cassandra.utils.ByteBufferUtil
import scala.language.implicitConversions
import java.util.Date
import java.util.UUID
import java.net.InetAddress
import com.datastax.driver.core.DataType
import java.math.BigInteger
import scala.collection.JavaConverters._
import org.joda.time.DateTime


object RichByteBuffer {

  /* ByteBuffer to Typed Objects */
  implicit def ByteBuffer2Int(buffer: ByteBuffer): Int = ByteBufferUtil.toInt(buffer)

  implicit def ByteBuffer2Double(buffer: ByteBuffer): Double = ByteBufferUtil.toDouble(buffer)

  implicit def ByteBuffer2Float(buffer: ByteBuffer): Float = ByteBufferUtil.toFloat(buffer)

  implicit def ByteBuffer2Long(buffer: ByteBuffer): Long = ByteBufferUtil.toLong(buffer)

  implicit def ByteBuffer2String(buffer: ByteBuffer): String = ByteBufferUtil.string(buffer)

  implicit def ByteBuffer2Boolean(buffer: ByteBuffer): Boolean = buffer.get() == 1

  implicit def ByteBuffer2Date(buffer: ByteBuffer): Date = new Date(ByteBufferUtil.toLong(buffer))

  implicit def ByteBuffer2DateTime(buffer: ByteBuffer): DateTime = new DateTime(ByteBufferUtil.toLong(buffer))

  implicit def ByteBuffer2UUID(buffer: ByteBuffer): UUID = new UUID(buffer.getLong(buffer.position()), buffer.getLong(buffer.position() + 8))

  implicit def ByteBuffer2Byte(buffer: ByteBuffer): Byte = buffer.get()

  implicit def ByteBuffer2ByteArray(buffer: ByteBuffer): Array[Byte] = ByteBufferUtil.getArray(buffer)

  implicit def ByteBuffer2BigDecimal(buffer: ByteBuffer): BigDecimal = DataType.decimal().deserialize(buffer).asInstanceOf[java.math.BigDecimal]

  implicit def ByteBuffer2BigInteger(buffer: ByteBuffer): BigInteger = new BigInteger(ByteBufferUtil.getArray(buffer))

  implicit def ByteBuffer2InetAddress(buffer: ByteBuffer): InetAddress = DataType.inet().deserialize(buffer).asInstanceOf[InetAddress]

  implicit def TupleBB2TupleSS(t: (ByteBuffer, ByteBuffer)): (String, String) = (t._1, t._2)

  implicit def TupleBB2TupleSI(t: (ByteBuffer, ByteBuffer)): (String, Int) = (t._1, t._2)

  implicit def TupleBB2TupleSL(t: (ByteBuffer, ByteBuffer)): (String, Long) = (t._1, t._2)

  implicit def TupleBB2TupleSF(t: (ByteBuffer, ByteBuffer)): (String, Float) = (t._1, t._2)

  implicit def TupleBB2TupleSD(t: (ByteBuffer, ByteBuffer)): (String, Double) = (t._1, t._2)

  implicit def ListBB2ListString(l: List[ByteBuffer]): List[String] = l.map(x => ByteBufferUtil.string(x))

  implicit def MapSB2MapSS(m: Map[String, ByteBuffer]): Map[String, String] = m.map {
    case (k, v) => new Tuple2[String, String](k, v)
  }.toMap

  implicit def MapSB2MapSI(m: Map[String, ByteBuffer]): Map[String, Int] = m.map {
    case (k, v) => new Tuple2[String, Int](k, v)
  }.toMap

  //implicit def ByteBuffer2String(buffer: ByteBuffer, charset: Charset): String = ByteBufferUtil.string(buffer, charset)

  implicit def MapBB2MapSS(m: Map[ByteBuffer, ByteBuffer]): Map[String, String] = m.map {
    case (k, v) => new Tuple2[String, String](k, v)
  }.toMap

  implicit def MapBB2MapSB(m: Map[ByteBuffer, ByteBuffer]) = m.map {
    case (k, v) => new Tuple2[String, ByteBuffer](k, v)
  }.toMap


  /* Typed objects to ByteBuffer */
  implicit def String2ByteBuffer(str: String): ByteBuffer = ByteBufferUtil.bytes(str)

  implicit def Int2ByteBuffer(i: Int): ByteBuffer = ByteBufferUtil.bytes(i)

  implicit def Double2ByteBuffer(d: Double): ByteBuffer = ByteBufferUtil.bytes(d)

  implicit def String2ByteBuffer(f: Float): ByteBuffer = ByteBufferUtil.bytes(f)

  implicit def Long2ByteBuffer(l: Long): ByteBuffer = ByteBufferUtil.bytes(l)

  implicit def Boolean2ByteBuffer(bool: Boolean): ByteBuffer = if (bool) Array(1.toByte) else Array(0.toByte)

  implicit def Date2ByteBuffer(date: Date): ByteBuffer = ByteBufferUtil.bytes(date.getTime)

  implicit def DateTime2ByteBuffer(date: DateTime): ByteBuffer = ByteBufferUtil.bytes(date.getMillis)

  implicit def UUID2ByteBuffer(uuid: UUID): ByteBuffer = ByteBufferUtil.bytes(uuid)

  implicit def Byte2ByteBuffer(byte: Byte): ByteBuffer = Array(byte)

  implicit def ByteArray2ByteBuffer(bytes: Array[Byte]): ByteBuffer = ByteBuffer.wrap(bytes)

  implicit def BigDecimal2ByteBuffer(bigDec: BigDecimal): ByteBuffer = DataType.decimal().serialize(bigDec.bigDecimal)

  implicit def BigInteger2ByteBuffer(bigInt: BigInteger): ByteBuffer = bigInt.toByteArray

  implicit def ByteBuffer2InetAddress(address: InetAddress): ByteBuffer = ByteBufferUtil.bytes(address)

  implicit def TupleSS2TupleBB(t: (String, String)): (ByteBuffer, ByteBuffer) = (t._1, t._2)

  implicit def TupleSI2TupleBB(t: (String, Int)): (ByteBuffer, ByteBuffer) = (t._1, t._2)

  implicit def TupleSL2TupleBB(t: (String, Long)): (ByteBuffer, ByteBuffer) = (t._1, t._2)

  implicit def TupleSF2TupleBB(t: (String, Float)): (ByteBuffer, ByteBuffer) = (t._1, t._2)

  implicit def TupleSD2TupleBB(t: (String, Double)): (ByteBuffer, ByteBuffer) = (t._1, t._2)

  implicit def MapSS2MapBB(m: Map[String, String]) = m.map {
    case (k, v) => new Tuple2[ByteBuffer, ByteBuffer](k, v)
  }.toMap

  /* Conversions for Collections */

  implicit def MapSS2ByteBuffer(map: Map[String, String]): ByteBuffer = DataType.map(DataType.text(), DataType.text()).serialize(map.asJava)

  implicit def ByteBuffer2MapSS(buffer: ByteBuffer): Map[String, String] = DataType.map(DataType.text(), DataType.text()).deserialize(buffer).asInstanceOf[java.util.Map[String, String]].asScala.toMap


  implicit def StringS2ByteBuffer(ss: Set[String]): ByteBuffer =
    DataType.set(DataType.text()).serialize(ss.asJava)

  implicit def ByteBuffer2StringS(buffer: ByteBuffer): Set[String] =
    DataType.set(DataType.text()).deserialize(buffer)
      .asInstanceOf[java.util.Set[String]].asScala.toSet


  implicit def ListS2ByteBuffer(ls: List[String]): ByteBuffer =
    DataType.list(DataType.text()).serialize(ls.asJava)

  implicit def ByteBuffer2ListS(buffer: ByteBuffer): List[String] =
    DataType.list(DataType.text()).deserialize(buffer)
      .asInstanceOf[java.util.List[String]].asScala.toList

}

