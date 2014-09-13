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

package org.apache.spark.sql

import java.net.InetAddress
import java.nio.ByteBuffer
import java.sql.Timestamp
import java.util.{Date, UUID}

import org.apache.spark.sql.catalyst.expressions.Attribute


import com.datastax.driver.core.{DataType => CassandraDataType, Row => CassandraRow}

import scala.collection.JavaConversions._

object CassandraSparkDataConvertor {

  private[sql] def build(crow: CassandraRow, output: Seq[Attribute]): Array[Any] = {
    output.map(_.name).map{
      column =>
        readTypedValue(crow, column, crow.getColumnDefinitions.getType(column))
    }.toArray
  }

  private def readTypedValue(crow: CassandraRow, name: String, valueType: CassandraDataType): Any = {
    valueType.getName match {
      case CassandraDataType.Name.ASCII => crow.getString(name)
      case CassandraDataType.Name.BIGINT => crow.getLong(name)
      case CassandraDataType.Name.BOOLEAN => crow.getBool(name)
      case CassandraDataType.Name.COUNTER => crow.getLong(name)
      case CassandraDataType.Name.DECIMAL => BigDecimal(crow.getDecimal(name))
      case CassandraDataType.Name.DOUBLE => crow.getDouble(name)
      case CassandraDataType.Name.FLOAT => crow.getFloat(name)
      case CassandraDataType.Name.INT => crow.getInt(name)
      case CassandraDataType.Name.TEXT => crow.getString(name)
      case CassandraDataType.Name.VARINT => BigDecimal(crow.getVarint(name)) //Big Integer is treated as BigDecimal by Catalyst
      case CassandraDataType.Name.INET => crow.getInet(name)
      case CassandraDataType.Name.CUSTOM => crow.getBytes(name).array() //TODO: Stopgap solution. Explore UDF
      case CassandraDataType.Name.UUID => crow.getUUID(name).toString //TODO: Stopgap solution. Should be struct.
      case CassandraDataType.Name.TIMEUUID => crow.getUUID(name).toString //TODO: Stopgap solution. Should be struct.
      case CassandraDataType.Name.TIMESTAMP => new Timestamp(crow.getDate(name).getTime)
      case CassandraDataType.Name.VARCHAR => crow.getString(name)
      case CassandraDataType.Name.BLOB => {
        val bb = crow.getBytes(name)
        val ba = new Array[Byte](bb.limit - bb.position)
        bb.get(ba)
        ba
      }
      case CassandraDataType.Name.LIST => {
        val argType: Class[_] = valueType.getTypeArguments()(0).asJavaClass()
        crow.getList(name, argType).toList
      }
      case CassandraDataType.Name.SET => {
        val argType: Class[_] = valueType.getTypeArguments()(0).asJavaClass()
        crow.getSet(name, argType).toSeq
      }
      case CassandraDataType.Name.MAP => {
        val argType1: Class[_] = valueType.getTypeArguments()(0).asJavaClass()
        val argType2: Class[_] = valueType.getTypeArguments()(1).asJavaClass()
        crow.getMap(name, argType1, argType2).toMap
      }
    }
  }

  import scala.collection.JavaConversions._

  private[sql] def serializeValue(data: Any, dtype: SerCassandraDataType): ByteBuffer = {
    dtype.dataType match {
      case CassandraDataType.Name.ASCII => CassandraDataType.ascii().serialize(data)
      case CassandraDataType.Name.BIGINT => CassandraDataType.bigint().serialize(data.asInstanceOf[Long])
      case CassandraDataType.Name.BLOB => CassandraDataType.blob().serialize(ByteBuffer.wrap(data.asInstanceOf[Array[Byte]]))
      case CassandraDataType.Name.BOOLEAN => CassandraDataType.cboolean().serialize(data)
      case CassandraDataType.Name.COUNTER => CassandraDataType.counter().serialize(data)
      case CassandraDataType.Name.DECIMAL => CassandraDataType.decimal().serialize(data.asInstanceOf[BigDecimal].underlying())
      case CassandraDataType.Name.DOUBLE => CassandraDataType.cdouble().serialize(data)
      case CassandraDataType.Name.FLOAT => CassandraDataType.cfloat().serialize(data)
      case CassandraDataType.Name.INT => CassandraDataType.cint().serialize(data)
      case CassandraDataType.Name.TEXT => CassandraDataType.text().serialize(data)
      case CassandraDataType.Name.VARINT => CassandraDataType.varint().serialize(data.asInstanceOf[BigDecimal].toBigInt().underlying()) //Big Integer is treated as BigDecimal by Catalyst
      case CassandraDataType.Name.INET => if (data.isInstanceOf[String]) CassandraDataType.inet().serialize(InetAddress.getByName(data.asInstanceOf[String])) else CassandraDataType.inet().serialize(data.asInstanceOf[InetAddress])
      case CassandraDataType.Name.UUID => CassandraDataType.uuid().serialize(UUID.fromString(data.asInstanceOf[String])) //TODO: Stopgap solution. Should be struct.
      case CassandraDataType.Name.TIMEUUID => CassandraDataType.timeuuid().serialize(UUID.fromString(data.asInstanceOf[String])) //TODO: Stopgap solution. Should be struct.
      case CassandraDataType.Name.TIMESTAMP => CassandraDataType.timestamp().serialize(new Date(data.asInstanceOf[Timestamp].getTime))
      case CassandraDataType.Name.VARCHAR => CassandraDataType.varchar().serialize(data)
      case CassandraDataType.Name.LIST => {
        val par1 = CassandraDataType.allPrimitiveTypes().collectFirst {
          case dt if (dt.getName == dtype.param1.get) => dt
        }.get

        val seq: Seq[_] = data.asInstanceOf[Seq[_]]

        val jlist: java.util.List[_] = seq.toList

        CassandraDataType.list(par1).serialize(jlist)
      }
      case CassandraDataType.Name.SET => {
        val par1 = CassandraDataType.allPrimitiveTypes().collectFirst {
          case dt if (dt.getName == dtype.param1.get) => dt
        }.get

        val jset: java.util.Set[_] = setAsJavaSet(data.asInstanceOf[Seq[_]].toSet)
        CassandraDataType.set(par1).serialize(jset)
      }
      case CassandraDataType.Name.MAP => {

        val par1 = CassandraDataType.allPrimitiveTypes().collectFirst {
          case dt if (dt.getName == dtype.param1.get) => dt
        }.get


        val par2 = CassandraDataType.allPrimitiveTypes().collectFirst {
          case dt if (dt.getName == dtype.param2.get) => dt
        }.get

        CassandraDataType.map(par1, par2).serialize(mapAsJavaMap(data.asInstanceOf[Map[_, _]]))
      }
      case CassandraDataType.Name.CUSTOM => CassandraDataType.custom(data.getClass.getCanonicalName).serialize(data)
    }
  }
}
