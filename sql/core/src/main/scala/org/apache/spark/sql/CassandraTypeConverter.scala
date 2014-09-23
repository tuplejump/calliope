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

import com.datastax.driver.core.{Cluster, KeyspaceMetadata, Metadata, TableMetadata, DataType => CassandraDataType}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.types.{DataType => CatalystDataType, _}

import scala.collection.JavaConversions._


private[sql] object CassandraTypeConverter {

  def convertToAttributes(table: TableMetadata): Seq[Attribute] = {
    table.getColumns.map(column => new AttributeReference(column.getName, toCatalystType(column.getType), false)())
  }


  private def toCatalystType(dataType: CassandraDataType): CatalystDataType = {
    if (!dataType.isCollection) {
      toPrimitiveDataType(dataType)
    } else {
      toCollectionDataType(dataType)
    }
  }

  private def toCollectionDataType(dataType: CassandraDataType): CatalystDataType = {
    (dataType.getName: @unchecked) match {
      case CassandraDataType.Name.LIST => {
        val argTypes = dataType.getTypeArguments
        assert(argTypes.length == 1, "Impossible situation: Invalid List Argument Types [${argTypes}]")
        val elementType = toPrimitiveDataType(argTypes(0))
        ArrayType(elementType)
      }
      case CassandraDataType.Name.SET => {
        val argTypes = dataType.getTypeArguments
        assert(argTypes.length == 1, s"Impossible situation: Invalid Set Argument Types [${argTypes}]")
        val elementType = toPrimitiveDataType(argTypes(0))
        ArrayType(elementType)
      }
      case CassandraDataType.Name.MAP => {
        val argTypes = dataType.getTypeArguments
        assert(argTypes.length == 2, "Impossible situation: Invalid Map Argument Types [${argTypes}]")
        val elementType1 = toPrimitiveDataType(argTypes(0))
        val elementType2 = toPrimitiveDataType(argTypes(1))
        MapType(elementType1, elementType2)
      }
    }
  }

  private def toPrimitiveDataType(dataType: CassandraDataType): CatalystDataType = {
    (dataType.getName: @unchecked) match {
      case CassandraDataType.Name.ASCII => StringType
      case CassandraDataType.Name.VARCHAR => StringType
      case CassandraDataType.Name.BIGINT => LongType
      case CassandraDataType.Name.BLOB => BinaryType
      case CassandraDataType.Name.BOOLEAN => BooleanType
      case CassandraDataType.Name.COUNTER => LongType
      case CassandraDataType.Name.DECIMAL => DecimalType
      case CassandraDataType.Name.DOUBLE => DoubleType
      case CassandraDataType.Name.FLOAT => FloatType
      case CassandraDataType.Name.INT => IntegerType
      case CassandraDataType.Name.TEXT => StringType
      case CassandraDataType.Name.VARINT => DecimalType //Big Integer is treated as BigDecimal by Catalyst
      case CassandraDataType.Name.INET => StringType //TODO: Stopgap solution
      case CassandraDataType.Name.UUID => StringType //TODO: Stopgap solution
      case CassandraDataType.Name.TIMEUUID => StringType //TODO: Stopgap solution
      case CassandraDataType.Name.TIMESTAMP => TimestampType
      case CassandraDataType.Name.CUSTOM => BinaryType //TODO: Stopgap solution. Explore use of UDF
    }
  }
}

