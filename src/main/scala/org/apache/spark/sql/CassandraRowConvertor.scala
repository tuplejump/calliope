package org.apache.spark.sql

import java.sql.Timestamp

import com.datastax.driver.core.{DataType => CassandraDataType, Row => CassandraRow}

import scala.collection.JavaConversions._

object CassandraSparkRowConvertor {

  def build(crow: CassandraRow): Array[Any] = {
    crow.getColumnDefinitions.map {
      cd =>
        readTypedValue(crow, cd.getName, cd.getType)
    }.toArray
  }

  private def readTypedValue(crow: CassandraRow, name: String, valueType: CassandraDataType): Any = {
    valueType.getName match {
      case CassandraDataType.Name.ASCII => crow.getString(name)
      case CassandraDataType.Name.BIGINT => crow.getLong(name)
      case CassandraDataType.Name.BLOB => crow.getBytes(name).array()
      case CassandraDataType.Name.BOOLEAN => crow.getBool(name)
      case CassandraDataType.Name.COUNTER => crow.getLong(name)
      case CassandraDataType.Name.DECIMAL => crow.getDecimal(name)
      case CassandraDataType.Name.DOUBLE => crow.getDouble(name)
      case CassandraDataType.Name.FLOAT => crow.getFloat(name)
      case CassandraDataType.Name.INT => crow.getInt(name)
      case CassandraDataType.Name.TEXT => crow.getString(name)
      case CassandraDataType.Name.VARINT => crow.getVarint(name) //Big Integer is treated as BigDecimal by Catalyst
      case CassandraDataType.Name.INET => crow.getInet(name).toString //TODO: Stopgap solution
      case CassandraDataType.Name.CUSTOM => crow.getBytes(name).array() //TODO: Stopgap solution. Explore UDF
      case CassandraDataType.Name.UUID => crow.getUUID(name).toString //TODO: Stopgap solution. Should be struct.
      case CassandraDataType.Name.TIMEUUID => crow.getUUID(name).toString //TODO: Stopgap solution. Should be struct.
      case CassandraDataType.Name.TIMESTAMP => new Timestamp(crow.getDate(name).getTime)
      case CassandraDataType.Name.VARCHAR => crow.getString(name)
      case CassandraDataType.Name.LIST => {
        val argType: Class[_] = valueType.getTypeArguments()(0).asJavaClass()
        crow.getList(name, argType)
      }
      case CassandraDataType.Name.SET => {
        val argType: Class[_] = valueType.getTypeArguments()(0).asJavaClass()
        crow.getSet(name, argType)
      }
      case CassandraDataType.Name.MAP => {
        val argType1: Class[_] = valueType.getTypeArguments()(0).asJavaClass()
        val argType2: Class[_] = valueType.getTypeArguments()(1).asJavaClass()
        crow.getMap(name, argType1, argType2)
      }
    }
  }
}
