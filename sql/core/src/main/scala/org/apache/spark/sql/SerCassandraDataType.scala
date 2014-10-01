package org.apache.spark.sql

import com.datastax.driver.core.{DataType => CassanndraDataType}

import scala.collection.JavaConversions._

case class SerCassandraDataType(dataType: CassanndraDataType.Name, param1: Option[CassanndraDataType.Name], param2: Option[CassanndraDataType.Name])

object SerCassandraDataType {
  def fromDataType(dt: CassanndraDataType): SerCassandraDataType = {
    if (dt.isCollection) {
      (dt.getName: @unchecked) match {
        case CassanndraDataType.Name.MAP =>
          val params = dt.getTypeArguments
          SerCassandraDataType(dt.getName, Some(params(0).getName), Some(params(1).getName))
        case CassanndraDataType.Name.SET =>
          val params = dt.getTypeArguments
          SerCassandraDataType(dt.getName, Some(params(0).getName), None)
        case CassanndraDataType.Name.LIST =>
          val params = dt.getTypeArguments
          SerCassandraDataType(dt.getName, Some(params(0).getName), None)
      }
    } else {
      SerCassandraDataType(dt.getName, None, None)
    }
  }
}