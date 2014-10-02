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