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

package com.tuplejump.calliope.examples

import com.tuplejump.calliope.Types._
import java.util.Date

import scala.language.implicitConversions

/**
 * This class demonstrates some Transformer to help you write your own.
 */
class Transformers {

  /*
   *
   * Always include RichByteBuffer._, it will make your code much cleaner,
   * as you won't have to deal with the ByteBuffer => T transforms for basic types
   *
   */

  import com.tuplejump.calliope.utils.RichByteBuffer._

  /**
   *
   * Let us consider a Cassandra Column Family containing skinny rows for storing employees,
   *
   * +---------+------------------------------------------------------------+
   * | emp_id  | Columns                                                    |
   * +---------+------------------------------------------------------------+
   * |         | +-----------------+-----------+---------+----------------+ |
   * |         | | name            | dept      | age     | email          | |
   * |         | +-----------------+-----------+---------+----------------+ |
   * |         |                                                            |
   * +---------+------------------------------------------------------------+
   *
   * Which will be represented in CQL as,
   *
   * +-------------+----------------+-----------+--------------+------------------+
   * | emp_id (pk) | name           | dept      | age          | email            |
   * +-------------+----------------+-----------+--------------+------------------+
   *
   * And we would like to work with the Employee case class in our RDDs,
   * i.e. the RDD created should be RDD[Employee], given
   *
   * case class Employee(empId: String, name: String, dept: String, age: Int, email: String)
   *
   **/
  case class Employee(empId: String, name: String, dept: String, age: Int, email: String)

  /**
   *
   * Using <b>thriftCassandra</b> we need to provide an implicit convertor of type,
   * (ThriftRowKey, ThriftRowMap) => Employee
   *
   * Where
   * type ThriftRowKey = ByteBuffer
   * type ThriftColumnName = ByteBuffer
   * type ThriftColumnValue = ByteBuffer
   * type ThriftRowMap = Map[ThriftColumnName, ThriftColumnValue]
   *
   * i.e. the 'key' will have the emp_id or the thrift key
   * and the values will have the columns as a Map of name to their value.
   *
   * In the method below you'll see we use strings for ColumnNames and
   * we don't worry about converting the values as ByteBuffer to their data types i.e.
   * String for empId, name, dept, email and Int for age. This is possible as the implicits in
   * RichByteBuffer provide us with these transforms.
   *
   */

  implicit def thriftEmp2Employee(k: ThriftRowKey, v: ThriftRowMap): Employee = {
    Employee(k, v("name"), v("dept"), v("age"), v("email"))
  }


  /**
   *
   * Using <b>cql3Cassandra</b> on the same  we need to provide an implicit convertor of type,
   * (CQLRowKeyMap, CQLRowMap) => Employee
   *
   * Where
   * type CQLRowKeyMap = Map[CQLColumnName, CQLColumnValue]
   * type CQLRowMap = Map[CQLColumnName, CQLColumnValue]
   * type CQLColumnName = String
   * type CQLColumnValue = ByteBuffer
   *
   * In the method below you'll see we don't worry about converting the values as ByteBuffer to their data types i.e.
   * String for empId, name, dept, email and Int for age. This is possible as the implicits in
   * RichByteBuffer provide us with these transforms.
   *
   */
  implicit def cql3Emp2Employee(k: CQLRowKeyMap, v: CQLRowMap): Employee = {
    Employee(k("emp_id"), v("name"), v("dept"), v("age"), v("email"))
  }

  /**
   * To write the output of a Spark job which results in RDD[Employee] to this Column Family, we will need to provide
   * the reverse transformers
   **/

  /**
   *
   * Let us first look at how we write this out using <b>thriftSaveToCassandra</b>
   * This method expects us to provide two implicit transformers of type,
   * 1. U => ThriftRowKey
   * 2. U => ThriftRowMap
   *
   * As we have a simple Row Key for our table we can emit that as is.
   * The second function will require return a map for column_name -> column_value
   *
   **/

  implicit def employee2ThriftRowKey(emp: Employee): ThriftRowKey = {
    // The row key in the case class Employee is the empId, so we return that
    emp.empId
  }

  implicit def employee2ThriftRowMap(emp: Employee): ThriftRowMap = {
    // Here we have to return the map for this rows columns.
    Map(
      "name" -> emp.name,
      "dept" -> emp.dept,
      "age" -> emp.age,
      "email" -> emp.email
    )
  }

  /**
   *
   * Again, let us do the same with <b>cql3SaveToCassandra</b>
   *
   * Before we go to transformers,
   * note that using this method requires you to configure the saveWithQuery on Cql3CasBuilder.
   *
   * The query <b>SHOULD</b> be an <b>UPDATE</b> query setting the values and not specifying the key.
   * In our case here this will be,
   * <i>UPDATE empKeyspace.empTable set name = ?, dept = ?, age = ?, email = ?</i>
   *
   *
   * Coming to transformers, in this case we will need 2 transformers,
   * 1. U => CQLRowKeyMap
   * 2. U => CQLRowValues
   *
   * Where,
   * type CQLRowKeyMap = Map[CQLColumnName, CQLColumnValue]
   * type CQLRowValues = List[CQLColumnValue]
   * type CQLColumnName = String
   * type CQLColumnValue = ByteBuffer
   *
   * Thus we will need a method that take Employee e  and return ("emp_id" -> e.empId)
   *
   * And Another method that takes Employee e and returns a List(name, dept, age, email)
   * in the same order as mentioned in the query.
   *
   */
  implicit def employee2CqlRowKeyMap(emp: Employee): CQLRowKeyMap = {
    Map("emp_id" -> emp.empId)
  }

  implicit def employee2CqlRowValues(emp: Employee): CQLRowValues = {
    List(emp.name, emp.dept, emp.age, emp.email)
  }

  /**
   *
   * <b>Let us now take a look at working with wide rows.</b>
   * We will continue working with the same employee class, but change our,
   * Cassandra column family to use a wide row model.
   * For this Let us make dept the partitioning key and emp_id the clustering key.
   *
   * So our Column Family will look like this,   *
   * +---------+---------------------------------------------------------------------------------+
   * | dept    | Columns                                                                         |
   * +---------+---------------------------------------------------------------------------------+
   * |         | +-----------------+----------------+----------------+------------------+----+-- |
   * |         | | [emp_id].name   | [emp_id].dept  | [emp_id].age   | [emp_id].email   |... |   |
   * |         | +-----------------+----------------+----------------+------------------+----+-- |
   * |         |                                                                                 |
   * +---------+---------------------------------------------------------------------------------+
   *
   * Which will be represented in CQL as,
   *
   * +-----------+-------------+----------------+--------------+------------------+
   * | dept (pk) | emp_id (ck) | name           | age          | email            |
   * +-----------+-------------+----------------+--------------+------------------+
   *
   * In our use and also with our clients we have found that using CQL3
   * approach is much more easier to work with wisse rows as the data coming from Cassandra if already transformed
   * into a map structure.
   *
   * Hence, in this scenario we will focus on using the <b>cql3Cassandra</b>.
   *
   **/

  /**
   *
   * To read the data from this column family, we will need a transformer with signature,
   * (CQLRowKeyMap, CQLRowMap) => Employee
   *
   * The difference in this transformer from the last one we used for skinny rows with same signature is
   * that the CQLRowKeyMap (key) will have <b>dept</b> and <b>emp_id</b> as dept and emp_id are the partition
   * and clustering keys respectively.
   *
   * Notice in the method below we are reading the emp_id and dept from key and rest from the row's value.
   *
   **/

  implicit def cql3Wide2Employee(key: CQLRowKeyMap, v: CQLRowMap): Employee = {
    Employee(key("emp_id"), v("name"), key("dept"), v("age"), v("email"))
  }

  /**
   * To write to this column family we will again need the update query setting the values,
   *
   * UPDATE empKeyspace.empWideTable set name = ?, age = ?, email = ?
   *
   * Notice that we don't have dept in the set clause anymore.
   *
   * And now to write the transformers, we again need, 2 transformers,
   * 1. U => CQLRowKeyMap
   * 2. U => CQLRowValues
   *
   * As will be obvious by now, the first transformer will emit a Map,
   * with dept and emp_id as the keys mapped to their values.
   *
   * The second transformer will emit a list for name, age and email in that order.
   *
   */
  implicit def employee2CqlRowKeyMapWide(emp: Employee): CQLRowKeyMap = {
    Map("dept" -> emp.dept, "emp_id" -> emp.empId)
  }

  implicit def employee2CqlRowValuesWide(emp: Employee): CQLRowValues = {
    List(emp.name, emp.age, emp.email)
  }

  /**
   *
   * Continuing the same story, if name was also part of the key (either partition or clustering)
   * i.e. the primary key of the column family was defined as
   * (dept, name, emp_id) or ((dept, name), emp_id) then in our transformer
   * we would read it from the key i.e.
   *
   */
  implicit def cql3XWide2Employee(key: CQLRowKeyMap, v: CQLRowMap): Employee = {
    Employee(key("emp_id"), key("name"), key("dept"), v("age"), v("email"))
  }

  /**
   *
   * Finally, let us see at a special case of column family with a counter column.
   *
   * If we have created a CQL3 CF like this,
   *
   * +-----------+--------------+-------------------+
   * | fact (pk) | base_ts (ck) | count (counter)   |
   * +-----------+--------------+-------------------+
   *
   * There is nothing different in reading from the ColumnFamily.
   * So if we are using a case class called FactCount like,
   *
   * FactCount(fact: String, baseTs: Date, count: Long)
   *
   */

  case class FactCount(fact: String, baseTs: Date, count: Long)

  /**
   *
   * The transformer to read, will have the signature,
   * (CQLRowKeyMap, CQLRowMap) => FactCount
   *
   */
  implicit def cqlRow2FactCount(k: CQLRowKeyMap, v: CQLRowMap): FactCount =
    FactCount(k("fact"), k("base_ts"), v("count"))


  /**
   *
   * To write to this column family wee will use the following query,
   * UPDATE cube.facts set count = count + ?
   *
   * as we will want to update the count and cannot just set it.
   *
   * And our transformers in this case will be,
   *
   */

  implicit def factCount2RowMap(fc: FactCount): CQLRowKeyMap =
    Map("fact" -> fc.fact, "base_ts" -> fc.baseTs)

  implicit def factCount2RowValues(fc: FactCount): CQLRowValues = List(fc.count)

}






















