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

import com.tuplejump.calliope.hadoop.ConfigHelper
import org.apache.cassandra.thrift.{SliceRange, SlicePredicate}
import org.apache.hadoop.mapreduce.Job
import org.apache.cassandra.utils.ByteBufferUtil

import scala.collection.JavaConversions._
import com.tuplejump.calliope.queries.FinalQuery
import org.apache.hadoop.conf.Configuration
import com.tuplejump.calliope.hadoop.cql3.CqlConfigHelper

trait CasBuilder extends Serializable {
  def configuration: Configuration
}

object CasBuilder {
  /**
   * Get a CQL3 based configuration builder
   * @return BaseCql3CasBuilder
   */
  def cql3 = new BaseCql3CasBuilder()

  /**
   * Get a Thrift based configuration builder
   * @return BaseThriftCasBuilder
   */
  def thrift = new BaseThriftCasBuilder()

  /**
   * Get a Native driver based configuration builder
   * @return
   */
  def native = new BaseNativeCasBuilder()
}

class BaseThriftCasBuilder {
  /**
   * Configure the cassandra keyspace and column family to read from
   * @param keyspace Keyspace name
   * @param columnFamily Column family name
   * @return ThrifCasBuilder
   */
  def withColumnFamily(keyspace: String, columnFamily: String) = new ThriftCasBuilder(keyspace, columnFamily)
}


class BaseCql3CasBuilder {
  /**
   * Configure the cassandra keyspace and column family to read from
   * @param keyspace Keyspace name
   * @param columnFamily Column family name
   * @return Cql3CasBuilder
   */
  def withColumnFamily(keyspace: String, columnFamily: String) = new Cql3CasBuilder(keyspace, columnFamily)
}

class BaseNativeCasBuilder {
  /**
   * Configure the cassandra keyspace and column family to read from
   * @param keyspace Keyspace name
   * @param columnFamily Column family nam
   * @param inputCql The CQL query to use to fetch the records. It must be of the form
   *                 <quote>"select (*|[list of columns]) where token([partition_keys]) > ? and token([partition_keys]) <= ? [other where clauses] allow filtering"</quote>
   * @return Cql3CasBuilder
   */
  def withColumnFamilyAndQuery(keyspace: String, columnFamily: String, inputCql: String) = {
    val finalCql = if (inputCql.toLowerCase.trim.endsWith("allow filtering")) inputCql else inputCql + " allow filtering"
    new NativeCasBuilder(keyspace, columnFamily, finalCql)
  }

  /**
   * Configure the cassandra keyspace and column family to read from and the partition key columns to create the input splits
   * @param keyspace Keyspace to read from
   * @param columnFamily Column Family to read from
   * @param keys The partition key column names that are part of <b>token</b> function. You should pass all the keys here. It defaults to id
   * @return
   */
  def withColumnFamilyAndKeyColumns(keyspace: String, columnFamily: String, keys: String*) = {
    require(keys != null && keys.length > 0, "Must pass all the partition keys columns, which cannot be empty")
    val keyString = keys.mkString("\",\"")
    withColumnFamilyAndQuery(keyspace, columnFamily, s"select * from $columnFamily where token($keyString) > ? and token($keyString) <= ? allow filtering")
  }


}

abstract class CommonCasBuilder(keyspace: String,
                                columnFamily: String,
                                hasWideRows: Boolean = false,
                                host: String = "localhost",
                                port: String = "9160",
                                partitioner: CasPartitioners.Value = CasPartitioners.Murmur3Partitioner,
                                columns: Option[List[String]] = None,
                                username: Option[String] = None,
                                password: Option[String] = None,
                                readConsistencyLevel: Option[String] = None,
                                writeConsistencyLevel: Option[String] = None,
                                inputSplitSize: Option[Long] = None,
                                outputCompressionClass: Option[String] = None,
                                outputCompressionChunkLength: Option[String] = None,
                                customConfig: Option[Configuration] = None
                                 ) extends CasBuilder {

  protected def configure(): Job = {
    val job: Job = customConfig match {
      case Some(config) => new Job(config)
      case None => new Job()
    }

    //For Input
    ConfigHelper.setInputColumnFamily(job.getConfiguration, keyspace, columnFamily, hasWideRows)
    ConfigHelper.setInputInitialAddress(job.getConfiguration, host)
    ConfigHelper.setInputRpcPort(job.getConfiguration, port)
    ConfigHelper.setInputPartitioner(job.getConfiguration, partitioner.toString)

    readConsistencyLevel map {
      case cl: String => ConfigHelper.setReadConsistencyLevel(job.getConfiguration, cl)
    }

    //For Output
    ConfigHelper.setOutputColumnFamily(job.getConfiguration, keyspace, columnFamily)
    ConfigHelper.setOutputInitialAddress(job.getConfiguration, host)
    ConfigHelper.setOutputRpcPort(job.getConfiguration, port)
    ConfigHelper.setOutputPartitioner(job.getConfiguration, partitioner.toString)

    writeConsistencyLevel map {
      case cl: String => ConfigHelper.setWriteConsistencyLevel(job.getConfiguration, cl)
    }

    outputCompressionClass map {
      case cc: String => ConfigHelper.setOutputCompressionClass(job.getConfiguration, cc)
    }

    outputCompressionChunkLength map {
      case ccl: String => ConfigHelper.setOutputCompressionChunkLength(job.getConfiguration, ccl)
    }

    username map {
      case user: String => ConfigHelper.setInputKeyspaceUserName(job.getConfiguration, user)
    }

    password map {
      case pass: String => ConfigHelper.setInputKeyspacePassword(job.getConfiguration, pass)
    }

    inputSplitSize map {
      case isize => ConfigHelper.setInputSplitSize(job.getConfiguration, isize.toInt)
    }

    job

  }
}

class ThriftCasBuilder(keyspace: String,
                       columnFamily: String,
                       hasWideRows: Boolean = false,
                       host: String = "localhost",
                       port: String = "9160",
                       partitioner: CasPartitioners.Value = CasPartitioners.Murmur3Partitioner,
                       columns: Option[List[String]] = None,
                       username: Option[String] = None,
                       password: Option[String] = None,
                       query: Option[FinalQuery] = None,
                       colSliceFrom: Array[Byte] = Array.empty[Byte],
                       colSliceTo: Array[Byte] = Array.empty[Byte],
                       readConsistencyLevel: Option[String] = None,
                       writeConsistencyLevel: Option[String] = None,
                       inputSplitSize: Option[Long] = None,
                       outputCompressionClass: Option[String] = None,
                       outputCompressionChunkLength: Option[String] = None,
                       customConfig: Option[Configuration] = None
                        ) extends CommonCasBuilder(keyspace, columnFamily, hasWideRows, host, port, partitioner, columns, username, password, readConsistencyLevel, writeConsistencyLevel, inputSplitSize, outputCompressionClass, outputCompressionChunkLength, customConfig) {

  /**
   * Configure the Cassandra node to use for initial connection. This must be accessible from Spark Context.
   * @param host
   */
  def onHost(host: String) = new ThriftCasBuilder(
    keyspace, columnFamily, hasWideRows, host, port, partitioner, columns, username, password, query, colSliceFrom, colSliceTo, readConsistencyLevel, writeConsistencyLevel, inputSplitSize, outputCompressionClass, outputCompressionChunkLength, customConfig)

  /**
   * Configure the port to use for initial connection
   * @param port
   */
  def onPort(port: String) = new ThriftCasBuilder(
    keyspace, columnFamily, hasWideRows, host, port, partitioner, columns, username, password, query, colSliceFrom, colSliceTo, readConsistencyLevel, writeConsistencyLevel, inputSplitSize, outputCompressionClass, outputCompressionChunkLength, customConfig)

  /**
   * The partitioner to use, Random/Ordered/Murmur3
   * @param partitioner
   * @return
   */
  def patitionedUsing(partitioner: CasPartitioners.Value) = new ThriftCasBuilder(
    keyspace, columnFamily, hasWideRows, host, port, partitioner, columns, username, password, query, colSliceFrom, colSliceTo, readConsistencyLevel, writeConsistencyLevel, inputSplitSize, outputCompressionClass, outputCompressionChunkLength, customConfig)

  /**
   * Columns (as List[String]) to read from Cassandra
   * @param columns
   */
  def columns(columns: List[String]): ThriftCasBuilder = new ThriftCasBuilder(
    keyspace, columnFamily, hasWideRows, host, port, partitioner, Some(columns), username, password, query, colSliceFrom, colSliceTo, readConsistencyLevel, writeConsistencyLevel, inputSplitSize, outputCompressionClass, outputCompressionChunkLength, customConfig)

  /**
   * Columns to read from Cassandra
   * @param columns
   */
  def columns(columns: String*): ThriftCasBuilder = new ThriftCasBuilder(
    keyspace, columnFamily, hasWideRows, host, port, partitioner, Some(columns.toList), username, password, query, colSliceFrom, colSliceTo, readConsistencyLevel, writeConsistencyLevel, inputSplitSize, outputCompressionClass, outputCompressionChunkLength, customConfig)

  /**
   * Whether the column family is wide row.
   * @param hasWideRows
   */
  def forWideRows(hasWideRows: Boolean) = new ThriftCasBuilder(
    keyspace, columnFamily, hasWideRows, host, port, partitioner, columns, username, password, query, colSliceFrom, colSliceTo, readConsistencyLevel, writeConsistencyLevel, inputSplitSize, outputCompressionClass, outputCompressionChunkLength, customConfig)

  /**
   * Range of columns to fetch
   * @param start
   * @param finish
   */
  def columnsInRange(start: Array[Byte], finish: Array[Byte]) = new ThriftCasBuilder(
    keyspace, columnFamily, hasWideRows, host, port, partitioner, columns, username, password, query, start, finish, readConsistencyLevel, writeConsistencyLevel, inputSplitSize, outputCompressionClass, outputCompressionChunkLength, customConfig)

  /**
   * User to login to cassandra cluster
   * @param user
   */
  def authAs(user: String) = new ThriftCasBuilder(
    keyspace, columnFamily, hasWideRows, host, port, partitioner, columns, Some(user), password, query, colSliceFrom, colSliceTo, readConsistencyLevel, writeConsistencyLevel, inputSplitSize, outputCompressionClass, outputCompressionChunkLength, customConfig)

  /**
   * Password for user to authenticate with cassandra
   * @param password
   */
  def withPassword(password: String) = new ThriftCasBuilder(
    keyspace, columnFamily, hasWideRows, host, port, partitioner, columns, username, Some(password), query, colSliceFrom, colSliceTo, readConsistencyLevel, writeConsistencyLevel, inputSplitSize, outputCompressionClass, outputCompressionChunkLength, customConfig)

  /**
   * Query to filter using secondary indexes
   * @param query
   */
  def where(query: FinalQuery) = new ThriftCasBuilder(
    keyspace, columnFamily, hasWideRows, host, port, partitioner, columns, username, password, Some(query), colSliceFrom, colSliceTo, readConsistencyLevel, writeConsistencyLevel, inputSplitSize, outputCompressionClass, outputCompressionChunkLength, customConfig)

  /**
   * Use this consistency level for read (do this only if you are sure that you need it, this may affect the performance)
   * @param consistencyLevel
   * @return
   */
  def useReadConsistencyLevel(consistencyLevel: String) = new ThriftCasBuilder(
    keyspace, columnFamily, hasWideRows, host, port, partitioner, columns, username, password, query, colSliceFrom, colSliceTo, Some(consistencyLevel), writeConsistencyLevel, inputSplitSize, outputCompressionClass, outputCompressionChunkLength, customConfig)

  /**
   * Use this consistency level for write (do this only if you are sure that you need it, this may affect the performance)
   * @param consistencyLevel
   * @return
   */
  def useWriteConsistencyLevel(consistencyLevel: String) = new ThriftCasBuilder(
    keyspace, columnFamily, hasWideRows, host, port, partitioner, columns, username, password, query, colSliceFrom, colSliceTo, readConsistencyLevel, Some(consistencyLevel), inputSplitSize, outputCompressionClass, outputCompressionChunkLength, customConfig)

  /**
   * Set the number of keys to in split range that is processed per task
   * @param splitSize
   */
  def inputSplitSize(splitSize: Int) = new ThriftCasBuilder(
    keyspace, columnFamily, hasWideRows, host, port, partitioner, columns, username, password, query, colSliceFrom, colSliceTo, readConsistencyLevel, writeConsistencyLevel, Some(splitSize), outputCompressionClass, outputCompressionChunkLength, customConfig)

  /**
   * Set the compression class to use to output from maps job
   * @param compressionClass
   * @return
   */
  def useOutputCompressionClass(compressionClass: String) = new ThriftCasBuilder(
    keyspace, columnFamily, hasWideRows, host, port, partitioner, columns, username, password, query, colSliceFrom, colSliceTo, readConsistencyLevel, writeConsistencyLevel, inputSplitSize, Some(compressionClass), outputCompressionChunkLength, customConfig)

  /**
   * Set the size of data to compression in a chunk
   * @param chunkLength
   * @return
   */
  def setOutputCompressionChunkLength(chunkLength: String) = new ThriftCasBuilder(
    keyspace, columnFamily, hasWideRows, host, port, partitioner, columns, username, password, query, colSliceFrom, colSliceTo, readConsistencyLevel, writeConsistencyLevel, inputSplitSize, outputCompressionClass, Some(chunkLength), customConfig)

  /**
   * Apply the given hadoop configuration
   * @param config
   * @return
   */
  def applyCustomConfig(config: Configuration) = new ThriftCasBuilder(
    keyspace, columnFamily, hasWideRows, host, port, partitioner, columns, username, password, query, colSliceFrom, colSliceTo, readConsistencyLevel, writeConsistencyLevel, inputSplitSize, outputCompressionClass, outputCompressionChunkLength, Some(config))


  override def configuration = {

    val job = configure()

    val predicate = new SlicePredicate()
    columns match {
      case Some(colList: List[_]) =>
        predicate.setColumn_names(colList.map(col => ByteBufferUtil.bytes(col)))

      case None =>
        val sliceRange = new SliceRange()
        sliceRange.setStart(colSliceFrom)
        sliceRange.setFinish(colSliceTo)
        predicate.setSlice_range(sliceRange)
    }

    ConfigHelper.setInputSlicePredicate(job.getConfiguration, predicate)

    query map {
      case q: FinalQuery => ConfigHelper.setInputRange(job.getConfiguration, q.getExpressions)
    }
    job.getConfiguration
  }

}

class Cql3CasBuilder(keyspace: String,
                     columnFamily: String,
                     host: String = "localhost",
                     port: String = "9160",
                     partitioner: CasPartitioners.Value = CasPartitioners.Murmur3Partitioner,
                     columns: Option[List[String]] = None,
                     username: Option[String] = None,
                     password: Option[String] = None,
                     pageSize: Option[Long] = None,
                     whereClause: Option[String] = None,
                     preparedSaveQuery: Option[String] = None,
                     readConsistencyLevel: Option[String] = None,
                     writeConsistencyLevel: Option[String] = None,
                     inputSplitSize: Option[Long] = None,
                     outputCompressionClass: Option[String] = None,
                     outputCompressionChunkLength: Option[String] = None,
                     customConfig: Option[Configuration] = None
                      ) extends CommonCasBuilder(keyspace, columnFamily, false, host, port, partitioner, columns, username, password, readConsistencyLevel, writeConsistencyLevel, inputSplitSize, outputCompressionClass, outputCompressionChunkLength, customConfig) {

  /**
   * Set Cassandra node for initial connection. Must be reachable from Spark Context.
   * @param host The Cassandra hostname or IP Address
   */
  def onHost(host: String) = new Cql3CasBuilder(
    keyspace, columnFamily, host, port, partitioner, columns, username, password, pageSize, whereClause, preparedSaveQuery, readConsistencyLevel, writeConsistencyLevel, inputSplitSize, outputCompressionClass, outputCompressionChunkLength, customConfig)

  /**
   * Set Port to use for initial cassandra connection
   * @param port The Cassandra RPC port
   */
  def onPort(port: String) = new Cql3CasBuilder(
    keyspace, columnFamily, host, port, partitioner, columns, username, password, pageSize, whereClause, preparedSaveQuery, readConsistencyLevel, writeConsistencyLevel, inputSplitSize, outputCompressionClass, outputCompressionChunkLength, customConfig)

  /**
   * Set The partitioner being used by this column family
   * @param partitioner The partitioner configured for your Cassandra Cluster
   */
  def patitionedUsing(partitioner: CasPartitioners.Value) = new Cql3CasBuilder(
    keyspace, columnFamily, host, port, partitioner, columns, username, password, pageSize, whereClause, preparedSaveQuery, readConsistencyLevel, writeConsistencyLevel, inputSplitSize, outputCompressionClass, outputCompressionChunkLength, customConfig)

  /**
   * Set List of columns to be read
   * @param columns List of columns to select
   */
  def columns(columns: List[String]): Cql3CasBuilder = new Cql3CasBuilder(
    keyspace, columnFamily, host, port, partitioner, Some(columns), username, password, pageSize, whereClause, preparedSaveQuery, readConsistencyLevel, writeConsistencyLevel, inputSplitSize, outputCompressionClass, outputCompressionChunkLength, customConfig)

  /**
   * Set The columns to be read from Cassandra
   * @param columns The columns to select
   */
  def columns(columns: String*): Cql3CasBuilder = new Cql3CasBuilder(
    keyspace, columnFamily, host, port, partitioner, Some(columns.toList), username, password, pageSize, whereClause, preparedSaveQuery, readConsistencyLevel, writeConsistencyLevel, inputSplitSize, outputCompressionClass, outputCompressionChunkLength, customConfig)

  /**
   * Set User to authenticate with to Cassandra
   * @param user The username to authenticate with Cassaandra
   */
  def authAs(user: String) = new Cql3CasBuilder(
    keyspace, columnFamily, host, port, partitioner, columns, Some(user), password, pageSize, whereClause, preparedSaveQuery, readConsistencyLevel, writeConsistencyLevel, inputSplitSize, outputCompressionClass, outputCompressionChunkLength, customConfig)

  /**
   * Set Password to use for authenticating the user with cassandra
   * @param pass The password to authenticate with Cassaandra
   */
  def withPassword(pass: String) = new Cql3CasBuilder(
    keyspace, columnFamily, host, port, partitioner, columns, username, Some(pass), pageSize, whereClause, preparedSaveQuery, readConsistencyLevel, writeConsistencyLevel, inputSplitSize, outputCompressionClass, outputCompressionChunkLength, customConfig)

  /**
   * The number of rows to be fetched from cassandra in a single iterator. This should be  as large as possible but not larger.
   * @param size The number of CQL rows to fetch in one page
   */
  def setPageSize(size: Long) = new Cql3CasBuilder(
    keyspace, columnFamily, host, port, partitioner, columns, username, password, Some(size), whereClause, preparedSaveQuery, readConsistencyLevel, writeConsistencyLevel, inputSplitSize, outputCompressionClass, outputCompressionChunkLength, customConfig)

  /**
   * The CQL3 where predicate to use for filtering on secondary indexes in cassandra
   * @param clause The where clause
   */
  def where(clause: String) = new Cql3CasBuilder(
    keyspace, columnFamily, host, port, partitioner, columns, username, password, pageSize, Some(clause), preparedSaveQuery, readConsistencyLevel, writeConsistencyLevel, inputSplitSize, outputCompressionClass, outputCompressionChunkLength, customConfig)

  /**
   * The CQL3 Update query to be used while persisting data to Cassandra
   * @param query The update query
   */
  def saveWithQuery(query: String) = new Cql3CasBuilder(
    keyspace, columnFamily, host, port, partitioner, columns, username, password, pageSize, whereClause, Some(query), readConsistencyLevel, writeConsistencyLevel, inputSplitSize, outputCompressionClass, outputCompressionChunkLength, customConfig)

  /**
   * Use this consistency level for read (do this only if you are sure that you need it, this may affect the performance)
   * @param consistencyLevel The consistency level to read at
   */
  def useReadConsistencyLevel(consistencyLevel: String) = new Cql3CasBuilder(
    keyspace, columnFamily, host, port, partitioner, columns, username, password, pageSize, whereClause, preparedSaveQuery, Some(consistencyLevel), writeConsistencyLevel, inputSplitSize, outputCompressionClass, outputCompressionChunkLength, customConfig)

  /**
   * Use this consistency level for write (do this only if you are sure that you need it, this may affect the performance)
   * @param consistencyLevel The consistency level to write at
   * @return
   */
  def useWriteConsistencyLevel(consistencyLevel: String) = new Cql3CasBuilder(
    keyspace, columnFamily, host, port, partitioner, columns, username, password, pageSize, whereClause, preparedSaveQuery, readConsistencyLevel, Some(consistencyLevel), inputSplitSize, outputCompressionClass, outputCompressionChunkLength, customConfig)

  /**
   * Set the compression class to use to output from maps job
   * @param compressionClass Compression to use for output
   */
  def useOutputCompressionClass(compressionClass: String) = new Cql3CasBuilder(
    keyspace, columnFamily, host, port, partitioner, columns, username, password, pageSize, whereClause, preparedSaveQuery, readConsistencyLevel, writeConsistencyLevel, inputSplitSize, Some(compressionClass), outputCompressionChunkLength, customConfig)

  /**
   * Set the size of data to compress in a single chunk
   * @param chunkLength The size of the compression chunk
   */
  def setOutputCompressionChunkLength(chunkLength: String) = new Cql3CasBuilder(
    keyspace, columnFamily, host, port, partitioner, columns, username, password, pageSize, whereClause, preparedSaveQuery, readConsistencyLevel, writeConsistencyLevel, inputSplitSize, outputCompressionClass, Some(chunkLength), customConfig)

  /**
   * Set the number of keys to in split range that is processed per task
   * @param splitSize
   */
  def inputSplitSize(splitSize: Int) = new Cql3CasBuilder(
    keyspace, columnFamily, host, port, partitioner, columns, username, password, pageSize, whereClause, preparedSaveQuery, readConsistencyLevel, writeConsistencyLevel, Some(splitSize), outputCompressionClass, outputCompressionChunkLength, customConfig)


  /**
   * Apply the given hadoop configuration
   * @param config Custom hadoop configuration
   */
  def applyCustomConfig(config: Configuration) = new Cql3CasBuilder(
    keyspace, columnFamily, host, port, partitioner, columns, username, password, pageSize, whereClause, preparedSaveQuery, readConsistencyLevel, writeConsistencyLevel, inputSplitSize, outputCompressionClass, outputCompressionChunkLength, Some(config))

  override def configuration = {
    val job = configure()

    val string: String = columns match {
      case Some(l: List[String]) => l.mkString(",")
      case None => ""
    }

    CqlConfigHelper.setInputColumns(job.getConfiguration, string)
    pageSize map {
      case ps: Long => CqlConfigHelper.setInputCQLPageRowSize(job.getConfiguration, ps.toString)
    }

    whereClause map {
      case wc: String => CqlConfigHelper.setInputWhereClauses(job.getConfiguration, wc)
    }

    preparedSaveQuery map {
      case pql: String => CqlConfigHelper.setOutputCql(job.getConfiguration, pql)
    }


    job.getConfiguration
  }

}

class NativeCasBuilder(keyspace: String,
                       columnFamily: String,
                       inputCql: String,
                       host: String = "localhost",
                       port: String = "9160",
                       nativePort: String = "9042",
                       partitioner: CasPartitioners.Value = CasPartitioners.Murmur3Partitioner,
                       username: Option[String] = None,
                       password: Option[String] = None,
                       pageSize: Option[Long] = None,
                       preparedSaveQuery: Option[String] = None,
                       writeConsistencyLevel: Option[String] = None,
                       inputSplitSize: Option[Long] = None,
                       outputCompressionClass: Option[String] = None,
                       outputCompressionChunkLength: Option[String] = None,
                       customConfig: Option[Configuration] = None,
                       useMultiRangeSplits: Boolean = false,
                       rangesPerMultiRangeSplit: Option[Int] = None
                        ) extends CommonCasBuilder(keyspace, columnFamily, false, host, port, partitioner, None, username, password, None, writeConsistencyLevel, inputSplitSize, outputCompressionClass, outputCompressionChunkLength, customConfig) {

  /**
   * Cassandra node for initial connection. Must be reachable from Spark Context.
   * @param host
   */
  def onHost(host: String) = new NativeCasBuilder(
    keyspace, columnFamily, inputCql, host, port, nativePort, partitioner, username, password, pageSize, preparedSaveQuery, writeConsistencyLevel, inputSplitSize, outputCompressionClass, outputCompressionChunkLength, customConfig, useMultiRangeSplits, rangesPerMultiRangeSplit)

  /**
   * Port to use for initial cassandra connection
   * @param port
   */
  def onPort(port: String) = new NativeCasBuilder(
    keyspace, columnFamily, inputCql, host, port, nativePort, partitioner, username, password, pageSize, preparedSaveQuery, writeConsistencyLevel, inputSplitSize, outputCompressionClass, outputCompressionChunkLength, customConfig, useMultiRangeSplits, rangesPerMultiRangeSplit)

  /**
   * Set the native port the cassandra server is listening on
   * @param nativePort
   */
  def onNativePort(nativePort: String) = new NativeCasBuilder(
    keyspace, columnFamily, inputCql, host, port, nativePort, partitioner, username, password, pageSize, preparedSaveQuery, writeConsistencyLevel, inputSplitSize, outputCompressionClass, outputCompressionChunkLength, customConfig, useMultiRangeSplits, rangesPerMultiRangeSplit)

  /**
   * The partitioner being used by this column family
   * @param partitioner
   */
  def patitionedUsing(partitioner: CasPartitioners.Value) = new NativeCasBuilder(
    keyspace, columnFamily, inputCql, host, port, nativePort, partitioner, username, password, pageSize, preparedSaveQuery, writeConsistencyLevel, inputSplitSize, outputCompressionClass, outputCompressionChunkLength, customConfig, useMultiRangeSplits, rangesPerMultiRangeSplit)

  /**
   * User to authenticate with to Cassandra
   * @param user
   */
  def authAs(user: String) = new NativeCasBuilder(
    keyspace, columnFamily, inputCql, host, port, nativePort, partitioner, Some(user), password, pageSize, preparedSaveQuery, writeConsistencyLevel, inputSplitSize, outputCompressionClass, outputCompressionChunkLength, customConfig, useMultiRangeSplits, rangesPerMultiRangeSplit)

  /**
   * Password to use for authenticating the user with cassandra
   * @param pass
   */
  def withPassword(pass: String) = new NativeCasBuilder(
    keyspace, columnFamily, inputCql, host, port, nativePort, partitioner, username, Some(pass), pageSize, preparedSaveQuery, writeConsistencyLevel, inputSplitSize, outputCompressionClass, outputCompressionChunkLength, customConfig, useMultiRangeSplits, rangesPerMultiRangeSplit)

  /**
   * The number of rows to be fetched from cassandra in a single iterator. This should be  as large as possible but not larger.
   * @param size
   */
  def setPageSize(size: Long) = new NativeCasBuilder(
    keyspace, columnFamily, inputCql, host, port, nativePort, partitioner, username, password, Some(size), preparedSaveQuery, writeConsistencyLevel, inputSplitSize, outputCompressionClass, outputCompressionChunkLength, customConfig, useMultiRangeSplits, rangesPerMultiRangeSplit)

  /**
   * The CQL3 Update query to be used while persisting data to Cassandra
   * @param query
   */
  def saveWithQuery(query: String) = new NativeCasBuilder(
    keyspace, columnFamily, inputCql, host, port, nativePort, partitioner, username, password, pageSize, Some(query), writeConsistencyLevel, inputSplitSize, outputCompressionClass, outputCompressionChunkLength, customConfig, useMultiRangeSplits, rangesPerMultiRangeSplit)

  /**
   * Use this consistency level for write (do this only if you are sure that you need it, this may affect the performance)
   * @param consistencyLevel
   */
  def useWriteConsistencyLevel(consistencyLevel: String) = new NativeCasBuilder(
    keyspace, columnFamily, inputCql, host, port, nativePort, partitioner, username, password, pageSize, preparedSaveQuery, Some(consistencyLevel), inputSplitSize, outputCompressionClass, outputCompressionChunkLength, customConfig, useMultiRangeSplits, rangesPerMultiRangeSplit)

  /**
   * Set the compression class to use to output from maps job
   * @param compressionClass
   */
  def useOutputCompressionClass(compressionClass: String) = new NativeCasBuilder(
    keyspace, columnFamily, inputCql, host, port, nativePort, partitioner, username, password, pageSize, preparedSaveQuery, writeConsistencyLevel, inputSplitSize, Some(compressionClass), outputCompressionChunkLength, customConfig, useMultiRangeSplits, rangesPerMultiRangeSplit)

  /**
   * Set the size of data to compression in a chunk
   * @param chunkLength
   */
  def setOutputCompressionChunkLength(chunkLength: String) = new NativeCasBuilder(
    keyspace, columnFamily, inputCql, host, port, nativePort, partitioner, username, password, pageSize, preparedSaveQuery, writeConsistencyLevel, inputSplitSize, outputCompressionClass, Some(chunkLength), customConfig, useMultiRangeSplits, rangesPerMultiRangeSplit)

  /**
   * Set the number of keys to in split range that is processed per task
   * @param splitSize
   */
  def inputSplitSize(splitSize: Int) = new NativeCasBuilder(
    keyspace, columnFamily, inputCql, host, port, nativePort, partitioner, username, password, pageSize, preparedSaveQuery, writeConsistencyLevel, Some(splitSize), outputCompressionClass, outputCompressionChunkLength, customConfig, useMultiRangeSplits, rangesPerMultiRangeSplit)

  /**
   * Apply the given hadoop configuration
   * @param config
   */
  def applyCustomConfig(config: Configuration) = new NativeCasBuilder(
    keyspace, columnFamily, inputCql, host, port, nativePort, partitioner, username, password, pageSize, preparedSaveQuery, writeConsistencyLevel, inputSplitSize, outputCompressionClass, outputCompressionChunkLength, Some(config), useMultiRangeSplits, rangesPerMultiRangeSplit)

  /**
   * Configure to compute with multiple ranges per split and set the number of splits in a range
   * @param rangesPerSplit
   * @return
   */
  def mergeRangesInMultiRangeSplit(rangesPerSplit: Int) = new NativeCasBuilder(
    keyspace, columnFamily, inputCql, host, port, nativePort, partitioner, username, password, pageSize, preparedSaveQuery, writeConsistencyLevel, inputSplitSize, outputCompressionClass, outputCompressionChunkLength, customConfig, true, Some(rangesPerSplit))

  override def configuration = {
    val job = configure()

    CqlConfigHelper.setInputNativePort(job.getConfiguration, nativePort)
    CqlConfigHelper.setInputCql(job.getConfiguration, inputCql)

    pageSize map {
      case ps: Long => CqlConfigHelper.setInputCQLPageRowSize(job.getConfiguration, ps.toString)
    }

    preparedSaveQuery map {
      case pql: String => CqlConfigHelper.setOutputCql(job.getConfiguration, pql)
    }

    if (this.useMultiRangeSplits) {
      CqlConfigHelper.setMultirangeInputSplit(job.getConfiguration, true)
      CqlConfigHelper.setRangesInMultiRangeSplit(job.getConfiguration, this.rangesPerMultiRangeSplit.get)
    }

    job.getConfiguration
  }

}


object CasPartitioners extends Enumeration {
  type CasPartitioner = Value
  val Murmur3Partitioner = Value("Murmur3Partitioner")
  val RandomPartitioner = Value("RandomPartitioner")
  val ByteOrderedPartitioner = Value("ByteOrderedPartitioner")
}
