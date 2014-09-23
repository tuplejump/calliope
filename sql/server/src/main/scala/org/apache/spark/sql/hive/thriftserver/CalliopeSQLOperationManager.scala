/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive.thriftserver

import java.sql.{DatabaseMetaData, Timestamp}
import java.util
import java.util.{Map => JMap}

import com.datastax.driver.core.KeyspaceMetadata
import com.tuplejump.calliope.server.ReflectionUtils
import com.tuplejump.calliope.sql.{CassandraProperties, CassandraSchemaHelper}
import org.apache.hadoop.hive.common.`type`.HiveDecimal
import org.apache.hadoop.hive.metastore.TableType
import org.apache.hadoop.hive.metastore.api.FieldSchema
import org.apache.hive.service.cli._
import org.apache.hive.service.cli.operation._
import org.apache.hive.service.cli.session.HiveSession
import org.apache.hive.service.cli.thrift.TColumnValue
import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.plans.logical.SetCommand
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.hive.{HiveContext, HiveMetastoreTypes}
import org.apache.spark.sql.{SQLConf, SchemaRDD, Row => SparkRow}

import scala.collection.JavaConversions._
import scala.collection.mutable.{ArrayBuffer, Map}
import scala.math.{random, round}
import scala.util.matching.Regex

/**
 * Executes queries using Spark SQL, and maintains a list of handles to active queries.
 */
class CalliopeSQLOperationManager(hiveContext: HiveContext)
  extends OperationManager with Logging {

  val handleToOperation = ReflectionUtils
    .getSuperField[JMap[OperationHandle, Operation]](this, "handleToOperation")

  // TODO: Currenlty this will grow infinitely, even as sessions expire
  val sessionToActivePool = Map[HiveSession, String]()

  override def newExecuteStatementOperation(
      parentSession: HiveSession,
      statement: String,
      confOverlay: JMap[String, String],
      async: Boolean): ExecuteStatementOperation = synchronized {

    val operation = new ExecuteStatementOperation(parentSession, statement, confOverlay) {
      private var result: SchemaRDD = _
      private var iter: Iterator[SparkRow] = _
      private var dataTypes: Array[DataType] = _

      def close(): Unit = {
        // RDDs will be cleaned automatically upon garbage collection.
        logDebug("CLOSING")
      }

      def getNextRowSet(order: FetchOrientation, maxRowsL: Long): RowSet = {
        if (!iter.hasNext) {
          new RowSet()
        } else {
          // maxRowsL here typically maps to java.sql.Statement.getFetchSize, which is an int
          val maxRows = maxRowsL.toInt
          var curRow = 0
          var rowSet = new ArrayBuffer[Row](maxRows.min(1024))

          while (curRow < maxRows && iter.hasNext) {
            val sparkRow = iter.next()
            val row = new Row()
            var curCol = 0

            while (curCol < sparkRow.length) {
              if (sparkRow.isNullAt(curCol)) {
                addNullColumnValue(sparkRow, row, curCol)
              } else {
                addNonNullColumnValue(sparkRow, row, curCol)
              }
              curCol += 1
            }
            rowSet += row
            curRow += 1
          }
          new RowSet(rowSet, 0)
        }
      }

      def addNonNullColumnValue(from: SparkRow, to: Row, ordinal: Int) {
        dataTypes(ordinal) match {
          case StringType =>
            to.addString(from(ordinal).asInstanceOf[String])
          case IntegerType =>
            to.addColumnValue(ColumnValue.intValue(from.getInt(ordinal)))
          case BooleanType =>
            to.addColumnValue(ColumnValue.booleanValue(from.getBoolean(ordinal)))
          case DoubleType =>
            to.addColumnValue(ColumnValue.doubleValue(from.getDouble(ordinal)))
          case FloatType =>
            to.addColumnValue(ColumnValue.floatValue(from.getFloat(ordinal)))
          case DecimalType =>
            val hiveDecimal = from.get(ordinal).asInstanceOf[BigDecimal].bigDecimal
            to.addColumnValue(ColumnValue.stringValue(new HiveDecimal(hiveDecimal)))
          case LongType =>
            to.addColumnValue(ColumnValue.longValue(from.getLong(ordinal)))
          case ByteType =>
            to.addColumnValue(ColumnValue.byteValue(from.getByte(ordinal)))
          case ShortType =>
            to.addColumnValue(ColumnValue.shortValue(from.getShort(ordinal)))
          case TimestampType =>
            to.addColumnValue(
              ColumnValue.timestampValue(from.get(ordinal).asInstanceOf[Timestamp]))
          case BinaryType | _: ArrayType | _: StructType | _: MapType =>
            val hiveString = result
              .queryExecution
              .asInstanceOf[HiveContext#QueryExecution]
              .toHiveString((from.get(ordinal), dataTypes(ordinal)))
            to.addColumnValue(ColumnValue.stringValue(hiveString))
        }
      }

      def addNullColumnValue(from: SparkRow, to: Row, ordinal: Int) {
        dataTypes(ordinal) match {
          case StringType =>
            to.addString(null)
          case IntegerType =>
            to.addColumnValue(ColumnValue.intValue(null))
          case BooleanType =>
            to.addColumnValue(ColumnValue.booleanValue(null))
          case DoubleType =>
            to.addColumnValue(ColumnValue.doubleValue(null))
          case FloatType =>
            to.addColumnValue(ColumnValue.floatValue(null))
          case DecimalType =>
            to.addColumnValue(ColumnValue.stringValue(null: HiveDecimal))
          case LongType =>
            to.addColumnValue(ColumnValue.longValue(null))
          case ByteType =>
            to.addColumnValue(ColumnValue.byteValue(null))
          case ShortType =>
            to.addColumnValue(ColumnValue.intValue(null))
          case TimestampType =>
            to.addColumnValue(ColumnValue.timestampValue(null))
          case BinaryType | _: ArrayType | _: StructType | _: MapType =>
            to.addColumnValue(ColumnValue.stringValue(null: String))
        }
      }

      def getResultSetSchema: TableSchema = {
        logInfo(s"Result Schema: ${result.queryExecution.analyzed.output}")
        if (result.queryExecution.analyzed.output.size == 0) {
          new TableSchema(new FieldSchema("Result", "string", "") :: Nil)
        } else {
          val schema = result.queryExecution.analyzed.output.map { attr =>
            new FieldSchema(attr.name, HiveMetastoreTypes.toMetastoreType(attr.dataType), "")
          }
          new TableSchema(schema)
        }
      }

      def run(): Unit = {
        logInfo(s"Running query '$statement'")
        setState(OperationState.RUNNING)
        try {
          result = hiveContext.sql(statement)
          logDebug(result.queryExecution.toString())
          result.queryExecution.logical match {
            case SetCommand(Some(key), Some(value)) if (key == SQLConf.THRIFTSERVER_POOL) =>
              sessionToActivePool(parentSession) = value
              logInfo(s"Setting spark.scheduler.pool=$value for future statements in this session.")
            case _ =>
          }

          val groupId = round(random * 1000000).toString
          hiveContext.sparkContext.setJobGroup(groupId, statement)
          sessionToActivePool.get(parentSession).foreach { pool =>
            hiveContext.sparkContext.setLocalProperty("spark.scheduler.pool", pool)
          }
          iter = {
            val resultRdd = result.queryExecution.toRdd
            val useIncrementalCollect =
              hiveContext.getConf("spark.sql.thriftServer.incrementalCollect", "false").toBoolean
            if (useIncrementalCollect) {
              resultRdd.toLocalIterator
            } else {
              resultRdd.collect().iterator
            }
          }
          dataTypes = result.queryExecution.analyzed.output.map(_.dataType).toArray
          setHasResultSet(true)
        } catch {
          // Actually do need to catch Throwable as some failures don't inherit from Exception and
          // HiveServer will silently swallow them.
          case e: Throwable =>
            logError("Error executing query:",e)
            throw new HiveSQLException(e.toString)
        }
        setState(OperationState.FINISHED)
      }
    }

   handleToOperation.put(operation.getHandle, operation)
   operation
  }

  override def newGetCatalogsOperation(parentSession: HiveSession): GetCatalogsOperation = {
    val operation: GetCatalogsOperation = new GetCatalogsOperation(parentSession){
      override def run(): Unit = {
        println("Running GetCatalogsOperation")
        super.run()
        val rowSet = ReflectionUtils.getSuperField[RowSet](this, "rowSet")
        val rows = ReflectionUtils.getPrivateField[java.util.ArrayList[Row]](rowSet, "rows")
        println(rows)
        //setSuperField(obj : Object, fieldName: String, fieldValue: Object)
      }
    }

    handleToOperation.put(operation.getHandle, operation)
    return operation
  }

  override def newGetSchemasOperation(parentSession: HiveSession, catalogName: String, schemaName: String): GetSchemasOperation = {
    val operation: GetSchemasOperation = new GetSchemasOperation(parentSession, catalogName, schemaName){
      val schemaNameI = schemaName
      private val RESULT_SET_SCHEMA = ReflectionUtils.getSuperField[TableSchema](this, "RESULT_SET_SCHEMA")

      override def run(): Unit = {
        println("Running GetSchemasOperation")
        super.run()
        val rowSet = ReflectionUtils.getSuperField[RowSet](this, "rowSet")
        val rows = ReflectionUtils.getPrivateField[util.ArrayList[Row]](rowSet, "rows")
        printRows(rows)
        val patternString:String = if(schemaNameI.isEmpty){
          "(.*)"
        } else{
          val subpatterns: Array[String] = convertPattern(schemaNameI).trim.split("\\|")
          s"(?i)(${subpatterns.mkString(" || ")})"
        }

        println(s"PATTERN: $patternString")

        val pattern = patternString.r

        val cassandraProperties = CassandraProperties(hiveContext.sparkContext)

        val cassandraMeta = CassandraSchemaHelper.getCassandraMetadata(cassandraProperties.cassandraHost,
          cassandraProperties.cassandraNativePort,
          cassandraProperties.cassandraUsername,
          cassandraProperties.cassandraPassword)

        if(cassandraMeta != null){
          cassandraMeta.getKeyspaces.map(_.getName).foreach {
            case pattern(dbname) =>
              println(s"Adding $dbname")
              rowSet.addRow(RESULT_SET_SCHEMA, Array(dbname, ""));
            case _ => //Do nothing
          }
        }

        ReflectionUtils.setSuperField(this, "rowSet", rowSet)
      }
    }
    handleToOperation.put(operation.getHandle, operation)
    operation
  }

  def printRows(rows: util.ArrayList[Row]) {
    println("[[")
    rows.foreach(println)
    println("]]")
  }

  private def convertPattern(pattern: String): String = {
  val wStr = ".*"
  pattern.replaceAll("([^\\\\])%", "$1" + wStr).replaceAll("\\\\%", "%").replaceAll("^%", wStr)
}

  override def newGetTablesOperation(parentSession: HiveSession, catalogName: String, schemaName: String, tableName: String, tableTypes: util.List[String]): MetadataOperation = {
    val operation: MetadataOperation = new GetTablesOperation(parentSession, catalogName, schemaName, tableName, tableTypes) {
      private val RESULT_SET_SCHEMA = ReflectionUtils.getSuperField[TableSchema](this, "RESULT_SET_SCHEMA")

      val schemaNameI = schemaName
      val tableNameI = tableName

      override def run(): Unit = {
        println("Running GetTablesOperation")
        super.run()
        val rowSet = ReflectionUtils.getSuperField[RowSet](this, "rowSet")
        val rows = ReflectionUtils.getPrivateField[java.util.ArrayList[Row]](rowSet, "rows")
        printRows(rows)

        val schemaPattern = if(schemaNameI.isEmpty){
          "(.*)"
        } else{
          val subpatterns: Array[String] = convertPattern(schemaNameI).trim.split("\\|")
          s"(?i)(${subpatterns.mkString(" || ")})"
        }


        val cassandraProperties = CassandraProperties(hiveContext.sparkContext)

        val cassandraMeta = CassandraSchemaHelper.getCassandraMetadata(cassandraProperties.cassandraHost,
          cassandraProperties.cassandraNativePort,
          cassandraProperties.cassandraUsername,
          cassandraProperties.cassandraPassword)

        if (cassandraMeta != null) {
          val tablePattern = if(tableNameI == null || tableNameI.isEmpty){
            "(.*)"
          } else{
            val subpatterns: Array[String] = convertPattern(schemaNameI).trim.split("\\|")
            s"(?i)(${subpatterns.mkString(" || ")})"
          }


          cassandraMeta.getKeyspaces.filter(k => (!k.getName.startsWith("system")) && k.getName.matches(schemaPattern)).foreach {
            ks =>
              ks.getTables.filter(_.getName.matches(tablePattern)).foreach {
                tbl =>
                  println(s"Adding table: ${tbl.getName}")
                  val rowData: Array[AnyRef] = Array[AnyRef](
                    "",
                    "",
                    s"${ks.getName}.${tbl.getName}",
                    "CASSANDRA_TABLES",
                    tbl.getOptions.getComment)

                  rowSet.addRow(RESULT_SET_SCHEMA, rowData)
              }
          }

          ReflectionUtils.setSuperField(this, "rowSet", rowSet)
        }
      }
    }
    handleToOperation.put(operation.getHandle, operation)
    operation
  }

  override def newGetTableTypesOperation(parentSession: HiveSession): GetTableTypesOperation = {
    val operation: GetTableTypesOperation = new GetTableTypesOperation(parentSession){
      private val RESULT_SET_SCHEMA = ReflectionUtils.getSuperField[TableSchema](this, "RESULT_SET_SCHEMA")

      override def run(): Unit = {
        println("Running GetTableTypesOperation")
        super.run()
        val rowSet = ReflectionUtils.getSuperField[RowSet](this, "rowSet")
        val rows = ReflectionUtils.getPrivateField[java.util.ArrayList[Row]](rowSet, "rows")
        printRows(rows)
        rowSet.addRow(RESULT_SET_SCHEMA, Array("CASSANDRA_TABLES"))

        ReflectionUtils.setSuperField(this, "rowSet", rowSet)
      }
    }
    handleToOperation.put(operation.getHandle, operation)
    operation
  }

  override def newGetColumnsOperation(parentSession: HiveSession, catalogName: String, schemaName: String, tableName: String, columnName: String): GetColumnsOperation = {
    val operation: GetColumnsOperation = new GetColumnsOperation(parentSession, catalogName, schemaName, tableName, columnName){
      private val RESULT_SET_SCHEMA = ReflectionUtils.getSuperField[TableSchema](this, "RESULT_SET_SCHEMA")

      override def run(): Unit = {
        println(s"Running GetColumnsOperation $schemaName : $tableName")

        val keyspace = if(schemaName == null || schemaName.isEmpty || schemaName.equals("*")) {
          val splits = tableName.split("\\.")
          splits(0)
        }else {
          schemaName
        }
        println(s"Checking keyspace: $keyspace")

        val cassandraProperties = CassandraProperties(hiveContext.sparkContext)

        val cassandraMeta = CassandraSchemaHelper.getCassandraMetadata(cassandraProperties.cassandraHost,
          cassandraProperties.cassandraNativePort,
          cassandraProperties.cassandraUsername,
          cassandraProperties.cassandraPassword)

        val keyspaceMeta: KeyspaceMetadata = if(cassandraMeta != null) cassandraMeta.getKeyspace(keyspace) else null
        println(keyspaceMeta)
        if (keyspaceMeta != null) {
          val rowSet = new RowSet()
          val tableStr = if(tableName.contains(".")) tableName.split("\\.")(1) else tableName
          setState(OperationState.RUNNING)
          val table = keyspaceMeta.getTable(tableStr)

          table.getColumns.zipWithIndex.foreach {
            case (column, idx) =>
            val rowData: Array[AnyRef] = Array(
              null,
              "",
              s"$keyspace.$tableStr",
              column.getName,
              toJavaSqlType(column.getType),
              column.getType.getName.toString,
              getTypeSize(column.getType),
              null,
              getDecimalDigits(column.getType),
              getNumPrecRadix(column.getType),
              new Integer(DatabaseMetaData.columnNullable),
              "",
              null,
              null,
              null,
              null,
              new Integer(idx),
              "YES",
              null,
              null,
              null,
              null,
              "NO")

            rowSet.addRow(RESULT_SET_SCHEMA, rowData)
          }

          ReflectionUtils.setSuperField(this, "rowSet", rowSet)
          setState(OperationState.FINISHED)
        } else {
          super.run()
          val rowSet = ReflectionUtils.getSuperField[RowSet](this, "rowSet")
          val rows = ReflectionUtils.getPrivateField[java.util.ArrayList[Row]](rowSet, "rows")
          printRows(rows)
        }
      }
    }
    handleToOperation.put(operation.getHandle, operation)
    operation
  }

  import com.datastax.driver.core.{DataType => CassandraDataType}
  private def toJavaSqlType(dataType: CassandraDataType): Integer = {
    dataType.getName match {
      case CassandraDataType.Name.ASCII => java.sql.Types.VARCHAR
      case CassandraDataType.Name.BIGINT => java.sql.Types.BIGINT
      case CassandraDataType.Name.BLOB => java.sql.Types.BLOB
      case CassandraDataType.Name.BOOLEAN => java.sql.Types.BOOLEAN
      case CassandraDataType.Name.COUNTER => java.sql.Types.BIGINT
      case CassandraDataType.Name.DECIMAL => java.sql.Types.DECIMAL
      case CassandraDataType.Name.DOUBLE => java.sql.Types.DOUBLE
      case CassandraDataType.Name.FLOAT => java.sql.Types.FLOAT
      case CassandraDataType.Name.INT => java.sql.Types.INTEGER
      case CassandraDataType.Name.TEXT => java.sql.Types.VARCHAR
      case CassandraDataType.Name.VARINT => java.sql.Types.DECIMAL //Big Integer is treated as BigDecimal by Catalyst
      case CassandraDataType.Name.INET => java.sql.Types.VARCHAR //TODO: Stopgap solution
      case CassandraDataType.Name.UUID => java.sql.Types.VARCHAR //TODO: Stopgap solution
      case CassandraDataType.Name.TIMEUUID => java.sql.Types.VARCHAR //TODO: Stopgap solution
      case CassandraDataType.Name.TIMESTAMP => java.sql.Types.TIMESTAMP
      case CassandraDataType.Name.CUSTOM => java.sql.Types.BLOB //TODO: Stopgap solution. Explore use of UDF
      case CassandraDataType.Name.MAP => java.sql.Types.VARCHAR
      case CassandraDataType.Name.SET => java.sql.Types.VARCHAR
      case CassandraDataType.Name.LIST => java.sql.Types.VARCHAR
      case CassandraDataType.Name.VARCHAR => java.sql.Types.VARCHAR
    }
  }

  private def getTypeSize(dataType: CassandraDataType): Integer = {
    dataType.getName match {
      case CassandraDataType.Name.ASCII => Integer.MAX_VALUE
      case CassandraDataType.Name.VARCHAR => Integer.MAX_VALUE
      case CassandraDataType.Name.BLOB => Integer.MAX_VALUE
      case CassandraDataType.Name.TIMESTAMP => 30
      case _ => null
    }
  }

  private def getDecimalDigits(dataType: CassandraDataType): Integer = {
    dataType.getName match {
      case CassandraDataType.Name.BIGINT => 0
      case CassandraDataType.Name.BOOLEAN => 0
      case CassandraDataType.Name.COUNTER => 0
      case CassandraDataType.Name.INT => 0
      case CassandraDataType.Name.FLOAT => 7
      case CassandraDataType.Name.DOUBLE => 15
      case _ => null
    }
  }


  private def getPrecision(dataType: CassandraDataType): Integer = {
    dataType.getName match {
      case CassandraDataType.Name.INT => 10
      case CassandraDataType.Name.BIGINT => 19
      case CassandraDataType.Name.FLOAT => 7
      case CassandraDataType.Name.DOUBLE => 15
      case _ => null
    }
  }

  /**
   * Scale for this type.
   */
  private def getScale(dataType: CassandraDataType): Integer = {
    dataType.getName match {
      case CassandraDataType.Name.BIGINT => 0
      case CassandraDataType.Name.BOOLEAN => 0
      case CassandraDataType.Name.COUNTER => 0
      case CassandraDataType.Name.INT => 0
      case CassandraDataType.Name.FLOAT => 7
      case CassandraDataType.Name.DOUBLE => 15
      case CassandraDataType.Name.DECIMAL => Integer.MAX_VALUE
      case CassandraDataType.Name.VARINT => Integer.MAX_VALUE
      case _ => null
    }
  }

  private def getNumPrecRadix(dataType: CassandraDataType): Integer = {
    dataType.getName match {
      case CassandraDataType.Name.BIGINT => 10
      case CassandraDataType.Name.COUNTER => 10
      case CassandraDataType.Name.INT => 10
      case CassandraDataType.Name.DECIMAL => 10
      case CassandraDataType.Name.VARINT => 10
      case CassandraDataType.Name.FLOAT => 2
      case CassandraDataType.Name.DOUBLE => 2
      case _ => null
    }
  }

}
