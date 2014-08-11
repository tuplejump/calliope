package com.tuplejump.calliope2

import com.datastax.driver.core.{Cluster, KeyspaceMetadata, Metadata, TableMetadata, DataType => CassandraDataType}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.catalyst.types.{DataType => CatalystDataType, _}

import scala.collection.JavaConversions._

case class CassandraRelation(host: String, port: String, keyspace: String, columnFamily: String, @transient conf: Option[Configuration] = None)
  extends LeafNode with MultiInstanceRelation {
  @transient private val cassandraSchema: TableMetadata = CassandraTypeConverter.getCassandraSchema(host, port, keyspace, columnFamily)

  val partitionKeys = cassandraSchema.getPartitionKey.map(_.getName).toSeq

  val indexes = cassandraSchema.getClusteringColumns.map(_.getName) ++ cassandraSchema.getColumns.filter(_.getIndex != null).map(_.getName)

  override def newInstance() = new CassandraRelation(host, port, keyspace, columnFamily, conf).asInstanceOf[this.type]

  override val output: Seq[Attribute] = CassandraTypeConverter.convertToAttributes(cassandraSchema)

  def canUseFilter(filter: Expression): Boolean = {
    println(filter)
    filter match {
      case p@EqualTo(left: NamedExpression, right: Literal) if useFilter(left.name) => {
        true
      }
      case _ => {
        false
      }
    }
  }

  private def useFilter(column: String): Boolean = {
    println(indexes)
    println("Checking for index on - " + column)
    indexes.contains(column)
  }
}

private[calliope2] object CassandraTypeConverter {
  def getCassandraSchema(host: String, port: String, keyspace: String, columnFamily: String): TableMetadata = {
    require(keyspace != null, "Unable to read schema: keyspace is null")
    require(columnFamily != null, "Unable to read schema: columnFamily is null")

    val driver = new Cluster.Builder().addContactPoint(host).withPort(port.toInt).build().connect()
    val clusterMeta: Metadata = driver.getCluster.getMetadata
    val keyspaceMeta: KeyspaceMetadata = clusterMeta.getKeyspace( s""""${keyspace}"""")
    val tableMeta = keyspaceMeta.getTable( s""""${columnFamily}"""")
    tableMeta
  }

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
    dataType.getName match {
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
    dataType.getName match {
      case CassandraDataType.Name.ASCII => StringType
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
      case CassandraDataType.Name.TIMESTAMP => StringType //TODO: Stopgap solution
      case CassandraDataType.Name.CUSTOM => BinaryType //TODO: Stopgap solution. Should be struct.
    }
  }
}