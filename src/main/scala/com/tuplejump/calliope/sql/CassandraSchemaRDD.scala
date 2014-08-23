package com.tuplejump.calliope.sql

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

  val partitionKeys = cassandraSchema.getPartitionKey.map(_.getName).toList

  val clusteringKeys: List[String] = cassandraSchema.getClusteringColumns.map(_.getName).toList

  val indexes: List[String] = cassandraSchema.getColumns.filter(_.getIndex != null).map(_.getName).toList diff partitionKeys

  override def newInstance() = new CassandraRelation(host, port, keyspace, columnFamily, conf).asInstanceOf[this.type]

  override val output: Seq[Attribute] = CassandraTypeConverter.convertToAttributes(cassandraSchema)

  def pushdownPredicates(filters: Seq[Expression]): PushdownFilters = {

    //Get the defined equal filters
    val equals = filters.map(mapEqualsToColumnNames).filter(_.isDefined).map(_.get)

    val equalFilters: Seq[String] = equals.map(_._1)
    //Get the clustering keys in the equal list
    val equalsOnClusteringKeys = clusteringKeys.intersect(equalFilters).toList

    val clusteringKeysToUse: List[String] = getClusterKeysToUse(clusteringKeys, equalsOnClusteringKeys)


    println(s"Clustering Keys: $clusteringKeys")
    println(s"Equals on Clustering Keys: $equalsOnClusteringKeys")
    println(s"Clustering Keys to Use: $clusteringKeysToUse")
    println(s"Indexes: $indexes")

    val equalIndexes: List[String] = equalFilters.intersect(indexes).toList

    val gtlts = filters.map(mapGtLtToColumnNames).filter(_.isDefined).map(_.get)

    val gtltFilters = gtlts.map(_._1)

    //Since we can use NON EQ indexes only if we have atleast one eq index check for that
    val nonEqIndexes: List[String] = if (equalIndexes.isEmpty) {
      List.empty[String]
    } else {
      gtltFilters.intersect(indexes).toList
    }

    // If a gtlt query is on clustering key immediately following the eq keys we can use that
    val nextClusteringKey = if (clusteringKeys.length > clusteringKeysToUse.length) Some(clusteringKeys(clusteringKeysToUse.length)) else None

    val gtltClusteringKey: List[String] = nextClusteringKey match {
      case Some(nck) => if (gtltFilters.contains(nck)) {
        gtltFilters.filter(_ == nck).toList
      } else {
        List.empty[String]
      }
      case None => List.empty[String]
    }

    var possibleFilters = (equals ++ gtlts)

    val pushdownExpr = (clusteringKeysToUse ++ gtltClusteringKey ++ equalIndexes ++ nonEqIndexes).distinct.flatMap {
      col =>
        possibleFilters.filter(_._1 == col)
    }.map(_._2)

    val retainExpr = filters.toList diff pushdownExpr

    PushdownFilters(pushdownExpr, retainExpr)
  }

  private def getClusterKeysToUse(clusteringKeys: List[String], filteredClusteringKeys: List[String], index: Int = 0): List[String] = {
    if (filteredClusteringKeys.isEmpty) {
      List.empty[String]
    } else {
      if (filteredClusteringKeys(0) != clusteringKeys(index)) {
        List.empty[String]
      } else {
        List(filteredClusteringKeys(0)) ::: getClusterKeysToUse(clusteringKeys, filteredClusteringKeys.tail, index + 1)
      }
    }
  }

  private def mapEqualsToColumnNames: Expression => Option[(String, Expression)] = {
    case p@EqualTo(left: NamedExpression, right: Literal) => Some(left.name -> p)
    case p@EqualTo(Cast(left: NamedExpression, _), right: Literal) => Some(left.name -> p)
    case _ => None
  }

  private def mapGtLtToColumnNames: Expression => Option[(String, Expression)] = {
    case p@LessThan(left: NamedExpression, right: Literal) => Some(left.name -> p)
    case p@LessThan(Cast(left: NamedExpression, _), right: Literal) => Some(left.name -> p)
    case p@GreaterThan(left: NamedExpression, right: Literal) => Some(left.name -> p)
    case p@GreaterThan(Cast(left: NamedExpression, _), right: Literal) => Some(left.name -> p)
    case _ => None
  }

  private def canUseFilter(filter: Expression): Boolean = {
    println(s" Checking Filter: $filter")
    filter match {
      case p@EqualTo(left: NamedExpression, right: Literal) if useFilter(left.name) => {
        true
      }
      case p@EqualTo(Cast(left: NamedExpression, _), right: Literal) if useFilter(left.name) => {
        true
      }
      case p@LessThan(left: NamedExpression, right: Literal) if useFilter(left.name) => {
        true
      }
      case p@LessThan(Cast(left: NamedExpression, _), right: Literal) if useFilter(left.name) => {
        true
      }
      case p@GreaterThan(left: NamedExpression, right: Literal) if useFilter(left.name) => {
        true
      }
      case p@GreaterThan(Cast(left: NamedExpression, _), right: Literal) if useFilter(left.name) => {
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

case class PushdownFilters(filtersToPushdown: Seq[Expression], filtersToRetain: Seq[Expression])

private[sql] object CassandraTypeConverter {
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
      case CassandraDataType.Name.TIMESTAMP => TimestampType
      case CassandraDataType.Name.CUSTOM => BinaryType //TODO: Stopgap solution. Explore use of UDF
    }
  }
}