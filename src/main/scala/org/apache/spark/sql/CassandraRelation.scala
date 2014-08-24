package org.apache.spark.sql

import com.datastax.driver.core._
import com.twitter.chill.MeatLocker
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.LeafNode

import scala.collection.JavaConversions._


case class CassandraRelation(host: String, nativePort: String, rpcPort: String, keyspace: String, table: String, @transient conf: Option[Configuration] = None)
  extends LeafNode with MultiInstanceRelation {

  @transient private val cassandraSchema: TableMetadata = getCassandraSchema(host, nativePort, keyspace, table)

  private[sql] val partitionKeys: List[String] = cassandraSchema.getPartitionKey.map(_.getName).toList

  private[sql] val clusteringKeys: List[String] = cassandraSchema.getClusteringColumns.map(_.getName).toList

  //private[sql] val columns: List[String] = cassandraSchema.getColumns.map(_.getName).toList

  private[sql] val columns: Map[String, SerCassandraDataType] = cassandraSchema.getColumns.map(c => c.getName -> SerCassandraDataType.fromDataType(c.getType)).toMap

  private val indexes: List[String] = cassandraSchema.getColumns.filter(_.getIndex != null).map(_.getName).toList diff partitionKeys

  override def newInstance() = new CassandraRelation(host, nativePort, rpcPort, keyspace, table, conf).asInstanceOf[this.type]

  override val output: Seq[Attribute] = CassandraTypeConverter.convertToAttributes(cassandraSchema)

  def pushdownPredicates(filters: Seq[Expression]): PushdownFilters = {

    //Get the defined equal filters
    val equals = filters.map(mapEqualsToColumnNames).filter(_.isDefined).map(_.get)

    val equalFilters: Seq[String] = equals.map(_._1)
    //Get the clustering keys in the equal list
    val equalsOnClusteringKeys = clusteringKeys.intersect(equalFilters).toList

    val clusteringKeysToUse: List[String] = getClusterKeysToUse(clusteringKeys, equalsOnClusteringKeys)

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

  private def getCassandraSchema(host: String, port: String, keyspace: String, columnFamily: String): TableMetadata = {
    require(keyspace != null, "Unable to read schema: keyspace is null")
    require(columnFamily != null, "Unable to read schema: columnFamily is null")

    val driver = new Cluster.Builder().addContactPoint(host).withPort(port.toInt).build().connect()
    val clusterMeta: Metadata = driver.getCluster.getMetadata
    val keyspaceMeta: KeyspaceMetadata = clusterMeta.getKeyspace( s""""${keyspace}"""")
    val tableMeta = keyspaceMeta.getTable( s""""${columnFamily}"""")
    tableMeta
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
}


case class PushdownFilters(filtersToPushdown: Seq[Expression], filtersToRetain: Seq[Expression])

case class SerCassandraDataType(dataType: DataType.Name, param1: Option[DataType.Name], param2: Option[DataType.Name])

object SerCassandraDataType {
  def fromDataType(dt: DataType): SerCassandraDataType = {
    if(dt.isCollection){
      dt.getName match {
        case DataType.Name.MAP =>
          val params = dt.getTypeArguments
          SerCassandraDataType(dt.getName, Some(params(0).getName), Some(params(1).getName))
        case DataType.Name.SET =>
          val params = dt.getTypeArguments
          SerCassandraDataType(dt.getName, Some(params(0).getName), None)
        case DataType.Name.LIST =>
          val params = dt.getTypeArguments
          SerCassandraDataType(dt.getName, Some(params(0).getName), None)
      }
    } else {
      SerCassandraDataType(dt.getName, None, None)
    }
  }
}