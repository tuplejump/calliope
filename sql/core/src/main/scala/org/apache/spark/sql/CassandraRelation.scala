
package org.apache.spark.sql

import com.datastax.driver.core.{Cluster, KeyspaceMetadata, Metadata, TableMetadata, DataType => CassanndraDataType}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.LeafNode

import scala.collection.JavaConversions._

case class CassandraRelation(host: String, nativePort: String,
                             rpcPort: String, keyspace: String, table: String,
                             @transient sqlContext: SQLContext,
                             cassandraUsername: Option[String] = None,
                             cassandraPassword: Option[String] = None,
                             mayUseStartgate: Boolean = false,
                             @transient conf: Option[Configuration] = None)
  extends LeafNode with MultiInstanceRelation {
  @transient private[sql] val cassandraSchema: TableMetadata =
    CassandraSchemaHelper.getCassandraTableSchema(host, nativePort, keyspace, table, cassandraUsername, cassandraPassword)

  assert(cassandraSchema != null, s"Invalid Keyspace [$keyspace] or Table [$table] ")

  private[sql] val partitionKeys: List[String] = cassandraSchema.getPartitionKey.map(_.getName).toList

  private[sql] val clusteringKeys: List[String] = cassandraSchema.getClusteringColumns.map(_.getName).toList

  private[sql] val columns: Map[String, SerCassandraDataType] = cassandraSchema.getColumns.map{
    c => c.getName -> SerCassandraDataType.fromDataType(c.getType)
  }.toMap

  private val indexes: List[String] = cassandraSchema.getColumns.filter(_.getIndex != null).map(_.getName).toList

  override def newInstance() =
    new CassandraRelation(host,
      nativePort,
      rpcPort,
      keyspace,
      table,
      sqlContext,
      cassandraUsername,
      cassandraPassword,
      mayUseStartgate,
      conf).asInstanceOf[this.type]

  override val output: Seq[Attribute] = CassandraTypeConverter.convertToAttributes(cassandraSchema)

  private val isStargatePermitted = mayUseStartgate || (conf match {
    case Some(c) =>
      c.get(CalliopeSqlSettings.enableStargateKey) == "true" || c.get(s"calliope.stargate.$keyspace.$table.enable") == "true"
    case None => false
  })

  private[sql] val stargateIndex: Option[String] = if (isStargatePermitted) {
    cassandraSchema.getColumns.filter(_.getIndex != null).map(_.getIndex).collectFirst {
      case idx if (idx.isCustomIndex && idx.getIndexClassName == "com.tuplejump.stargate.RowIndex") =>
        idx.getIndexedColumn.getName
    }
  } else {
    None
  }

  def pushdownPredicates(filters: Seq[Expression]): PushdownFilters = {
    stargateIndex match {
      case Some(idxColumn) => StargatePushdownHandler.getPushdownFilters(filters)
      case None => CassandraPushdownHandler.getPushdownFilters(filters, partitionKeys, clusteringKeys, indexes)
    }
  }

  //TODO: Find better way of getting estimated result sizes from Cassandra
  override lazy val statistics: Statistics =
    Statistics(sizeInBytes = sqlContext.defaultSizeInBytes)
}

private[sql] object CassandraSchemaHelper {

  private[sql] def getCassandraTableSchema(host: String,
                                           port: String,
                                           keyspace: String,
                                           columnFamily: String,
                                           cassandraUsername: Option[String],
                                           cassandraPassword: Option[String]): TableMetadata = {

    require(keyspace != null, "Unable to read schema: keyspace is null")
    require(columnFamily != null, "Unable to read schema: columnFamily is null")

    val clusterMeta: Metadata = getCassandraMetadata(host, port, cassandraUsername, cassandraPassword)
    val keyspaceMeta: KeyspaceMetadata = clusterMeta.getKeyspace( s""""${keyspace}"""")
    val tableMeta = keyspaceMeta.getTable( s""""${columnFamily}"""")
    tableMeta
  }

  def getCassandraMetadata(host: String, nativePort: String, cassandraUsername: Option[String], cassandraPassword: Option[String]): Metadata = {
    val builder = if(cassandraUsername.isDefined && cassandraPassword.isDefined) {
      new Cluster.Builder()
        .addContactPoint(host)
        .withPort(nativePort.toInt)
      .withCredentials(cassandraUsername.get, cassandraPassword.get)
    }else{
      new Cluster.Builder()
        .addContactPoint(host)
        .withPort(nativePort.toInt)
    }

    val driver = builder.build().connect()

    val clusterMeta: Metadata = driver.getCluster.getMetadata
    clusterMeta
  }
}

private[sql] trait PushdownHandler {
  protected[sql] def mapEqualsToColumnNames: Expression => Option[(String, Expression)] = {
    case p@EqualTo(left: NamedExpression, right: Literal) => Some(left.name -> p)
    case p@EqualTo(Cast(left: NamedExpression, _), right: Literal) => Some(left.name -> p)
    case _ => None
  }

  protected[sql] def mapGtLtToColumnNames: Expression => Option[(String, Expression)] = {
    case p@LessThan(left: NamedExpression, right: Literal) => Some(left.name -> p)
    case p@LessThan(Cast(left: NamedExpression, _), right: Literal) => Some(left.name -> p)
    case p@LessThanOrEqual(left: NamedExpression, right: Literal) => Some(left.name -> p)
    case p@LessThanOrEqual(Cast(left: NamedExpression, _), right: Literal) => Some(left.name -> p)
    case p@GreaterThan(left: NamedExpression, right: Literal) => Some(left.name -> p)
    case p@GreaterThan(Cast(left: NamedExpression, _), right: Literal) => Some(left.name -> p)
    case p@GreaterThanOrEqual(left: NamedExpression, right: Literal) => Some(left.name -> p)
    case p@GreaterThanOrEqual(Cast(left: NamedExpression, _), right: Literal) => Some(left.name -> p)
    case _ => None
  }
}

object StargatePushdownHandler extends PushdownHandler with Logging {
  private[sql] def getPushdownFilters(filters: Seq[Expression]): PushdownFilters = {
    logInfo("Using STARGATE filter")
    val (pushdown, retain) = filters.partition(isSupportedQuery)

    logInfo(s"PUSHDOWN: $pushdown")

    PushdownFilters(pushdown, retain)
  }

  private val isSupportedQuery: Expression => Boolean = {
    case p: Expression if isSupportedSimpleQuery(p) => true
    case p: Expression if isSupportedComplexQuery(p) => true
    case _ => false
  }

  private val isSupportedComplexQuery: Expression => Boolean = {
    case p@Or(left: Expression, right: Expression) =>
      (isSupportedSimpleQuery(right) || isSupportedComplexQuery(right)) &&
        (isSupportedSimpleQuery(left) || isSupportedComplexQuery(left))

    case p@And(left: Expression, right: Expression) =>
      (isSupportedSimpleQuery(right) || isSupportedComplexQuery(right)) &&
        (isSupportedSimpleQuery(left) || isSupportedComplexQuery(left))

    case p@Not(left: Expression) =>
      isSupportedSimpleQuery(left) || isSupportedComplexQuery(left)

    case _ => false
  }

  private val isSupportedSimpleQuery: Expression => Boolean = {
    case p@EqualTo(left: NamedExpression, right: Literal) => true
    case p@EqualTo(Cast(left: NamedExpression, _), right: Literal) => true
    case p@LessThan(left: NamedExpression, right: Literal) => true
    case p@LessThan(Cast(left: NamedExpression, _), right: Literal) => true
    case p@LessThanOrEqual(left: NamedExpression, right: Literal) => true
    case p@LessThanOrEqual(Cast(left: NamedExpression, _), right: Literal) => true
    case p@GreaterThan(left: NamedExpression, right: Literal) => true
    case p@GreaterThan(Cast(left: NamedExpression, _), right: Literal) => true
    case p@GreaterThanOrEqual(left: NamedExpression, right: Literal) => true
    case p@GreaterThanOrEqual(Cast(left: NamedExpression, _), right: Literal) => true
    case p@In(left: NamedExpression, right: Seq[Literal@unchecked]) => true
    case _ => false
  }
}

object CassandraPushdownHandler extends PushdownHandler {
  private[sql] def getPushdownFilters(filters: Seq[Expression], partitionKeys: List[String], clusteringKeys: List[String], allIndexes: List[String]): PushdownFilters = {
    val indexes: List[String] = allIndexes diff partitionKeys

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
}

case class PushdownFilters(filtersToPushdown: Seq[Expression], filtersToRetain: Seq[Expression])

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