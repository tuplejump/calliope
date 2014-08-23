package org.apache.spark.sql

import java.sql.Timestamp

import com.datastax.driver.core.{DataType => CassandraDataType}
import com.tuplejump.calliope.CasBuilder
import com.tuplejump.calliope.sql.CassandraRelation
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.execution.{LeafNode, SparkPlan}

import scala.collection.JavaConversions._


class CassandraQueryContext(sc: SparkContext) extends SQLContext(sc) {

  self =>

  override protected[sql] val planner: SparkPlanner = new CassandraAwarePlanner()

  def cassandraTable(keyspace: String, table: String): SchemaRDD = cassandraTable("127.0.0.1", "9042", keyspace, table)

  def cassandraTable(host: String, port: String, keyspace: String, table: String): SchemaRDD = {
    new SchemaRDD(this, CassandraRelation(host, port, keyspace, table, Some(sparkContext.hadoopConfiguration)))
  }


  protected[sql] class CassandraAwarePlanner extends SparkPlanner {
    override val strategies: Seq[Strategy] = CommandStrategy(self) ::
      TakeOrdered ::
      PartialAggregation ::
      LeftSemiJoin ::
      HashJoin ::
      InMemoryScans ::
      CassandraOperations ::
      ParquetOperations ::
      BasicOperators ::
      CartesianProduct ::
      BroadcastNestedLoopJoin :: Nil


    object CassandraOperations extends Strategy {
      override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
        case PhysicalOperation(projectList, filters: Seq[Expression], relation: CassandraRelation) =>

          val pushdownFilters = relation.pushdownPredicates(filters)

          val scan: (Seq[Attribute]) => SparkPlan = CassandraTableScan(_, relation, pushdownFilters.filtersToPushdown)(sqlContext)

          pruneFilterProject(projectList, filters, { f => pushdownFilters.filtersToRetain}, scan) :: Nil

        case _ => Nil
      }

      def selectFilters(relation: CassandraRelation)(condition: Expression => Boolean): (Seq[Expression]) => Seq[Expression] = {
        filters =>
          filters.filter(condition)
      }
    }

  }

}

import com.datastax.driver.core.{Row => CassandraRow}

case class CassandraTableScan(
                               // note: output cannot be transient, see
                               // https://issues.apache.org/jira/browse/SPARK-1367
                               output: Seq[Attribute],
                               relation: CassandraRelation,
                               filters: Seq[Expression])(
                               @transient val sqlContext: SQLContext) extends LeafNode {

  override def execute(): RDD[Row] = {

    import com.tuplejump.calliope.Implicits._

    println(s"Predicates: $filters")

    implicit val cassandraRow2sparkRow: CassandraRow => Row = {
      row =>
        new GenericRow(CassandraSparkRowConvertor.build(row))
    }

    val keyString: String = relation.partitionKeys.mkString(",")

    println(s"Filters to Use: $filters")

    val baseQuery = s"SELECT * FROM ${relation.keyspace}.${relation.columnFamily} WHERE token($keyString) > ? AND token($keyString) < ?"

    val queryToUse = if (filters.length <= 0) {
      baseQuery
    } else {
      val filterString = filters.map {
        case EqualTo(left: NamedExpression, right: Literal) => Some(buildQueryString("=", left, right))
        case EqualTo(Cast(left: NamedExpression, _), right: Literal) => Some(buildQueryString("=", left, right))
        case p@LessThan(left: NamedExpression, right: Literal) => Some(buildQueryString("<", left, right))
        case p@LessThan(Cast(left: NamedExpression, _), right: Literal) => Some(buildQueryString("<", left, right))
        case p@GreaterThan(left: NamedExpression, right: Literal) => Some(buildQueryString(">", left, right))
        case p@GreaterThan(Cast(left: NamedExpression, _), right: Literal) => Some(buildQueryString(">", left, right))
      }.filter(_.isDefined).map(_.get).mkString(" AND ")

      println(filterString)
      s"$baseQuery AND $filterString"
    } + " ALLOW FILTERING"

    println(queryToUse)

    val cas = CasBuilder.native
      .withColumnFamilyAndQuery(relation.keyspace, relation.columnFamily, queryToUse)
      .onHost(relation.host)
      .onNativePort(relation.port)
      .onPort("9160")
      .mergeRangesInMultiRangeSplit(256)

    sqlContext.sparkContext.nativeCassandra[Row](cas)
  }

  private def buildQueryString(comparatorSign: String, expr: NamedExpression, literal: Literal): String = {
    literal.dataType match {
      case BooleanType =>
        s"${expr.name} ${comparatorSign} ${literal.value.asInstanceOf[Boolean]}"
      case IntegerType =>
        s"${expr.name} ${comparatorSign} ${literal.value.asInstanceOf[Integer]}"
      case LongType =>
        s"${expr.name} ${comparatorSign} ${literal.value.asInstanceOf[Long]}"
      case DoubleType =>
        s"${expr.name} ${comparatorSign} ${literal.value.asInstanceOf[Double]}"
      case FloatType =>
        s"${expr.name} ${comparatorSign} ${literal.value.asInstanceOf[Float]}"
      case StringType =>
        s"${expr.name} ${comparatorSign} '${literal.value.asInstanceOf[String]}'"
      case _ =>
        s"${expr.name} ${comparatorSign} '${literal.value.asInstanceOf[String]}'"
    }
  }
}

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
      case CassandraDataType.Name.CUSTOM => crow.getBytes(name).array() //TODO: Stopgap solution. Should be struct.
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


/* object CassandraFilterBuilder {

  import org.apache.spark.sql.catalyst.expressions.Expression

  def createFilter(expression: Expression): Option[CassandraFilter] = {

  }
}

case class CassandraFilter(expr1: String, expr2: String, condition: String) */