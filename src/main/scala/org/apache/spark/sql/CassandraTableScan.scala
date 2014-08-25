package org.apache.spark.sql

import com.datastax.driver.core.{Row => CassandraRow}
import com.tuplejump.calliope.CasBuilder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.execution.LeafNode

case class CassandraTableScan(
                               // note: output cannot be transient, see
                               // https://issues.apache.org/jira/browse/SPARK-1367
                               output: Seq[Attribute],
                               relation: CassandraRelation,
                               filters: Seq[Expression])(
                               @transient val sqlContext: SQLContext) extends LeafNode with Logging {

  override def execute(): RDD[Row] = {

    import com.tuplejump.calliope.Implicits._

    implicit val cassandraRow2sparkRow: CassandraRow => Row = {
      row =>
        new GenericRow(CassandraSparkDataConvertor.build(row))
    }

    val projection = if(output.isEmpty) "*" else output.map(_.name).mkString(",")

    println(s"PROJECTION: $projection")

    val keyString: String = relation.partitionKeys.mkString(",")

    val baseQuery = s"SELECT ${projection} FROM ${relation.keyspace}.${relation.table} WHERE token($keyString) > ? AND token($keyString) < ?"

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

      s"$baseQuery AND $filterString"
    } + " ALLOW FILTERING"

    //Logger(queryToUse)
    logger.info(s"Generated CQL: $queryToUse")

    val cas = CasBuilder.native
      .withColumnFamilyAndQuery(relation.keyspace, relation.table, queryToUse)
      .onHost(relation.host)
      .onPort(relation.rpcPort)
      .onNativePort(relation.nativePort)
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
