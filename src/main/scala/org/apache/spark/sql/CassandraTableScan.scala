package org.apache.spark.sql

import com.datastax.driver.core.{Row => CassandraRow}
import com.tuplejump.calliope.CasBuilder
import com.tuplejump.calliope.sql.CassandraRelation
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
