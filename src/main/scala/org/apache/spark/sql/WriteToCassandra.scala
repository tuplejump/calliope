package org.apache.spark.sql

import com.datastax.driver.core.{DataType => CassandraDataType}
import com.tuplejump.calliope.Implicits._
import com.tuplejump.calliope.Types.CQLRowMap
import org.apache.hadoop.mapreduce.SparkHadoopMapReduceUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{SparkPlan, UnaryNode}

case class WriteToCassandra(relation: CassandraRelation, child: SparkPlan)
                           (@transient val sqlContext: SQLContext) extends UnaryNode with SparkHadoopMapReduceUtil {


  private val columnMap: Map[String, Int] = child.output.zipWithIndex.map { case (a, i) => a.name -> i}.toMap
  private val childRdd = child.execute()

  override def execute(): RDD[Row] = {

    val keyColumns = relation.partitionKeys ++ relation.clusteringKeys
    val columnsWithTypes: List[(String, SerCassandraDataType)] = relation.columns.toList
    val valueColumns = columnsWithTypes.map(_._1) diff keyColumns

    implicit val sparkRow2casRowMap: Row => CQLRowMap = {
      row =>
        columnsWithTypes.view.map {
          case (colName, sdtype) =>
            colName -> CassandraSparkDataConvertor.serializeValue(row(columnMap(colName)), sdtype)
        }.toMap
    }

    childRdd.saveToCas(relation.host, relation.rpcPort, relation.keyspace, relation.table, keyColumns, valueColumns)

    childRdd
  }

  override def output: Seq[Attribute] = child.output

  override def otherCopyArgs = sqlContext :: Nil

}
