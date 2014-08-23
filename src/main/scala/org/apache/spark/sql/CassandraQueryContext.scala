package org.apache.spark.sql

import com.datastax.driver.core.{DataType => CassandraDataType}
import com.tuplejump.calliope.sql.CassandraRelation
import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan


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
