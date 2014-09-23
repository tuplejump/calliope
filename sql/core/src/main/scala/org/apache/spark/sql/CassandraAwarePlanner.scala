package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan

protected[sql] trait CassandraAwarePlanner {
  self: SQLContext#SparkPlanner =>

  object CassandraOperations extends Strategy {
    override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case PhysicalOperation(projectList, filters: Seq[Expression], relation: CassandraRelation) =>
        val pushdownFilters = relation.pushdownPredicates(filters)
        val scan: (Seq[Attribute]) => SparkPlan = CassandraTableScan(_, relation, pushdownFilters.filtersToPushdown)
        pruneFilterProject(projectList, filters, { f => pushdownFilters.filtersToRetain}, scan) :: Nil

      case SaveToCassandra(host, nativePort, rpcPort, keyspace, table, username, password, logicalPlan) =>
        val relation = CassandraRelation(host, nativePort, rpcPort, keyspace, table, sqlContext,
          username, password, false, Some(sparkContext.hadoopConfiguration))
        WriteToCassandra(relation, planLater(logicalPlan)) :: Nil

      case logical.InsertIntoTable(relation: CassandraRelation, partition, child, overwrite) =>
        WriteToCassandra(relation, planLater(child)) :: Nil

      case ops =>
        logInfo(s"Cassandra Operations doesn't handle $ops")
        Nil
    }

    def selectFilters(relation: CassandraRelation)(condition: Expression => Boolean): (Seq[Expression]) => Seq[Expression] = {
      filters =>
        filters.filter(condition)
    }
  }
}
