package com.tuplejump.calliope.sql

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.optimizer._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.{Rule, RuleExecutor}

object StargateOptimizerHelper {

  object StargateRewriter extends Rule[LogicalPlan] with Logging {
    def apply(plan: LogicalPlan): LogicalPlan = {
      logInfo("I AM THE OPTIMIZER")
      plan
    }
  }

}

object StargateOptimizer extends RuleExecutor[LogicalPlan] {
  val batches =
    Batch("Combine Limits", FixedPoint(100),
      CombineLimits) ::
      Batch("ConstantFolding", FixedPoint(100),
        NullPropagation,
        ConstantFolding,
        LikeSimplification,
        BooleanSimplification,
        SimplifyFilters,
        SimplifyCasts,
        SimplifyCaseConversionExpressions) ::
      Batch("Filter Pushdown", FixedPoint(100),
        CombineFilters,
        PushPredicateThroughProject,
        PushPredicateThroughJoin,
        ColumnPruning) ::
      Batch("Stargate Rewriter", FixedPoint(100),
        StargateOptimizerHelper.StargateRewriter) :: Nil
}