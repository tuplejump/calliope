/*
 * Licensed to Tuplejump Software Pvt. Ltd. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Tuplejump Software Pvt. Ltd. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

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