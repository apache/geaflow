/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.geaflow.dsl.optimize;


import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.rules.AggregateProjectPullUpConstantsRule;
import org.apache.calcite.rel.rules.AggregateRemoveRule;
import org.apache.calcite.rel.rules.FilterAggregateTransposeRule;
import org.apache.calcite.rel.rules.FilterCorrelateRule;
import org.apache.calcite.rel.rules.FilterMergeRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.JoinPushExpressionsRule;
import org.apache.calcite.rel.rules.ProjectFilterTransposeRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.ProjectSortTransposeRule;
import org.apache.calcite.rel.rules.ProjectToWindowRule;
import org.apache.calcite.rel.rules.PruneEmptyRules;
import org.apache.calcite.rel.rules.ReduceExpressionsRule;
import org.apache.calcite.rel.rules.SortRemoveRule;
import org.apache.calcite.rel.rules.UnionEliminatorRule;
import org.apache.calcite.rel.rules.UnionToDistinctRule;
import org.apache.geaflow.dsl.optimize.rule.*;

public class OptimizeRules {

    private static final List<RelOptRule> PRE_REWRITE_RULES = ImmutableList.of();

    private static final List<RelOptRule> LOGICAL_RULES = ImmutableList.of(
        ReduceExpressionsRule.FILTER_INSTANCE,
        ReduceExpressionsRule.PROJECT_INSTANCE,
        ReduceExpressionsRule.JOIN_INSTANCE,
        FilterMergeRule.INSTANCE,
        FilterAggregateTransposeRule.INSTANCE,
        ProjectToWindowRule.PROJECT,
        ProjectToWindowRule.INSTANCE,
        FilterCorrelateRule.INSTANCE,
        GQLAggregateProjectMergeRule.INSTANCE,
        AggregateProjectPullUpConstantsRule.INSTANCE,
        ProjectMergeRule.INSTANCE,
        ProjectSortTransposeRule.INSTANCE,
        JoinPushExpressionsRule.INSTANCE,
        UnionToDistinctRule.INSTANCE,
        AggregateRemoveRule.INSTANCE,
        SortRemoveRule.INSTANCE,
        PruneEmptyRules.AGGREGATE_INSTANCE,
        PruneEmptyRules.FILTER_INSTANCE,
        PruneEmptyRules.JOIN_LEFT_INSTANCE,
        PruneEmptyRules.JOIN_RIGHT_INSTANCE,
        PruneEmptyRules.PROJECT_INSTANCE,
        PruneEmptyRules.SORT_INSTANCE,
        PruneEmptyRules.UNION_INSTANCE,
        ProjectFilterTransposeRule.INSTANCE,
        FilterProjectTransposeRule.INSTANCE,
        GQLProjectRemoveRule.INSTANCE,
        UnionEliminatorRule.INSTANCE,
        GQLMatchUnionMergeRule.INSTANCE,
        MatchSortToLogicalSortRule.INSTANCE,
        PathModifyMergeRule.INSTANCE,
        AddVertexResetRule.INSTANCE,
        PushJoinFilterConditionRule.INSTANCE,
        PushConsecutiveJoinConditionRule.INSTANCE,
        TableJoinTableToGraphRule.INSTANCE,
        MatchJoinMatchMergeRule.INSTANCE,
        MatchJoinTableToGraphMatchRule.INSTANCE,
        TableJoinMatchToGraphMatchRule.INSTANCE,
        MatchJoinMatchMergeRule.INSTANCE,
        FilterToMatchRule.INSTANCE,
        // Issue #363 optimization rules - ID filter optimization and join reordering
        IdFilterPushdownRule.INSTANCE,  // Push ID filters down early for direct index lookup
        FilterMatchNodeTransposeRule.INSTANCE,
        MatchFilterMergeRule.INSTANCE,
        TableScanToGraphRule.INSTANCE,
        MatchIdFilterSimplifyRule.INSTANCE,
        AnchorNodePriorityRule.INSTANCE,  // Identify and prioritize anchor nodes with ID filters
        GraphJoinReorderRule.INSTANCE,  // Reorder joins based on anchor node selectivity
        MatchEdgeLabelFilterRemoveRule.INSTANCE,
        GraphMatchFieldPruneRule.INSTANCE,
        ProjectFieldPruneRule.INSTANCE
    );

    private static final List<RelOptRule> POST_OPTIMIZE_RULES = ImmutableList.of(
        PathInputReplaceRule.INSTANCE
    );

    public static final List<RuleGroup> RULE_GROUPS = ImmutableList.of(
        RuleGroup.of(PRE_REWRITE_RULES, 10),
        RuleGroup.of(LOGICAL_RULES, 5),
        RuleGroup.of(POST_OPTIMIZE_RULES, 0)
    );
}
