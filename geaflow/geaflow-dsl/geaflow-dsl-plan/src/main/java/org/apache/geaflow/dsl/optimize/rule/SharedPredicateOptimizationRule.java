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

package org.apache.geaflow.dsl.optimize.rule;

import java.util.Arrays;
import java.util.List;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.geaflow.dsl.rel.match.MatchFilter;
import org.apache.geaflow.dsl.rel.match.MatchSharedPredicate;
import org.apache.geaflow.dsl.rel.match.MatchUnion;
import org.apache.geaflow.dsl.util.GQLRexUtil;

/**
 * Optimization rule for shared predicate patterns.
 * This rule converts MatchSharedPredicate to a more efficient MatchUnion + MatchFilter combination.
 *
 * The transformation is:
 * MatchSharedPredicate(left, right, condition, distinct) ->
 * MatchFilter(MatchUnion(left, right, distinct), condition)
 */
public class SharedPredicateOptimizationRule extends RelOptRule {

    /**
     * Singleton instance of the rule
     */
    public static final SharedPredicateOptimizationRule INSTANCE = new SharedPredicateOptimizationRule();

    /**
     * Constructor for SharedPredicateOptimizationRule
     */
    private SharedPredicateOptimizationRule() {
        super(operand(MatchSharedPredicate.class, any()));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        MatchSharedPredicate sharedPredicate = call.rel(0);
        
        // Create union of left and right path patterns
        List<RelNode> inputs = Arrays.asList(sharedPredicate.getLeft(), sharedPredicate.getRight());
        MatchUnion union = MatchUnion.create(
            sharedPredicate.getCluster(),
            sharedPredicate.getTraitSet(),
            inputs,
            !sharedPredicate.isDistinct() // MatchUnion uses 'all' parameter (true for union all, false for distinct)
        );

        // Apply the shared predicate condition as a filter
        MatchFilter filter = MatchFilter.create(
            union,
            sharedPredicate.getCondition(),
            sharedPredicate.getPathSchema()
        );

        // Transform the original shared predicate to the optimized union + filter
        call.transformTo(filter);
    }
}
