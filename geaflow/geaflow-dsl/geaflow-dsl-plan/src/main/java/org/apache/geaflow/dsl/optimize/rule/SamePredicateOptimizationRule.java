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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rex.RexNode;
import org.apache.geaflow.dsl.rel.match.MatchFilter;
import org.apache.geaflow.dsl.rel.match.MatchSamePredicate;
import org.apache.geaflow.dsl.rel.match.MatchUnion;

/**
 * Optimization rule for same predicate patterns.
 * This rule converts MatchSamePredicate to a more efficient MatchUnion + MatchFilter combination.
 * 
 * The transformation is:
 * MatchSamePredicate(left, right, condition, distinct) 
 * -> MatchFilter(MatchUnion(left, right, distinct), condition)
 */
public class SamePredicateOptimizationRule extends RelOptRule {

    /**
     * Singleton instance of the rule
     */
    public static final SamePredicateOptimizationRule INSTANCE = new SamePredicateOptimizationRule();

    /**
     * Private constructor for singleton pattern
     */
    private SamePredicateOptimizationRule() {
        super(operand(MatchSamePredicate.class, any()));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        MatchSamePredicate samePredicate = call.rel(0);
        
        // Create union operation from left and right path patterns
        MatchUnion union = MatchUnion.create(
            samePredicate.getCluster(),
            samePredicate.getTraitSet(),
            samePredicate.getInputs(),
            samePredicate.isDistinct()
        );
        
        // Apply the shared predicate condition as a filter
        MatchFilter filter = MatchFilter.create(
            union,
            samePredicate.getCondition(),
            samePredicate.getPathSchema()
        );
        
        // Transform the original same predicate to the optimized union + filter
        call.transformTo(filter);
    }

    // Rule name for debugging and logging
    public String getRuleName() {
        return "SamePredicateOptimizationRule";
    }
}
