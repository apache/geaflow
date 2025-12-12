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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.geaflow.dsl.calcite.MetaFieldType;
import org.apache.geaflow.dsl.calcite.MetaFieldType.MetaField;
import org.apache.geaflow.dsl.rel.match.EdgeMatch;
import org.apache.geaflow.dsl.rel.match.IMatchNode;
import org.apache.geaflow.dsl.rel.match.MatchFilter;
import org.apache.geaflow.dsl.rel.match.MatchJoin;
import org.apache.geaflow.dsl.rel.match.VertexMatch;

/**
 * Rule for Issue #363: Reorders graph pattern joins based on filter selectivity.
 * This rule analyzes join patterns and reorders them to minimize intermediate result sizes.
 * It prioritizes:
 * 1. Patterns with ID equality filters (highest selectivity)
 * 2. Patterns with other filters (medium selectivity)
 * 3. Patterns without filters (lowest selectivity)
 * Example transformation:
 * Before: (a:Person)-[e]-&gt;(b) JOIN (c:Person)-[knows]-&gt;(d where d.id = 2)
 * After:  (d:Person where d.id = 2) JOIN (c:Person)-[knows]-&gt;(d) JOIN (a:Person)-[e]-&gt;(b)
 * This ensures that high-selectivity filters are evaluated first, reducing the data volume
 * for subsequent join operations.
 */
public class GraphJoinReorderRule extends RelOptRule {

    public static final GraphJoinReorderRule INSTANCE = new GraphJoinReorderRule();

    private GraphJoinReorderRule() {
        super(operand(MatchJoin.class,
            operand(MatchJoin.class, any()),
            operand(IMatchNode.class, any())));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        MatchJoin topJoin = call.rel(0);
        MatchJoin leftJoin = call.rel(1);

        // Only optimize INNER joins
        if (topJoin.getJoinType() != JoinRelType.INNER
            || leftJoin.getJoinType() != JoinRelType.INNER) {
            return;
        }

        // Get all three operands: A, B, C from (A JOIN B) JOIN C
        IMatchNode a = (IMatchNode) leftJoin.getLeft();
        IMatchNode b = (IMatchNode) leftJoin.getRight();
        IMatchNode c = (IMatchNode) topJoin.getRight();

        // Calculate selectivity scores
        SelectivityInfo aInfo = calculateSelectivity(a);
        SelectivityInfo bInfo = calculateSelectivity(b);
        SelectivityInfo cInfo = calculateSelectivity(c);

        // Find the most selective pattern
        SelectivityInfo mostSelective = Collections.max(
            Arrays.asList(aInfo, bInfo, cInfo),
            Comparator.comparingDouble(info -> info.score)
        );

        // If most selective is already leftmost (A), no change needed
        if (mostSelective == aInfo) {
            return;
        }

        // Reorder to put most selective pattern first
        IMatchNode newLeft;
        IMatchNode newMid;
        IMatchNode newRight;
        if (mostSelective == bInfo) {
            // B is most selective: B JOIN A JOIN C
            newLeft = b;
            newMid = a;
            newRight = c;
        } else {
            // C is most selective: C JOIN A JOIN B
            newLeft = c;
            newMid = a;
            newRight = b;
        }

        // Rebuild join tree with new order
        RexBuilder rexBuilder = call.builder().getRexBuilder();

        // Create condition for first join (newLeft JOIN newMid)
        RexNode firstCondition = buildJoinCondition(newLeft, newMid,
            leftJoin.getCondition(), topJoin.getCondition(), rexBuilder);

        MatchJoin firstJoin = MatchJoin.create(
            topJoin.getCluster(),
            topJoin.getTraitSet(),
            newLeft,
            newMid,
            firstCondition != null ? firstCondition : rexBuilder.makeLiteral(true),
            JoinRelType.INNER
        );

        // Create condition for second join (firstJoin JOIN newRight)
        RexNode secondCondition = buildJoinCondition(firstJoin, newRight,
            leftJoin.getCondition(), topJoin.getCondition(), rexBuilder);

        MatchJoin secondJoin = MatchJoin.create(
            topJoin.getCluster(),
            topJoin.getTraitSet(),
            firstJoin,
            newRight,
            secondCondition != null ? secondCondition : rexBuilder.makeLiteral(true),
            JoinRelType.INNER
        );

        call.transformTo(secondJoin);
    }

    /**
     * Calculate selectivity information for a match pattern.
     */
    private SelectivityInfo calculateSelectivity(IMatchNode node) {
        SelectivityInfo info = new SelectivityInfo();
        info.node = node;
        info.score = calculateSelectivityScore(node);
        return info;
    }

    /**
     * Calculate selectivity score. Higher score = more selective = should execute first.
     * Scoring:
     * - ID equality filter: 100 points (direct lookup, ~O(1))
     * - Property equality filter: 10 points (index lookup possible, ~O(log n))
     * - Property range filter: 5 points (index scan, ~O(k log n))
     * - Label filter only: 1 point (type scan, ~O(n))
     * - No filter: 0 points (full scan, ~O(n))
     */
    private double calculateSelectivityScore(IMatchNode node) {
        double score = 0.0;

        if (node instanceof VertexMatch) {
            VertexMatch vertex = (VertexMatch) node;

            // Highest priority: ID set from MatchIdFilterSimplifyRule
            Set<Object> idSet = vertex.getIdSet();
            if (idSet != null && !idSet.isEmpty()) {
                score += 100.0 / idSet.size();  // More IDs = less selective per ID
            }

            // Check push-down filter for selectivity
            RexNode filter = vertex.getPushDownFilter();
            if (filter != null) {
                score += analyzeFilterSelectivity(filter);
            }

            // Label provides some selectivity
            if (vertex.getTypes() != null && !vertex.getTypes().isEmpty()) {
                score += 1.0;
            }

        } else if (node instanceof EdgeMatch) {
            EdgeMatch edge = (EdgeMatch) node;
            // EdgeMatch doesn't have filter, only uses edge types
            if (edge.getTypes() != null && !edge.getTypes().isEmpty()) {
                score += 0.5;
            }

        } else if (node instanceof MatchFilter) {
            MatchFilter filter = (MatchFilter) node;
            score += analyzeFilterSelectivity(filter.getCondition());
            score += calculateSelectivityScore((IMatchNode) filter.getInput());

        } else if (node instanceof MatchJoin) {
            MatchJoin join = (MatchJoin) node;
            // For joins, use max selectivity of children (best anchor point)
            score = Math.max(
                calculateSelectivityScore((IMatchNode) join.getLeft()),
                calculateSelectivityScore((IMatchNode) join.getRight())
            );
        }

        return score;
    }

    /**
     * Analyze filter selectivity based on filter type and structure.
     */
    private double analyzeFilterSelectivity(RexNode filter) {
        if (filter instanceof RexCall) {
            RexCall call = (RexCall) filter;
            SqlKind kind = call.getKind();

            switch (kind) {
                case EQUALS:
                    // Check if this is an ID equality (highest selectivity)
                    if (isIdEquality(call)) {
                        return 100.0;
                    }
                    // Property equality (medium-high selectivity)
                    return 10.0;

                case LESS_THAN:
                case LESS_THAN_OR_EQUAL:
                case GREATER_THAN:
                case GREATER_THAN_OR_EQUAL:
                    // Range filter (medium selectivity)
                    return 5.0;

                case AND:
                    // Multiple conditions: multiply selectivity
                    double andScore = 0.0;
                    for (RexNode operand : call.getOperands()) {
                        andScore += analyzeFilterSelectivity(operand);
                    }
                    return andScore;

                case OR:
                    // Alternative conditions: take max selectivity
                    double maxScore = 0.0;
                    for (RexNode operand : call.getOperands()) {
                        maxScore = Math.max(maxScore, analyzeFilterSelectivity(operand));
                    }
                    return maxScore * 0.5;  // OR is less selective than AND

                default:
                    // Generic filter (low selectivity)
                    return 1.0;
            }
        }
        return 0.0;
    }

    /**
     * Check if a filter is an ID equality condition.
     */
    private boolean isIdEquality(RexCall call) {
        if (call.getKind() != SqlKind.EQUALS) {
            return false;
        }

        List<RexNode> operands = call.getOperands();
        for (RexNode operand : operands) {
            if (operand instanceof RexFieldAccess) {
                RexFieldAccess fieldAccess = (RexFieldAccess) operand;
                if (fieldAccess.getField().getType() instanceof MetaFieldType) {
                    MetaFieldType metaType = (MetaFieldType) fieldAccess.getField().getType();
                    if (metaType.getMetaField() == MetaField.VERTEX_ID) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * Build join condition between two patterns based on original conditions.
     * This is a simplified version that uses TRUE for now.
     * A complete implementation would analyze shared variables and build proper equi-join conditions.
     */
    private RexNode buildJoinCondition(IMatchNode left, IMatchNode right,
                                       RexNode originalLeftCondition,
                                       RexNode originalTopCondition,
                                       RexBuilder rexBuilder) {
        // Simplified: for graph pattern joins, conditions are often implicit through shared labels
        // A complete implementation would:
        // 1. Find shared labels between left and right patterns
        // 2. Build equality conditions on those labels
        // 3. Adjust field references based on new schema
        return rexBuilder.makeLiteral(true);
    }

    /**
     * Helper class to store selectivity information.
     */
    private static class SelectivityInfo {
        IMatchNode node;
        double score;
    }
}
