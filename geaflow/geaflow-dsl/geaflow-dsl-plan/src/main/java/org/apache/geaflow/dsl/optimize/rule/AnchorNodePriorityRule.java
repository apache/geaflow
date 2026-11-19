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

import java.util.List;
import java.util.Set;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlKind;
import org.apache.geaflow.dsl.calcite.MetaFieldType;
import org.apache.geaflow.dsl.calcite.MetaFieldType.MetaField;
import org.apache.geaflow.dsl.rel.match.EdgeMatch;
import org.apache.geaflow.dsl.rel.match.IMatchNode;
import org.apache.geaflow.dsl.rel.match.MatchFilter;
import org.apache.geaflow.dsl.rel.match.MatchJoin;
import org.apache.geaflow.dsl.rel.match.SingleMatchNode;
import org.apache.geaflow.dsl.rel.match.VertexMatch;
import org.apache.geaflow.dsl.rex.PathInputRef;
import org.apache.geaflow.dsl.util.GQLRelUtil;

/**
 * Rule for Issue #363: Identifies anchor nodes (vertices with ID equality filters)
 * and reorders join operations to prioritize these high-selectivity nodes.
 * This rule transforms queries like:
 *   MATCH (a:Person where a.id = 4)-[e]-&gt;(b), (c:Person)-[knows]-&gt;(d where d.id = 2)
 * Into an execution plan that processes anchor nodes (a, d) first, then expands edges.
 * Benefits:
 * - Reduces intermediate result set size by starting with high-selectivity filters
 * - Enables direct index lookup for ID-based vertex access
 * - Improves join order by identifying selective predicates early
 */
public class AnchorNodePriorityRule extends RelOptRule {

    public static final AnchorNodePriorityRule INSTANCE = new AnchorNodePriorityRule();

    private AnchorNodePriorityRule() {
        super(operand(MatchJoin.class, any()));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        MatchJoin join = call.rel(0);

        // Only optimize INNER joins
        if (join.getJoinType() != JoinRelType.INNER) {
            return;
        }

        // Use GQLRelUtil.toRel() to unwrap HepRelVertex wrappers from Calcite optimizer
        IMatchNode left = (IMatchNode) GQLRelUtil.toRel(join.getLeft());
        IMatchNode right = (IMatchNode) GQLRelUtil.toRel(join.getRight());

        // Calculate anchor scores for left and right patterns
        double leftScore = calculateAnchorScore(left);
        double rightScore = calculateAnchorScore(right);

        // If right side has higher anchor score, swap to process it first
        if (rightScore > leftScore && rightScore > 0) {
            // Swap join operands to prioritize anchor node
            RexNode swappedCondition = swapJoinCondition(join.getCondition(),
                left.getPathSchema().getFieldCount(),
                right.getPathSchema().getFieldCount(),
                call.builder().getRexBuilder());

            MatchJoin newJoin = MatchJoin.create(
                join.getCluster(),
                join.getTraitSet(),
                right,  // Swap: right becomes left
                left,   // Swap: left becomes right
                swappedCondition,
                join.getJoinType()
            );

            // Calcite requires the transformed node to keep an identical output row type.
            // Swapping join operands changes field order, so skip the rewrite unless the
            // result schema matches the original join schema.
            if (!newJoin.getRowType().equals(join.getRowType())) {
                return;
            }

            call.transformTo(newJoin);
        }
    }

    /**
     * Calculate anchor score for a match pattern.
     * Higher score indicates better selectivity (should be processed first).
     * Scoring factors:
     * - ID equality filter: +10 points (direct index lookup)
     * - Other filters: +1 point (reduces result set)
     * - No filters: 0 points
     */
    private double calculateAnchorScore(IMatchNode node) {
        if (node instanceof SingleMatchNode) {
            return calculateSingleNodeScore((SingleMatchNode) node);
        } else if (node instanceof MatchJoin) {
            MatchJoin join = (MatchJoin) node;
            // For joins, return max score of children (best anchor in subtree)
            // Use GQLRelUtil.toRel() to unwrap HepRelVertex wrappers
            return Math.max(
                calculateAnchorScore((IMatchNode) GQLRelUtil.toRel(join.getLeft())),
                calculateAnchorScore((IMatchNode) GQLRelUtil.toRel(join.getRight()))
            );
        } else if (node instanceof MatchFilter) {
            MatchFilter filter = (MatchFilter) node;
            // Use GQLRelUtil.toRel() to unwrap HepRelVertex wrappers
            double baseScore = calculateAnchorScore((IMatchNode) GQLRelUtil.toRel(filter.getInput()));
            // Add bonus for filter presence
            return baseScore + 1.0;
        }
        return 0.0;
    }

    /**
     * Calculate score for a single match node (VertexMatch or EdgeMatch).
     */
    private double calculateSingleNodeScore(SingleMatchNode node) {
        double score = 0.0;

        if (node instanceof VertexMatch) {
            VertexMatch vertex = (VertexMatch) node;

            // High priority: Has ID set (from MatchIdFilterSimplifyRule)
            Set<Object> idSet = vertex.getIdSet();
            if (idSet != null && !idSet.isEmpty()) {
                score += 10.0 * idSet.size();
            }

            // Medium priority: Has push-down filter with ID equality
            RexNode filter = vertex.getPushDownFilter();
            if (filter != null) {
                if (hasIdEqualityFilter(filter, vertex.getLabel())) {
                    score += 10.0;
                } else {
                    score += 1.0;  // Other filters also help
                }
            }
        } else if (node instanceof EdgeMatch) {
            EdgeMatch edge = (EdgeMatch) node;
            // EdgeMatch doesn't have filter
            if (edge.getTypes() != null && !edge.getTypes().isEmpty()) {
                score += 0.5;  // Edge type filters help somewhat
            }
        }

        // Recursively check input
        if (node.getInput() != null) {
            // Use GQLRelUtil.toRel() to unwrap HepRelVertex wrappers
            score += calculateAnchorScore((IMatchNode) GQLRelUtil.toRel(node.getInput()));
        }

        return score;
    }

    /**
     * Check if filter contains ID equality condition.
     */
    private boolean hasIdEqualityFilter(RexNode condition, String targetLabel) {
        if (condition instanceof RexCall) {
            RexCall call = (RexCall) condition;

            if (call.getKind() == SqlKind.EQUALS) {
                // Check if this is id = literal
                List<RexNode> operands = call.getOperands();
                for (int i = 0; i < operands.size(); i++) {
                    RexNode operand = operands.get(i);
                    RexNode other = operands.get(1 - i);

                    if (operand instanceof RexFieldAccess && other instanceof RexLiteral) {
                        RexFieldAccess fieldAccess = (RexFieldAccess) operand;
                        if (isIdField(fieldAccess, targetLabel)) {
                            return true;
                        }
                    }
                }
            } else if (call.getKind() == SqlKind.AND) {
                // Check all conjunctions
                for (RexNode operand : call.getOperands()) {
                    if (hasIdEqualityFilter(operand, targetLabel)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * Check if a field access refers to an ID field.
     */
    private boolean isIdField(RexFieldAccess fieldAccess, String targetLabel) {
        RexNode referenceExpr = fieldAccess.getReferenceExpr();

        // Check if references target label
        boolean isTargetLabel = false;
        if (referenceExpr instanceof PathInputRef) {
            isTargetLabel = ((PathInputRef) referenceExpr).getLabel().equals(targetLabel);
        } else if (referenceExpr instanceof RexInputRef) {
            isTargetLabel = true;  // Direct reference
        }

        // Check if field is ID
        if (isTargetLabel && fieldAccess.getField().getType() instanceof MetaFieldType) {
            MetaFieldType metaType = (MetaFieldType) fieldAccess.getField().getType();
            return metaType.getMetaField() == MetaField.VERTEX_ID;
        }

        return false;
    }

    /**
     * Swap join condition when operands are swapped.
     * Updates field references to reflect new input positions.
     * Preserves PathInputRef labels which are critical for graph pattern matching.
     */
    private RexNode swapJoinCondition(RexNode condition, int leftFieldCount,
                                      int rightFieldCount, RexBuilder builder) {
        return condition.accept(new RexShuttle() {
            @Override
            public RexNode visitInputRef(RexInputRef inputRef) {
                int index = inputRef.getIndex();
                int newIndex;

                if (index < leftFieldCount) {
                    // Was referencing left, now references right (shift by rightFieldCount)
                    newIndex = index + rightFieldCount;
                } else {
                    // Was referencing right, now references left (shift back)
                    newIndex = index - leftFieldCount;
                }

                // Preserve PathInputRef with label information - critical for graph patterns
                if (inputRef instanceof PathInputRef) {
                    return ((PathInputRef) inputRef).copy(newIndex);
                }
                return new RexInputRef(newIndex, inputRef.getType());
            }
        });
    }
}
