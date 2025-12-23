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

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.geaflow.dsl.calcite.MetaFieldType;
import org.apache.geaflow.dsl.calcite.MetaFieldType.MetaField;
import org.apache.geaflow.dsl.rel.match.IMatchNode;
import org.apache.geaflow.dsl.rel.match.MatchFilter;
import org.apache.geaflow.dsl.rel.match.VertexMatch;
import org.apache.geaflow.dsl.rex.PathInputRef;
import org.apache.geaflow.dsl.util.GQLRexUtil;

/**
 * Rule for Issue #363: Aggressively pushes ID equality filters to VertexMatch nodes.
 * This rule specifically targets ID filters (where vertex.id = literal) and ensures they are
 * pushed down as close to the VertexMatch as possible, enabling direct index lookups.
 * Example transformation:
 * Before:
 *   MatchFilter(condition: a.id = 4 AND a.name = "John")
 *     VertexMatch(a:Person)
 * After:
 *   MatchFilter(condition: a.name = "John")
 *     VertexMatch(a:Person, pushDownFilter: a.id = 4)
 * This prioritizes ID filters, which have the highest selectivity and can be resolved
 * through direct index lookups rather than full vertex scans.
 */
public class IdFilterPushdownRule extends RelOptRule {

    public static final IdFilterPushdownRule INSTANCE = new IdFilterPushdownRule();

    private IdFilterPushdownRule() {
        super(operand(MatchFilter.class,
            operand(VertexMatch.class, any())));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        MatchFilter filter = call.rel(0);
        VertexMatch vertexMatch = call.rel(1);

        // If vertex already has ID set, this has been optimized
        if (vertexMatch.getIdSet() != null && !vertexMatch.getIdSet().isEmpty()) {
            return;
        }

        List<RexNode> conditions = RelOptUtil.conjunctions(filter.getCondition());

        // Separate ID filters from other filters
        List<RexNode> idFilters = new ArrayList<>();
        List<RexNode> otherFilters = new ArrayList<>();

        for (RexNode condition : conditions) {
            if (isIdFilter(condition, vertexMatch.getLabel(), vertexMatch)) {
                idFilters.add(condition);
            } else {
                otherFilters.add(condition);
            }
        }

        // If no ID filters found, nothing to push
        if (idFilters.isEmpty()) {
            return;
        }

        RexBuilder builder = call.builder().getRexBuilder();

        // Combine existing push-down filter with new ID filters
        List<RexNode> pushDownFilters = new ArrayList<>(idFilters);
        if (vertexMatch.getPushDownFilter() != null) {
            pushDownFilters.add(vertexMatch.getPushDownFilter());
        }
        RexNode combinedPushDown = GQLRexUtil.and(pushDownFilters, builder);

        // Create new VertexMatch with push-down filter
        VertexMatch newVertexMatch = new VertexMatch(
            vertexMatch.getCluster(),
            vertexMatch.getTraitSet(),
            vertexMatch.getInput(),
            vertexMatch.getLabel(),
            vertexMatch.getTypes(),
            vertexMatch.getNodeType(),
            vertexMatch.getPathSchema(),
            combinedPushDown,
            vertexMatch.getIdSet(),
            vertexMatch.getFields()
        );

        // If there are remaining filters, keep them
        if (!otherFilters.isEmpty()) {
            RexNode remainingCondition = GQLRexUtil.and(otherFilters, builder);
            MatchFilter newFilter = MatchFilter.create(
                newVertexMatch,
                remainingCondition,
                filter.getPathSchema()
            );
            call.transformTo(newFilter);
        } else {
            // All filters pushed down
            call.transformTo(newVertexMatch);
        }
    }

    /**
     * Check if a condition is an ID equality filter for the target label.
     */
    private boolean isIdFilter(RexNode condition, String targetLabel, IMatchNode matchNode) {
        if (!(condition instanceof RexCall)) {
            return false;
        }

        RexCall call = (RexCall) condition;
        if (call.getKind() != SqlKind.EQUALS) {
            return false;
        }

        List<RexNode> operands = call.getOperands();
        if (operands.size() != 2) {
            return false;
        }

        // Check both operand orders: id = literal or literal = id
        for (int i = 0; i < 2; i++) {
            RexNode first = operands.get(i);
            RexNode second = operands.get(1 - i);

            if (first instanceof RexFieldAccess && second instanceof RexLiteral) {
                RexFieldAccess fieldAccess = (RexFieldAccess) first;
                if (isIdFieldAccess(fieldAccess, targetLabel, matchNode)) {
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * Check if a field access references an ID field for the target label.
     * Uses the FilterMatchNodeTransposeRule pattern: index == fieldCount - 1 to detect current node.
     */
    private boolean isIdFieldAccess(RexFieldAccess fieldAccess, String targetLabel, IMatchNode matchNode) {
        RexNode referenceExpr = fieldAccess.getReferenceExpr();
        RelDataTypeField field = fieldAccess.getField();

        // Check if references the target label
        boolean referencesTarget = false;
        if (referenceExpr instanceof PathInputRef) {
            PathInputRef pathRef = (PathInputRef) referenceExpr;
            referencesTarget = pathRef.getLabel().equals(targetLabel);
        } else if (referenceExpr instanceof RexInputRef) {
            // RexInputRef (not PathInputRef) must reference current node
            // Use FilterMatchNodeTransposeRule pattern: index == fieldCount - 1
            RexInputRef inputRef = (RexInputRef) referenceExpr;
            int currentNodeIndex = matchNode.getPathSchema().getFieldCount() - 1;
            referencesTarget = (inputRef.getIndex() == currentNodeIndex);
        }

        // Check if field is VERTEX_ID
        if (referencesTarget && field.getType() instanceof MetaFieldType) {
            MetaFieldType metaType = (MetaFieldType) field.getType();
            return metaType.getMetaField() == MetaField.VERTEX_ID;
        }

        return false;
    }
}
