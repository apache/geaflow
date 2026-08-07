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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.dsl.calcite.MetaFieldType;
import org.apache.geaflow.dsl.calcite.MetaFieldType.MetaField;
import org.apache.geaflow.dsl.common.util.TypeCastUtil;
import org.apache.geaflow.dsl.rel.match.MatchFilter;
import org.apache.geaflow.dsl.rel.match.VertexMatch;
import org.apache.geaflow.dsl.rex.PathInputRef;
import org.apache.geaflow.dsl.util.GQLRexUtil;
import org.apache.geaflow.dsl.util.SqlTypeUtil;

public class MatchIdFilterSimplifyRule extends RelOptRule {

    public static final MatchIdFilterSimplifyRule INSTANCE = new MatchIdFilterSimplifyRule();

    private MatchIdFilterSimplifyRule() {
        super(operand(MatchFilter.class,
            operand(VertexMatch.class, any())));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        MatchFilter matchFilter = call.rel(0);
        VertexMatch vertexMatch = call.rel(1);

        if (!(matchFilter.getCondition() instanceof RexCall)) {
            return;
        }
        RexCall condition = (RexCall) matchFilter.getCondition();
        Set<Object> idSet = new HashSet<>();

        // First, try the original logic for pure ID filters (EQUALS or OR of EQUALS)
        boolean onlyHasIdFilter = findIdFilter(idSet, condition, vertexMatch);

        if (onlyHasIdFilter) {
            // Pure ID filter case: remove the MatchFilter entirely
            VertexMatch newVertexMatch = vertexMatch.copy(idSet);
            call.transformTo(newVertexMatch);
            return;
        }

        // Second, try to extract ID filters from AND conditions
        // This handles mixed conditions like: a.id = 1 AND a.name = 'John'
        if (condition.getKind() == SqlKind.AND) {
            idSet.clear();
            List<RexNode> remainingConditions = new ArrayList<>();
            extractIdFiltersFromAnd(idSet, remainingConditions, condition, vertexMatch);

            if (!idSet.isEmpty()) {
                VertexMatch newVertexMatch = vertexMatch.copy(idSet);

                if (remainingConditions.isEmpty()) {
                    call.transformTo(newVertexMatch);
                } else {
                    // Create new filter with remaining conditions
                    RexBuilder rexBuilder = call.builder().getRexBuilder();
                    RexNode remainingFilter = GQLRexUtil.and(remainingConditions, rexBuilder);
                    MatchFilter newMatchFilter = MatchFilter.create(
                        newVertexMatch,
                        remainingFilter,
                        matchFilter.getPathSchema()
                    );
                    call.transformTo(newMatchFilter);
                }
            }
        }
    }

    private boolean findIdFilter(Set<Object> idSet, RexCall condition, VertexMatch vertexMatch) {
        SqlKind kind = condition.getKind();
        if (kind == SqlKind.EQUALS) {
            List<RexNode> operands = condition.getOperands();
            RexFieldAccess fieldAccess = null;
            RexLiteral idLiteral = null;
            if (operands.get(0) instanceof RexFieldAccess && operands.get(1) instanceof RexLiteral) {
                fieldAccess = (RexFieldAccess) operands.get(0);
                idLiteral = (RexLiteral) operands.get(1);
            } else if (operands.get(1) instanceof RexFieldAccess && operands.get(0) instanceof RexLiteral) {
                fieldAccess = (RexFieldAccess) operands.get(1);
                idLiteral = (RexLiteral) operands.get(0);
            } else {
                return false;
            }
            RexNode referenceExpr = fieldAccess.getReferenceExpr();
            RelDataTypeField field = fieldAccess.getField();
            // Check if the field access references the current vertex
            boolean isRefInputVertex = false;
            if (referenceExpr instanceof PathInputRef) {
                // PathInputRef contains explicit label - check it matches
                isRefInputVertex = ((PathInputRef) referenceExpr).getLabel().equals(vertexMatch.getLabel());
            } else if (referenceExpr instanceof RexInputRef) {
                // RexInputRef requires index validation to ensure it references current node
                // Use FilterMatchNodeTransposeRule pattern: index == fieldCount - 1
                RexInputRef inputRef = (RexInputRef) referenceExpr;
                int currentNodeIndex = vertexMatch.getPathSchema().getFieldCount() - 1;
                isRefInputVertex = (inputRef.getIndex() == currentNodeIndex);
            }
            if (isRefInputVertex
                && field.getType() instanceof MetaFieldType
                && ((MetaFieldType) field.getType()).getMetaField() == MetaField.VERTEX_ID) {
                RelDataType dataType = ((MetaFieldType) field.getType()).getType();
                IType<?> idType = SqlTypeUtil.convertType(dataType);
                idSet.add(TypeCastUtil.cast(idLiteral.getValue(), idType));
                return true;
            }
            return false;
        } else if (kind == SqlKind.OR) {
            boolean onlyHasIdFilter = true;
            List<RexNode> operands = condition.getOperands();
            for (RexNode operand : operands) {
                if (operand instanceof RexCall) {
                    onlyHasIdFilter = onlyHasIdFilter && findIdFilter(idSet, (RexCall) operand,
                        vertexMatch);
                } else {
                    // Has other filter
                    return false;
                }
            }
            return onlyHasIdFilter;
        }
        return false;
    }

    /**
     * Extracts ID filters from AND conditions.
     * For example: a.id = 1 AND a.name = 'John' -> idSet={1}, remaining=[a.name = 'John']
     *
     * <p>Important: If multiple ID equality conditions are found in an AND (e.g., a.id=1 AND a.id=2),
     * this is either contradictory (different values) or redundant (same value). We only extract
     * the first ID filter found and keep subsequent ones as remaining conditions to preserve
     * correct semantics. The runtime will handle the contradiction if values differ.
     *
     * @param idSet output set to collect extracted ID values (at most one ID filter extracted)
     * @param remaining output list to collect non-ID conditions
     * @param condition the AND condition to process
     * @param vertexMatch the target vertex match node
     */
    private void extractIdFiltersFromAnd(Set<Object> idSet, List<RexNode> remaining,
                                         RexCall condition, VertexMatch vertexMatch) {
        // Track if we've already extracted an ID filter - only extract one to avoid semantic issues
        // with contradictory conditions like "a.id=1 AND a.id=2"
        boolean idFilterExtracted = !idSet.isEmpty();

        for (RexNode operand : condition.getOperands()) {
            if (operand instanceof RexCall) {
                RexCall opCall = (RexCall) operand;
                SqlKind opKind = opCall.getKind();

                if (opKind == SqlKind.AND) {
                    // Recursively handle nested AND
                    extractIdFiltersFromAnd(idSet, remaining, opCall, vertexMatch);
                    // Update flag after recursive call
                    idFilterExtracted = !idSet.isEmpty();
                } else if (opKind == SqlKind.EQUALS || opKind == SqlKind.OR) {
                    // Try to extract ID filter(s) from this operand
                    // Only extract if we haven't already extracted one
                    if (!idFilterExtracted) {
                        Set<Object> tempIdSet = new HashSet<>();
                        if (findIdFilter(tempIdSet, opCall, vertexMatch)) {
                            idSet.addAll(tempIdSet);
                            idFilterExtracted = true;
                        } else {
                            remaining.add(operand);
                        }
                    } else {
                        // Already have an ID filter - keep this as remaining
                        // This handles cases like "a.id=1 AND a.id=2" correctly
                        remaining.add(operand);
                    }
                } else {
                    remaining.add(operand);
                }
            } else {
                remaining.add(operand);
            }
        }
    }
}
