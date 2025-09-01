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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;
import org.apache.geaflow.dsl.calcite.MetaFieldType;
import org.apache.geaflow.dsl.calcite.MetaFieldType.MetaField;
import org.apache.geaflow.dsl.rel.match.EdgeMatch;
import org.apache.geaflow.dsl.rel.match.MatchFilter;
import org.apache.geaflow.dsl.rex.PathInputRef;

public class MatchEdgeLabelFilterRemoveRule extends RelOptRule {

    public static final MatchEdgeLabelFilterRemoveRule INSTANCE = new MatchEdgeLabelFilterRemoveRule();

    private MatchEdgeLabelFilterRemoveRule() {
        super(operand(MatchFilter.class,
            operand(EdgeMatch.class, any())));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        MatchFilter matchFilter = call.rel(0);
        EdgeMatch edgeMatch = call.rel(1);

        if (!(matchFilter.getCondition() instanceof RexCall)) {
            return;
        }
        Set<String> labelSetInFilter = new HashSet<>();
        RexCall condition = (RexCall) matchFilter.getCondition();
        boolean onlyHasLabelFilter = findLabelFilter(labelSetInFilter, condition, edgeMatch);
        if (!onlyHasLabelFilter) {
            return;
        }

        Set<String> edgeTypes = edgeMatch.getTypes();
        // If all labels in EdgeMatch are included in MatchFilter, we can remove it.
        for (String label : edgeTypes) {
            if (!labelSetInFilter.contains(label)) {
                return;
            }
        }

        call.transformTo(edgeMatch);
    }

    private boolean findLabelFilter(Set<String> labelSet, RexCall condition, EdgeMatch edgeMatch) {
        SqlKind kind = condition.getKind();
        if (kind == SqlKind.EQUALS) {
            List<RexNode> operands = condition.getOperands();
            RexFieldAccess fieldAccess = null;
            RexLiteral labelLiteral = null;
            if (operands.get(0) instanceof RexFieldAccess && operands.get(1) instanceof RexLiteral) {
                fieldAccess = (RexFieldAccess) operands.get(0);
                labelLiteral = (RexLiteral) operands.get(1);
            } else if (operands.get(1) instanceof RexFieldAccess && operands.get(0) instanceof RexLiteral) {
                fieldAccess = (RexFieldAccess) operands.get(1);
                labelLiteral = (RexLiteral) operands.get(0);
            } else {
                return false;
            }
            RexNode referenceExpr = fieldAccess.getReferenceExpr();
            RelDataTypeField field = fieldAccess.getField();
            boolean isRefInputEdge = (referenceExpr instanceof PathInputRef
                && ((PathInputRef) referenceExpr).getLabel().equals(edgeMatch.getLabel()))
                || referenceExpr instanceof RexInputRef;
            if (isRefInputEdge
                && field.getType() instanceof MetaFieldType
                && ((MetaFieldType) field.getType()).getMetaField() == MetaField.EDGE_TYPE) {
                labelSet.add(RexLiteral.stringValue(labelLiteral));
                return true;
            }
            return false;
        } else if (kind == SqlKind.OR) {
            boolean onlyHasIdFilter = true;
            List<RexNode> operands = condition.getOperands();
            for (RexNode operand : operands) {
                if (operand instanceof RexCall) {
                    onlyHasIdFilter = onlyHasIdFilter && findLabelFilter(labelSet, (RexCall) operand,
                        edgeMatch);
                } else {
                    // Has other filter
                    return false;
                }
            }
            return onlyHasIdFilter;
        }
        return false;
    }
}
