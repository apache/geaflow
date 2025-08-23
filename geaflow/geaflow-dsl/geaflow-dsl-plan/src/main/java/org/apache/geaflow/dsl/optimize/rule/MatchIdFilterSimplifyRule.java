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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.dsl.calcite.MetaFieldType;
import org.apache.geaflow.dsl.calcite.MetaFieldType.MetaField;
import org.apache.geaflow.dsl.common.util.TypeCastUtil;
import org.apache.geaflow.dsl.rel.match.MatchFilter;
import org.apache.geaflow.dsl.rel.match.VertexMatch;
import org.apache.geaflow.dsl.rex.PathInputRef;
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
        boolean onLyHasIdFilter = findIdFilter(idSet, condition, vertexMatch);

        if (!onLyHasIdFilter) {
            return;
        }

        VertexMatch newVertexMatch = vertexMatch.copy(idSet);
        call.transformTo(newVertexMatch);
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
            boolean isRefInputVertex = (referenceExpr instanceof PathInputRef
                && ((PathInputRef) referenceExpr).getLabel().equals(vertexMatch.getLabel()))
                || referenceExpr instanceof RexInputRef;
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
}
