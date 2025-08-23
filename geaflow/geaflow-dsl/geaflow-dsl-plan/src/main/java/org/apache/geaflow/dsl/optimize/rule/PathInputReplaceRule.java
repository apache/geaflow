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

import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.*;
import org.apache.geaflow.dsl.calcite.EdgeRecordType;
import org.apache.geaflow.dsl.calcite.PathRecordType;
import org.apache.geaflow.dsl.calcite.VertexRecordType;
import org.apache.geaflow.dsl.rel.match.IMatchNode;
import org.apache.geaflow.dsl.rel.match.MatchJoin;
import org.apache.geaflow.dsl.rex.PathInputRef;
import org.apache.geaflow.dsl.rex.RexLambdaCall;
import org.apache.geaflow.dsl.util.GQLRelUtil;

public class PathInputReplaceRule extends RelOptRule {

    public static final PathInputReplaceRule INSTANCE = new PathInputReplaceRule();

    private PathInputReplaceRule() {
        super(operand(RelNode.class, any()));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        RelNode node = call.rel(0);

        if (node instanceof IMatchNode) {
            PathRecordType pathRecordType = null;
            if (node instanceof MatchJoin) {
                pathRecordType = ((IMatchNode) node).getPathSchema();
            } else if (node.getInputs().size() == 1) {
                pathRecordType = ((IMatchNode) GQLRelUtil.toRel(node.getInput(0))).getPathSchema();
            }
            if (pathRecordType != null) {
                RelNode newNode = replaceInputRef(pathRecordType, node);
                call.transformTo(newNode);
            }
        } else {
            if (node.getInputs().size() == 1
                && node.getInput(0).getRowType() instanceof PathRecordType) {
                PathRecordType pathRecordType = (PathRecordType) node.getInput(0).getRowType();
                RelNode newNode = replaceInputRef(pathRecordType, node);
                call.transformTo(newNode);
            }
        }
    }

    private RelNode replaceInputRef(PathRecordType pathRecordType, RelNode node) {
        return node.accept(new PathRefReplaceVisitor(pathRecordType));
    }

    private static class PathRefReplaceVisitor extends RexShuttle {

        private final PathRecordType pathRecordType;

        public PathRefReplaceVisitor(PathRecordType pathRecordType) {
            this.pathRecordType = pathRecordType;
        }

        @Override
        public RexNode visitInputRef(RexInputRef inputRef) {
            RelDataTypeField pathField = pathRecordType.getFieldList().get(inputRef.getIndex());
            return new PathInputRef(pathField.getName(), pathField.getIndex(), pathField.getType());
        }

        @Override
        public RexNode visitCall(RexCall call) {
            if (call instanceof RexLambdaCall) {
                RexLambdaCall lambdaCall = (RexLambdaCall) call;
                PathRecordType pathRecordType = (PathRecordType) lambdaCall.getInput().getType();
                RexNode newValue = ((RexLambdaCall) call).getValue()
                    .accept(new PathRefReplaceVisitor(pathRecordType));
                return lambdaCall.clone(lambdaCall.type, Lists.newArrayList(lambdaCall.getInput(), newValue));
            } else {
                return super.visitCall(call);
            }
        }

        @Override
        public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
            if (fieldAccess.getReferenceExpr() instanceof RexCorrelVariable
                && (fieldAccess.getType() instanceof VertexRecordType
                || fieldAccess.getType() instanceof EdgeRecordType)) {
                String pathFieldName = fieldAccess.getField().getName();
                return new PathInputRef(fieldAccess.getField().getName(),
                    pathRecordType.getField(pathFieldName, true, false).getIndex(),
                    fieldAccess.getField().getType());
            }
            return super.visitFieldAccess(fieldAccess);
        }
    }
}
