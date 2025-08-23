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

package org.apache.geaflow.dsl.rel.match;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.BiRel;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.geaflow.dsl.calcite.JoinPathRecordType;
import org.apache.geaflow.dsl.calcite.PathRecordType;
import org.apache.geaflow.dsl.calcite.UnionPathRecordType;
import org.apache.geaflow.dsl.calcite.VertexRecordType.VirtualVertexRecordType;
import org.apache.geaflow.dsl.rel.MatchNodeVisitor;
import org.apache.geaflow.dsl.rex.PathInputRef;
import org.apache.geaflow.dsl.util.GQLRelUtil;
import org.apache.geaflow.dsl.util.GQLRexUtil;

public class MatchJoin extends BiRel implements IMatchNode {

    protected final RexNode condition;

    protected final JoinRelType joinType;

    protected MatchJoin(RelOptCluster cluster, RelTraitSet traitSet, RelNode left,
                        RelNode right, RexNode condition, JoinRelType joinType) {
        super(cluster, traitSet, left, right);
        this.condition = Objects.requireNonNull(condition);
        this.joinType = Objects.requireNonNull(joinType);
    }

    public MatchJoin copy(RelTraitSet traitSet, RexNode conditionExpr, RelNode left, RelNode right,
                          JoinRelType joinType) {
        return new MatchJoin(getCluster(), traitSet, left, right, conditionExpr, joinType);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert inputs.size() == 2;
        return new MatchJoin(getCluster(), traitSet, inputs.get(0), inputs.get(1), condition, joinType);
    }

    public static MatchJoin create(RelOptCluster cluster, RelTraitSet traitSet, IMatchNode left,
                                   IMatchNode right, RexNode condition, JoinRelType joinType) {
        return new MatchJoin(cluster, traitSet, left, right, condition, joinType);
    }

    @Override
    public PathRecordType getPathSchema() {
        return (PathRecordType) getRowType();
    }

    @Override
    public RelDataType getNodeType() {
        return VirtualVertexRecordType.of(getCluster().getTypeFactory());
    }

    @Override
    public IMatchNode copy(List<RelNode> inputs, PathRecordType pathType) {
        return new MatchJoin(getCluster(), getTraitSet(), inputs.get(0), inputs.get(1), condition, joinType);
    }

    @Override
    public <T> T accept(MatchNodeVisitor<T> visitor) {
        return visitor.visitJoin(this);
    }

    public RexNode getCondition() {
        return condition;
    }

    public JoinRelType getJoinType() {
        return joinType;
    }

    @Override
    protected RelDataType deriveRowType() {
        RelNode realLeft = GQLRelUtil.toRel(left);
        RelNode realRight = GQLRelUtil.toRel(right);
        RelDataType relRecordType = SqlValidatorUtil.deriveJoinRowType(((IMatchNode) realLeft).getPathSchema(),
            ((IMatchNode) realRight).getPathSchema(), joinType, getCluster().getTypeFactory(),
            null, new ArrayList<>());
        if (realLeft.getRowType() instanceof UnionPathRecordType
            || realRight.getRowType() instanceof UnionPathRecordType) {
            List<PathRecordType> leftUnionPaths =
                realLeft.getRowType() instanceof UnionPathRecordType
                    ? ((UnionPathRecordType) realLeft.getRowType()).getInputPathRecordTypes()
                    : Collections.singletonList(((IMatchNode) realLeft).getPathSchema());
            List<PathRecordType> rightUnionPaths =
                realRight.getRowType() instanceof UnionPathRecordType
                    ? ((UnionPathRecordType) realRight.getRowType()).getInputPathRecordTypes()
                    : Collections.singletonList(((IMatchNode) realRight).getPathSchema());
            List<PathRecordType> unionPaths = new ArrayList<>();
            for (PathRecordType leftPath : leftUnionPaths) {
                for (PathRecordType rightPath : rightUnionPaths) {
                    unionPaths.add(new PathRecordType(SqlValidatorUtil.deriveJoinRowType(leftPath,
                        rightPath, joinType, getCluster().getTypeFactory(),
                        null, new ArrayList<>()).getFieldList()));
                }
            }
            return new UnionPathRecordType(relRecordType.getFieldList(), unionPaths);
        } else {
            return new JoinPathRecordType(relRecordType.getFieldList());
        }
    }

    @Override
    public void replaceInput(int ordinalInParent, RelNode p) {
        super.replaceInput(ordinalInParent, p);
    }

    public JoinInfo analyzeCondition() {
        RexBuilder rexBuilder = getCluster().getRexBuilder();
        List<Integer> leftPathKeys = new ArrayList<>();
        List<Integer> rightPathKeys = new ArrayList<>();
        List<RexNode> nonEquiList = new ArrayList<>();
        this.splitJoinCondition(condition, leftPathKeys, rightPathKeys, nonEquiList);
        RexNode newRemaining = RexUtil.composeConjunction(rexBuilder, nonEquiList);
        return new PathJoinInfo(ImmutableIntList.copyOf(leftPathKeys),
            ImmutableIntList.copyOf(rightPathKeys), newRemaining);
    }

    @Override
    public RelNode accept(RexShuttle shuttle) {
        RexNode condition = shuttle.apply(this.condition);
        if (this.condition == condition) {
            return this;
        }
        return copy(traitSet, condition, left, right, joinType);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
            .item("condition", condition)
            .item("joinType", joinType.lowerName);
    }

    private static class PathJoinInfo extends JoinInfo {
        public final RexNode remaining;

        protected PathJoinInfo(ImmutableIntList leftKeys, ImmutableIntList rightKeys, RexNode remaining) {
            super(leftKeys, rightKeys);
            this.remaining = Objects.requireNonNull(remaining);
        }

        @Override
        public boolean isEqui() {
            return remaining.isAlwaysTrue();
        }

        @Override
        public RexNode getRemaining(RexBuilder rexBuilder) {
            return isEqui() ? rexBuilder.makeLiteral(true) : remaining;
        }
    }

    private void splitJoinCondition(RexNode condition, List<Integer> leftKeys,
                                    List<Integer> rightKeys, List<RexNode> nonEquiList) {
        if (condition instanceof RexCall) {
            RexCall call = (RexCall) condition;
            SqlKind kind = call.getKind();
            if (kind == SqlKind.AND) {
                for (RexNode operand : call.getOperands()) {
                    splitJoinCondition(operand, leftKeys, rightKeys, nonEquiList);
                }
                return;
            }
            int leftFieldCount = ((IMatchNode) left).getPathSchema().getFieldCount();
            if (kind == SqlKind.EQUALS) {
                final List<RexNode> operands = call.getOperands();
                if (GQLRexUtil.isVertexIdFieldAccess(operands.get(0))
                    && GQLRexUtil.isVertexIdFieldAccess(operands.get(1))) {
                    RexFieldAccess op0 = (RexFieldAccess) operands.get(0);
                    RexFieldAccess op1 = (RexFieldAccess) operands.get(1);
                    String op0PathLabel = ((PathInputRef) op0.getReferenceExpr()).getLabel();
                    String op1PathLabel = ((PathInputRef) op1.getReferenceExpr()).getLabel();
                    int op0Index = this.getPathSchema().getField(op0PathLabel, true, false).getIndex();
                    int op1Index = this.getPathSchema().getField(op1PathLabel, true, false).getIndex();
                    RelDataTypeField leftField;
                    RelDataTypeField rightField;
                    if (op0Index < leftFieldCount && op1Index >= leftFieldCount) {
                        leftField = ((IMatchNode) left).getPathSchema().getFieldList().get(op0Index);
                        rightField = ((IMatchNode) right).getPathSchema().getFieldList().get(op1Index - leftFieldCount);
                    } else if (op1Index < leftFieldCount && op0Index >= leftFieldCount) {
                        leftField = ((IMatchNode) left).getPathSchema().getFieldList().get(op1Index);
                        rightField = ((IMatchNode) right).getPathSchema().getFieldList().get(op0Index - leftFieldCount);
                    } else {
                        nonEquiList.add(condition);
                        return;
                    }
                    leftKeys.add(leftField.getIndex());
                    rightKeys.add(rightField.getIndex());
                    return;
                }
            }
        }
        if (!condition.isAlwaysTrue()) {
            nonEquiList.add(condition);
        }
    }
}
