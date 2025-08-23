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

import static org.apache.geaflow.dsl.util.GQLRelUtil.match;

import java.util.List;
import java.util.Objects;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.geaflow.dsl.calcite.EdgeRecordType;
import org.apache.geaflow.dsl.calcite.PathRecordType;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.rel.MatchNodeVisitor;

public class VirtualEdgeMatch extends SingleRel implements SingleMatchNode, IMatchNode {

    private final RexNode targetIdExpression;

    private final PathRecordType pathType;

    private final RelDataType nodeType;

    public VirtualEdgeMatch(RelOptCluster cluster, RelTraitSet traitSet,
                            RelNode input, RexNode targetIdExpression,
                            RelDataType nodeType, PathRecordType pathType) {
        super(cluster, traitSet, input);
        this.targetIdExpression = Objects.requireNonNull(targetIdExpression);
        if (match(input).getNodeType().getSqlTypeName() != SqlTypeName.VERTEX) {
            throw new GeaFlowDSLException("Illegal input type: " + match(input).getNodeType().getSqlTypeName()
                + " for " + getRelTypeName() + ", should be: " + SqlTypeName.VERTEX);
        }
        this.rowType = Objects.requireNonNull(pathType);
        this.pathType = Objects.requireNonNull(pathType);
        this.nodeType = Objects.requireNonNull(nodeType);
    }

    public RexNode getTargetId() {
        return targetIdExpression;
    }

    @Override
    public VirtualEdgeMatch copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert inputs.size() == 1;
        return copy(traitSet, sole(inputs), targetIdExpression);
    }

    public VirtualEdgeMatch copy(RelTraitSet traitSet, RelNode input, RexNode targetIdExpression) {
        return new VirtualEdgeMatch(getCluster(), traitSet, input, targetIdExpression, nodeType, pathType);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
            .item("targetId", targetIdExpression);
    }

    public static VirtualEdgeMatch create(IMatchNode input, RexNode targetIdExpression,
                                          PathRecordType pathType) {
        EdgeRecordType nodeType = EdgeRecordType.emptyEdgeType(targetIdExpression.getType(),
            input.getCluster().getTypeFactory());
        return new VirtualEdgeMatch(input.getCluster(), input.getTraitSet(), input,
            targetIdExpression, nodeType, pathType);
    }

    @Override
    public PathRecordType getPathSchema() {
        return pathType;
    }

    @Override
    public RelDataType getNodeType() {
        return nodeType;
    }

    @Override
    public <T> T accept(MatchNodeVisitor<T> visitor) {
        return visitor.visitVirtualEdgeMatch(this);
    }

    @Override
    public RelNode getInput() {
        return input;
    }

    @Override
    public IMatchNode copy(List<RelNode> inputs, PathRecordType pathSchema) {
        return new VirtualEdgeMatch(getCluster(), traitSet, sole(inputs),
            targetIdExpression, rowType, pathSchema);
    }

    @Override
    public RelNode accept(RexShuttle shuttle) {
        RexNode newExpression = targetIdExpression.accept(shuttle);
        return copy(traitSet, input, newExpression);
    }
}
