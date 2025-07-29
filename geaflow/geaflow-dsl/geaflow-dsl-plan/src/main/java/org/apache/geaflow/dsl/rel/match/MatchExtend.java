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

import com.google.common.collect.ImmutableList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexShuttle;
import org.apache.geaflow.dsl.calcite.GraphRecordType;
import org.apache.geaflow.dsl.calcite.PathRecordType;
import org.apache.geaflow.dsl.rel.MatchNodeVisitor;
import org.apache.geaflow.dsl.rel.PathModify;
import org.apache.geaflow.dsl.rex.PathInputRef;
import org.apache.geaflow.dsl.rex.RexObjectConstruct;
import org.apache.geaflow.dsl.util.GQLRelUtil;

public class MatchExtend extends PathModify implements SingleMatchNode {

    private final Set<String> rewriteFields;

    private MatchExtend(RelOptCluster cluster, RelTraitSet traits, RelNode input,
                        List<PathModifyExpression> expressions, RelDataType rowType,
                        GraphRecordType graphType) {
        super(cluster, traits, input, expressions.stream()
            .filter(exp -> rowType.getFieldNames().contains(exp.getPathFieldName()))
            .collect(Collectors.toList()), rowType, graphType);
        rewriteFields = new HashSet<>();
        for (int i = 0; i < this.expressions.size(); i++) {
            if (input.getRowType().getFieldNames().contains(this.expressions.get(i).getLeftVar().getLabel())) {
                rewriteFields.add(this.expressions.get(i).getLeftVar().getLabel());
            }
        }
    }

    public static MatchExtend create(RelNode input, List<PathModifyExpression> expressions,
                                     RelDataType rowType, GraphRecordType graphType) {
        return new MatchExtend(input.getCluster(), input.getTraitSet(), input, expressions, rowType,
            graphType);
    }

    public MatchExtend copy(RelTraitSet traitSet, RelNode input,
                            List<PathModifyExpression> expressions, RelDataType rowType,
                            GraphRecordType graphType) {
        return new MatchExtend(getCluster(), traitSet, input, expressions, rowType, graphType);
    }

    @Override
    public PathRecordType getPathSchema() {
        return (PathRecordType) getRowType();
    }

    @Override
    public RelDataType getNodeType() {
        return ((IMatchNode) GQLRelUtil.toRel(getInput())).getNodeType();
    }

    public Set<String> getRewriteFields() {
        return rewriteFields;
    }

    @Override
    public IMatchNode copy(List<RelNode> inputs, PathRecordType pathSchema) {
        return new MatchExtend(getCluster(), getTraitSet(), sole(inputs), expressions,
            pathSchema, modifyGraphType);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("expressions", expressions);
    }

    @Override
    public MatchExtend copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert inputs.size() == 1;
        return copy(traitSet, inputs.get(0), expressions, rowType, modifyGraphType);
    }

    @Override
    public RelNode accept(RexShuttle shuttle) {
        List<PathModifyExpression> rewriteExpressions =
            expressions.stream().map(exp -> {
                //If the LeftVar reference does not exist in the input, MatchExtend will default to
                // extending at the end of the path, and LeftVar does not need to be rewritten.
                PathInputRef leftVar = rewriteFields.contains(exp.getLeftVar().getLabel())
                    ? (PathInputRef) exp.getLeftVar().accept(shuttle) : exp.getLeftVar();
                RexObjectConstruct rewriteNode = (RexObjectConstruct) exp.getObjectConstruct().accept(shuttle);
                return new PathModifyExpression(leftVar, rewriteNode);
            }).collect(Collectors.toList());
        return copy(traitSet, input, rewriteExpressions, rowType);
    }

    @Override
    public PathModify copy(RelTraitSet traitSet, RelNode input,
                           List<PathModifyExpression> expressions, RelDataType rowType) {
        return copy(traitSet, input, expressions, rowType, modifyGraphType);
    }

    public ImmutableList<PathModifyExpression> getExpressions() {
        return ImmutableList.copyOf(expressions);
    }

    @Override
    public <T> T accept(MatchNodeVisitor<T> visitor) {
        return visitor.visitExtend(this);
    }
}
