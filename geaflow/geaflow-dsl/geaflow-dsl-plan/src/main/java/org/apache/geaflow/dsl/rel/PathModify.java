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

package org.apache.geaflow.dsl.rel;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexShuttle;
import org.apache.geaflow.dsl.calcite.GraphRecordType;
import org.apache.geaflow.dsl.rex.PathInputRef;
import org.apache.geaflow.dsl.rex.RexObjectConstruct;

public abstract class PathModify extends SingleRel {

    protected final ImmutableList<PathModifyExpression> expressions;

    protected final GraphRecordType modifyGraphType;

    protected PathModify(RelOptCluster cluster, RelTraitSet traits,
                         RelNode input, List<PathModifyExpression> expressions,
                         RelDataType rowType, GraphRecordType modifyGraphType) {
        super(cluster, traits, input);
        this.expressions = ImmutableList.copyOf(expressions);
        this.rowType = Objects.requireNonNull(rowType);
        this.modifyGraphType = Objects.requireNonNull(modifyGraphType);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("expressions", expressions);
    }

    @Override
    public PathModify copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert inputs.size() == 1;
        return copy(traitSet, inputs.get(0), expressions, rowType);
    }

    @Override
    public RelNode accept(RexShuttle shuttle) {
        List<PathModifyExpression> rewriteExpressions =
            expressions.stream().map(exp -> {
                PathInputRef rewriteLeftVar = (PathInputRef) exp.leftVar.accept(shuttle);
                RexObjectConstruct rewriteNode = (RexObjectConstruct) exp.getObjectConstruct().accept(shuttle);
                return new PathModifyExpression(rewriteLeftVar, rewriteNode);
            }).collect(Collectors.toList());
        return copy(traitSet, input, rewriteExpressions, rowType);
    }


    public abstract PathModify copy(RelTraitSet traitSet, RelNode input, List<PathModifyExpression> expressions,
                                    RelDataType rowType);

    public PathModify copy(RelDataType rowType) {
        return copy(traitSet, input, expressions, rowType);
    }

    public ImmutableList<PathModifyExpression> getExpressions() {
        return expressions;
    }

    public GraphRecordType getModifyGraphType() {
        return modifyGraphType;
    }

    public static class PathModifyExpression {

        private final PathInputRef leftVar;

        private final RexObjectConstruct expression;

        public PathModifyExpression(PathInputRef leftVar, RexObjectConstruct expression) {
            this.leftVar = leftVar;
            this.expression = expression;
        }

        public String getPathFieldName() {
            return leftVar.getLabel();
        }

        public int getIndex() {
            return leftVar.getIndex();
        }

        public PathInputRef getLeftVar() {
            return leftVar;
        }

        public RexObjectConstruct getObjectConstruct() {
            return expression;
        }

        public PathModifyExpression copy(RexObjectConstruct objectConstruct) {
            return new PathModifyExpression(leftVar, objectConstruct);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof PathModifyExpression)) {
                return false;
            }
            PathModifyExpression that = (PathModifyExpression) o;
            return Objects.equals(leftVar, that.leftVar) && Objects.equals(expression, that.expression);
        }

        @Override
        public int hashCode() {
            return Objects.hash(leftVar, expression);
        }

        @Override
        public String toString() {
            return leftVar.getLabel() + "=" + expression;
        }
    }
}
