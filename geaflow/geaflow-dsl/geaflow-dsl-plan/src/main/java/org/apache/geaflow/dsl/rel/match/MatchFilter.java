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
import org.apache.calcite.rex.RexChecker;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.Litmus;
import org.apache.geaflow.dsl.calcite.PathRecordType;
import org.apache.geaflow.dsl.rel.MatchNodeVisitor;
import org.apache.geaflow.dsl.util.GQLRelUtil;

public class MatchFilter extends SingleRel implements SingleMatchNode {

    private final RexNode condition;
    private final PathRecordType pathType;

    protected MatchFilter(RelOptCluster cluster, RelTraitSet traits,
                          RelNode input, RexNode condition, PathRecordType pathType) {
        super(cluster, traits, input);
        this.condition = Objects.requireNonNull(condition);
        this.pathType = Objects.requireNonNull(pathType);
    }

    public MatchFilter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
        return copy(traitSet, input, condition, pathType);
    }

    public MatchFilter copy(RelTraitSet traitSet, RelNode input,
                            RexNode condition, PathRecordType pathType) {
        return new MatchFilter(getCluster(), traitSet, input, condition, pathType);
    }

    @Override
    public IMatchNode copy(List<RelNode> inputs, PathRecordType pathSchema) {
        return copy(traitSet, sole(inputs), condition, pathSchema);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return copy(traitSet, sole(inputs), condition);
    }

    @Override
    public PathRecordType getPathSchema() {
        return pathType;
    }

    @Override
    public RelDataType getNodeType() {
        return match(input).getNodeType();
    }

    @Override
    public <T> T accept(MatchNodeVisitor<T> visitor) {
        return visitor.visitFilter(this);
    }

    @Override
    public boolean isValid(Litmus litmus, Context context) {
        if (RexUtil.isNullabilityCast(getCluster().getTypeFactory(), condition)) {
            return litmus.fail("Cast for just nullability not allowed");
        }
        final RexChecker checker =
            new RexChecker(((IMatchNode) GQLRelUtil.toRel(getInput())).getPathSchema(), context, litmus);
        condition.accept(checker);
        if (checker.getFailureCount() > 0) {
            return litmus.fail(null);
        }
        return litmus.succeed();
    }

    @Override
    public RelNode getInput() {
        return input;
    }

    @Override
    public RelNode accept(RexShuttle shuttle) {
        RexNode newCondition = condition.accept(shuttle);
        return copy(traitSet, input, newCondition);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
            .item("condition", condition);
    }

    public RexNode getCondition() {
        return condition;
    }

    public static MatchFilter create(RelNode input, RexNode condition, PathRecordType pathType) {
        return new MatchFilter(input.getCluster(), input.getTraitSet(), input, condition, pathType);
    }
}
