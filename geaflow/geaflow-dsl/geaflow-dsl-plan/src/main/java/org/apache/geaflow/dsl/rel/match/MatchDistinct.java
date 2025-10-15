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
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexShuttle;
import org.apache.geaflow.dsl.calcite.PathRecordType;
import org.apache.geaflow.dsl.rel.MatchNodeVisitor;

public class MatchDistinct extends SingleRel implements SingleMatchNode {

    protected MatchDistinct(RelOptCluster cluster, RelTraitSet traits,
                            RelNode input) {
        super(cluster, traits, input);
    }

    public MatchDistinct copy(RelTraitSet traitSet, RelNode input) {
        return new MatchDistinct(getCluster(), traitSet, input);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return copy(traitSet, sole(inputs));
    }

    @Override
    public PathRecordType getPathSchema() {
        return (PathRecordType) input.getRowType();
    }

    @Override
    public RelDataType getNodeType() {
        return match(getInput()).getNodeType();
    }

    @Override
    public <T> T accept(MatchNodeVisitor<T> visitor) {
        return visitor.visitDistinct(this);
    }

    @Override
    public RelNode getInput() {
        return input;
    }

    @Override
    public IMatchNode copy(List<RelNode> inputs, PathRecordType pathSchema) {
        return copy(traitSet, sole(inputs));
    }

    @Override
    public RelNode accept(RexShuttle shuttle) {
        return copy(traitSet, input);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("distinct", true);
    }

    public static MatchDistinct create(IMatchNode input) {
        return new MatchDistinct(input.getCluster(), input.getTraitSet(), input);
    }
}
