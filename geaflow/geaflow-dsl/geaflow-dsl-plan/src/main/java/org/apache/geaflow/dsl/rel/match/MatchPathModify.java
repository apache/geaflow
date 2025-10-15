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

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.geaflow.dsl.calcite.GraphRecordType;
import org.apache.geaflow.dsl.calcite.PathRecordType;
import org.apache.geaflow.dsl.rel.MatchNodeVisitor;
import org.apache.geaflow.dsl.rel.PathModify;
import org.apache.geaflow.dsl.util.GQLRelUtil;

public class MatchPathModify extends PathModify implements SingleMatchNode {

    protected MatchPathModify(RelOptCluster cluster, RelTraitSet traits,
                              RelNode input,
                              List<PathModifyExpression> expressions, RelDataType rowType,
                              GraphRecordType modifyGraphType) {
        super(cluster, traits, input, expressions, rowType, modifyGraphType);
    }

    @Override
    public PathModify copy(RelTraitSet traitSet, RelNode input, List<PathModifyExpression> expressions,
                           RelDataType rowType) {
        return new MatchPathModify(getCluster(), traitSet, input, expressions, rowType, modifyGraphType);
    }

    @Override
    public PathRecordType getPathSchema() {
        return (PathRecordType) getRowType();
    }

    @Override
    public RelDataType getNodeType() {
        return ((IMatchNode) GQLRelUtil.toRel(getInput())).getNodeType();
    }

    @Override
    public <T> T accept(MatchNodeVisitor<T> visitor) {
        return visitor.visitPathModify(this);
    }

    @Override
    public IMatchNode copy(List<RelNode> inputs, PathRecordType pathSchema) {
        return new MatchPathModify(getCluster(), getTraitSet(), sole(inputs), expressions,
            pathSchema, modifyGraphType);
    }

    public static MatchPathModify create(RelNode input, List<PathModifyExpression> expressions,
                                         RelDataType rowType,
                                         GraphRecordType modifyGraphType) {
        return new MatchPathModify(input.getCluster(), input.getTraitSet(), input,
            expressions, rowType, modifyGraphType);
    }
}
