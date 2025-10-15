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
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.geaflow.common.utils.ArrayUtil;
import org.apache.geaflow.dsl.calcite.PathRecordType;
import org.apache.geaflow.dsl.calcite.UnionPathRecordType;
import org.apache.geaflow.dsl.rel.MatchNodeVisitor;
import org.apache.geaflow.dsl.util.GQLRelUtil;

public class MatchUnion extends Union implements IMatchNode {

    protected MatchUnion(RelOptCluster cluster, RelTraitSet traits,
                         List<RelNode> inputs, boolean all) {
        super(cluster, traits, ArrayUtil.castList(inputs), all);
    }

    @Override
    public SetOp copy(RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
        return new MatchUnion(getCluster(), traitSet, ArrayUtil.castList(inputs), all);
    }

    public static MatchUnion create(RelOptCluster cluster, RelTraitSet traits,
                                    List<RelNode> inputs, boolean all) {
        return new MatchUnion(cluster, traits, inputs, all);
    }

    @Override
    protected RelDataType deriveRowType() {
        List<PathRecordType> inputPathTypes = inputs.stream()
            .map(input -> ((IMatchNode) GQLRelUtil.toRel(input)).getPathSchema())
            .collect(Collectors.toList());
        return new UnionPathRecordType(inputPathTypes, getCluster().getTypeFactory());
    }

    @Override
    public PathRecordType getPathSchema() {
        return (PathRecordType) getRowType();
    }

    @Override
    public RelDataType getNodeType() {
        return getPathSchema();
    }

    @Override
    public IMatchNode copy(List<RelNode> inputs, PathRecordType pathType) {
        return new MatchUnion(getCluster(), getTraitSet(), inputs, all);
    }

    @Override
    public <T> T accept(MatchNodeVisitor<T> visitor) {
        return visitor.visitUnion(this);
    }
}
