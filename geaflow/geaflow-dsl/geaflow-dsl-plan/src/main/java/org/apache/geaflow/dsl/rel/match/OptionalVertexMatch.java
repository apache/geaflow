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

import java.util.Collection;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.geaflow.dsl.calcite.PathRecordType;
import org.apache.geaflow.dsl.rel.MatchNodeVisitor;
import org.apache.geaflow.dsl.util.GQLRelUtil;

public class OptionalVertexMatch extends VertexMatch {

    public OptionalVertexMatch(RelOptCluster cluster, RelTraitSet traitSet, RelNode input,
                               String label, Collection<String> vertexTypes, RelDataType nodeType,
                               PathRecordType pathType) {
        super(cluster, traitSet, input, label, vertexTypes, nodeType, pathType, null);
    }

    public OptionalVertexMatch(RelOptCluster cluster, RelTraitSet traitSet, RelNode input,
                               String label, Collection<String> vertexTypes, RelDataType nodeType,
                               PathRecordType pathType, RexNode pushDownFilter) {
        super(cluster, traitSet, input, label, vertexTypes, nodeType, pathType, pushDownFilter);
    }

    @Override
    public SingleMatchNode copy(List<RelNode> inputs, PathRecordType pathSchema) {
        assert inputs.size() <= 1;
        RelNode input = inputs.isEmpty() ? null : inputs.get(0);
        return new OptionalVertexMatch(getCluster(), traitSet, input, getLabel(),
            getTypes(), getNodeType(), pathSchema, getPushDownFilter());
    }

    @Override
    public OptionalVertexMatch copy(RelTraitSet traitSet, List<RelNode> inputs) {
        RelNode input = GQLRelUtil.oneInput(inputs);
        return new OptionalVertexMatch(getCluster(), getTraitSet(), input,
            getLabel(), getTypes(), getNodeType(), getPathSchema(), getPushDownFilter());
    }

    public OptionalVertexMatch copy(RexNode pushDownFilter) {
        return new OptionalVertexMatch(getCluster(), getTraitSet(), getInput(),
            getLabel(), getTypes(), getNodeType(), getPathSchema(), pushDownFilter);
    }

    @Override
    public <T> T accept(MatchNodeVisitor<T> visitor) {
        return visitor.visitVertexMatch(this);
    }


    public static OptionalVertexMatch create(RelOptCluster cluster, SingleMatchNode input, String label,
                                             List<String> vertexTypes, RelDataType nodeType, PathRecordType pathType) {
        return new OptionalVertexMatch(cluster, cluster.traitSet(), input, label, vertexTypes, nodeType, pathType);
    }
}
