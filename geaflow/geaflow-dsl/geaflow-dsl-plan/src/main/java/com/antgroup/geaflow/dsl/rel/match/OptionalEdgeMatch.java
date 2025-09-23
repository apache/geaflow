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

package com.antgroup.geaflow.dsl.rel.match;

import com.antgroup.geaflow.dsl.calcite.PathRecordType;
import com.antgroup.geaflow.dsl.rel.MatchNodeVisitor;
import com.antgroup.geaflow.dsl.sqlnode.SqlMatchEdge.EdgeDirection;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexShuttle;

public class OptionalEdgeMatch extends EdgeMatch {

    private OptionalEdgeMatch(RelOptCluster cluster, RelTraitSet traitSet,
                              RelNode input, String label,
                              Collection<String> edgeTypes, EdgeDirection direction,
                              RelDataType nodeType, PathRecordType pathType) {
        super(cluster, traitSet, input, label, edgeTypes, direction, nodeType, pathType, new ArrayList<>());
    }

    @Override
    public IMatchNode copy(List<RelNode> inputs, PathRecordType pathSchema) {
        assert inputs.size() == 1;
        return new OptionalEdgeMatch(getCluster(), getTraitSet(), sole(inputs),
            getLabel(), getTypes(), getDirection(), getNodeType(), pathSchema);
    }

    @Override
    public OptionalEdgeMatch copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert inputs.size() == 1;
        return new OptionalEdgeMatch(getCluster(), getTraitSet(), sole(inputs),
            getLabel(), getTypes(), getDirection(), getNodeType(), getPathSchema());
    }

    @Override
    public RelNode accept(RexShuttle shuttle) {
        EdgeMatch newEdgeMatch = (EdgeMatch) super.accept(shuttle);
        return new OptionalEdgeMatch(
            newEdgeMatch.getCluster(), newEdgeMatch.getTraitSet(), newEdgeMatch.getInput(),
            newEdgeMatch.getLabel(), newEdgeMatch.getTypes(), newEdgeMatch.getDirection(),
            newEdgeMatch.getNodeType(), newEdgeMatch.getPathSchema());
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw);
    }

    public static OptionalEdgeMatch create(RelOptCluster cluster, SingleMatchNode input,
                                           String label, List<String> edgeTypes,
                                           EdgeDirection direction, RelDataType nodeType,
                                           PathRecordType pathType) {
        return new OptionalEdgeMatch(cluster, cluster.traitSet(), input, label, edgeTypes,
            direction, nodeType, pathType);
    }

    @Override
    public <T> T accept(MatchNodeVisitor<T> visitor) {
        return visitor.visitEdgeMatch(this);
    }
}
