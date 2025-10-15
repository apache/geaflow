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

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.geaflow.dsl.calcite.PathRecordType;
import org.apache.geaflow.dsl.calcite.VertexRecordType;
import org.apache.geaflow.dsl.rel.MatchNodeVisitor;

public class SubQueryStart extends AbstractRelNode implements SingleMatchNode {

    private final String queryName;

    private final PathRecordType pathType;

    private final VertexRecordType vertexType;

    protected SubQueryStart(RelOptCluster cluster, RelTraitSet traitSet, String queryName,
                            PathRecordType pathType, VertexRecordType vertexType) {
        super(cluster, traitSet);
        this.queryName = Objects.requireNonNull(queryName);
        this.pathType = pathType;
        this.rowType = pathType;
        this.vertexType = vertexType;
    }


    @Override
    public PathRecordType getPathSchema() {
        return pathType;
    }

    @Override
    public RelDataType getNodeType() {
        return vertexType;
    }

    @Override
    public <T> T accept(MatchNodeVisitor<T> visitor) {
        return visitor.visitSubQueryStart(this);
    }


    @Override
    public RelNode getInput() {
        return null;
    }

    @Override
    public List<RelNode> getInputs() {
        return Collections.emptyList();
    }

    @Override
    public IMatchNode copy(List<RelNode> inputs, PathRecordType pathSchema) {
        return new SubQueryStart(getCluster(), getTraitSet(), queryName, pathSchema, vertexType);
    }

    @Override
    public SubQueryStart copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert inputs.size() == 0;
        return new SubQueryStart(getCluster(), traitSet, queryName, pathType, vertexType);
    }

    public String getQueryName() {
        return queryName;
    }

    public static SubQueryStart create(RelOptCluster cluster, RelTraitSet traitSet, String queryName,
                                       PathRecordType parentPathType, VertexRecordType vertexType) {
        return new SubQueryStart(cluster, traitSet, queryName, parentPathType, vertexType);
    }
}
