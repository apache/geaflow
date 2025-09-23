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

import static com.antgroup.geaflow.dsl.util.GQLRelUtil.match;

import com.antgroup.geaflow.dsl.calcite.PathRecordType;
import com.antgroup.geaflow.dsl.common.exception.GeaFlowDSLException;
import com.antgroup.geaflow.dsl.rel.MatchNodeVisitor;
import com.antgroup.geaflow.dsl.sqlnode.SqlMatchEdge.EdgeDirection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.*;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.sql.type.SqlTypeName;

public class EdgeMatch extends AbstractRelNode implements SingleMatchNode, IMatchLabel {

    private RelNode input;

    private final String label;

    private List<RexFieldAccess> fields;

    private final ImmutableSet<String> edgeTypes;

    private final EdgeDirection direction;

    private final PathRecordType pathType;

    private final RelDataType nodeType;

    protected EdgeMatch(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, String label,
              Collection<String> edgeTypes, EdgeDirection direction, RelDataType nodeType,
              PathRecordType pathType, List<RexFieldAccess> fields) {
        super(cluster, traitSet);
        this.input = input;
        this.label = label;
        this.edgeTypes = ImmutableSet.copyOf(edgeTypes);
        if (input != null && match(input).getNodeType().getSqlTypeName() != SqlTypeName.VERTEX) {
            throw new GeaFlowDSLException("Illegal input type: " + match(input).getNodeType().getSqlTypeName()
                + " for " + getRelTypeName() + ", should be: " + SqlTypeName.VERTEX);
        }
        this.direction = direction;
        this.rowType = Objects.requireNonNull(pathType);
        this.pathType = Objects.requireNonNull(pathType);
        this.nodeType = Objects.requireNonNull(nodeType);
        this.fields = fields;
    }

    public void addField(RexFieldAccess field) {
        if (fields == null) {
            fields = new ArrayList<>();
        }
        fields.add(field);
    }

    @Override
    public String getLabel() {
        return label;
    }

    @Override
    public Set<String> getTypes() {
        return edgeTypes;
    }

    @Override
    public List<RelNode> getInputs() {
        if (input == null) {
            return Collections.emptyList();
        }
        return ImmutableList.of(input);
    }

    @Override
    public RelNode getInput() {
        return input;
    }

    @Override
    public IMatchNode copy(List<RelNode> inputs, PathRecordType pathSchema) {
        return new EdgeMatch(getCluster(), traitSet, sole(inputs), label, edgeTypes,
            direction, nodeType, pathSchema, fields);
    }

    public EdgeDirection getDirection() {
        return direction;
    }

    @Override
    public EdgeMatch copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert inputs.size() == 1;
        return new EdgeMatch(getCluster(), getTraitSet(), sole(inputs),
            label, edgeTypes, direction, nodeType, pathType, fields);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
            .item("input", input)
            .item("label", label)
            .item("edgeTypes", edgeTypes)
            .item("direction", direction);
    }

    @Override
    public void replaceInput(int ordinalInParent, RelNode p) {
        assert ordinalInParent == 0;
        this.input = p;
    }

    @Override
    protected RelDataType deriveRowType() {
        throw new UnsupportedOperationException();
    }

    public static EdgeMatch create(RelOptCluster cluster, SingleMatchNode input,
                                   String label, List<String> edgeTypes,
                                   EdgeDirection direction, RelDataType nodeType,
                                   PathRecordType pathType) {
        return new EdgeMatch(cluster, cluster.traitSet(), input, label, edgeTypes,
            direction, nodeType, pathType, null);
    }


    public List<RexFieldAccess> getFields() {return fields;}

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
        return visitor.visitEdgeMatch(this);
    }
}
