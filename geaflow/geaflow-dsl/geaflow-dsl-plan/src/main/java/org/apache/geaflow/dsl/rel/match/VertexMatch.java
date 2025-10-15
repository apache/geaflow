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
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.geaflow.dsl.calcite.PathRecordType;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.rel.MatchNodeVisitor;
import org.apache.geaflow.dsl.util.GQLRelUtil;

public class VertexMatch extends AbstractRelNode implements SingleMatchNode, IMatchLabel {

    private RelNode input;

    private final String label;

    private Set<RexFieldAccess> fields;

    private final ImmutableSet<String> vertexTypes;

    private final PathRecordType pathType;

    private final RelDataType nodeType;

    /**
     * The filter pushed down to the first vertex match.
     */
    private RexNode pushDownFilter;

    private Set<Object> idSet;

    public VertexMatch(RelOptCluster cluster, RelTraitSet traitSet, RelNode input,
                       String label, Collection<String> vertexTypes, RelDataType nodeType,
                       PathRecordType pathType) {
        this(cluster, traitSet, input, label, vertexTypes, nodeType, pathType, null);
    }

    public VertexMatch(RelOptCluster cluster, RelTraitSet traitSet, RelNode input,
                       String label, Collection<String> vertexTypes, RelDataType nodeType,
                       PathRecordType pathType, RexNode pushDownFilter) {
        this(cluster, traitSet, input, label, vertexTypes, nodeType, pathType, pushDownFilter,
            new HashSet<>(), null);
    }

    public VertexMatch(RelOptCluster cluster, RelTraitSet traitSet, RelNode input,
                       String label, Collection<String> vertexTypes, RelDataType nodeType,
                       PathRecordType pathType, RexNode pushDownFilter, Set<Object> idSet,
                       Set<RexFieldAccess> fields) {
        super(cluster, traitSet);
        this.input = input;
        this.label = label;
        this.vertexTypes = ImmutableSet.copyOf(vertexTypes);

        if (input != null && !(GQLRelUtil.toRel(input) instanceof SubQueryStart)
            && match(input).getNodeType().getSqlTypeName() != SqlTypeName.EDGE) {
            throw new GeaFlowDSLException("Illegal input type: " + match(input).getNodeType().getSqlTypeName()
                + " for: " + getRelTypeName() + ", should be: " + SqlTypeName.EDGE);
        }
        this.rowType = Objects.requireNonNull(pathType);
        this.pathType = Objects.requireNonNull(pathType);
        this.nodeType = Objects.requireNonNull(nodeType);
        this.pushDownFilter = pushDownFilter;
        this.idSet = idSet;
        this.fields = fields;
    }

    public void addField(RexFieldAccess field) {
        if (fields == null) {
            fields = new HashSet<>();
        }
        fields.add(field);
    }

    @Override
    public String getLabel() {
        return label;
    }

    @Override
    public Set<String> getTypes() {
        return vertexTypes;
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

    public RexNode getPushDownFilter() {
        return pushDownFilter;
    }

    public Set<Object> getIdSet() {
        return idSet;
    }

    public Set<RexFieldAccess> getFields() {
        return fields;
    }

    @Override
    public SingleMatchNode copy(List<RelNode> inputs, PathRecordType pathSchema) {
        assert inputs.size() <= 1;
        RelNode input = inputs.isEmpty() ? null : inputs.get(0);
        return new VertexMatch(getCluster(), traitSet, input, label,
            vertexTypes, nodeType, pathSchema, pushDownFilter, idSet, fields);
    }

    @Override
    public VertexMatch copy(RelTraitSet traitSet, List<RelNode> inputs) {
        RelNode input = GQLRelUtil.oneInput(inputs);
        return new VertexMatch(getCluster(), getTraitSet(), input,
            label, vertexTypes, nodeType, pathType, pushDownFilter, idSet, fields);
    }

    public VertexMatch copy(RexNode pushDownFilter) {
        return new VertexMatch(getCluster(), getTraitSet(), input,
            label, vertexTypes, nodeType, pathType, pushDownFilter, idSet, fields);
    }

    public VertexMatch copy(Set<Object> idSet) {
        return new VertexMatch(getCluster(), getTraitSet(), input,
            label, vertexTypes, nodeType, pathType, pushDownFilter, idSet, fields);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
            .item("input", input)
            .item("label", label)
            .item("vertexTypes", vertexTypes)
            .item("idSet", idSet);
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

    public static VertexMatch create(RelOptCluster cluster, SingleMatchNode input, String label,
                                     List<String> vertexTypes, RelDataType nodeType, PathRecordType pathType) {
        return new VertexMatch(cluster, cluster.traitSet(), input, label, vertexTypes, nodeType, pathType);
    }

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
        return visitor.visitVertexMatch(this);
    }

    @Override
    public RelNode accept(RexShuttle shuttle) {
        if (pushDownFilter != null) {
            RexNode newPushDownFilter = pushDownFilter.accept(shuttle);
            return copy(newPushDownFilter);
        }
        return this;
    }
}
