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

import java.util.List;
import java.util.Objects;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.schema.GeaFlowGraph;

public abstract class GraphModify extends SingleRel {
    /**
     * The graph to write.
     */
    protected final GeaFlowGraph graph;

    protected GraphModify(RelOptCluster cluster, RelTraitSet traitSet, GeaFlowGraph graph, RelNode input) {
        super(cluster, traitSet, input);
        this.graph = Objects.requireNonNull(graph);
        validateInput(input);
    }

    private void validateInput(RelNode input) {
        RelDataType inputType = input.getRowType();
        RelDataType graphType = getRowType();
        if (inputType.getFieldCount() != graphType.getFieldCount()) {
            throw new GeaFlowDSLException("Input type field size: " + inputType.getFieldCount()
                + " is not equal to the target graph field size: " + graphType.getFieldCount());
        }
        for (int i = 0; i < inputType.getFieldCount(); i++) {
            RelDataType inputFieldType = inputType.getFieldList().get(i).getType();
            RelDataType targetFieldType = graphType.getFieldList().get(i).getType();
            if (!inputFieldType.equals(targetFieldType)) {
                throw new GeaFlowDSLException("Input field type: " + inputFieldType
                    + " is mismatch with the target graph field type: " + targetFieldType);
            }
        }
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("table", graph.getName());
    }

    @Override
    protected RelDataType deriveRowType() {
        return graph.getRowType(getCluster().getTypeFactory());
    }

    public abstract GraphModify copy(RelTraitSet traitSet, GeaFlowGraph graph, RelNode input);

    @Override
    public GraphModify copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert inputs.size() == 1;
        return copy(traitSet, graph, inputs.get(0));
    }

    public GeaFlowGraph getGraph() {
        return graph;
    }
}
