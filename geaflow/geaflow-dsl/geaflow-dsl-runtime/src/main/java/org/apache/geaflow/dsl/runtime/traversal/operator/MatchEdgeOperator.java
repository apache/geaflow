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

package org.apache.geaflow.dsl.runtime.traversal.operator;

import java.util.*;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.geaflow.dsl.common.data.RowEdge;
import org.apache.geaflow.dsl.runtime.function.graph.MatchEdgeFunction;
import org.apache.geaflow.dsl.runtime.function.graph.MatchEdgeFunctionImpl;
import org.apache.geaflow.dsl.runtime.traversal.TraversalRuntimeContext;
import org.apache.geaflow.dsl.runtime.traversal.data.EdgeGroup;
import org.apache.geaflow.dsl.runtime.traversal.data.EdgeGroupRecord;
import org.apache.geaflow.dsl.runtime.traversal.data.VertexRecord;
import org.apache.geaflow.dsl.runtime.traversal.path.ITreePath;
import org.apache.geaflow.dsl.runtime.util.EdgeProjectorUtil;
import org.apache.geaflow.dsl.sqlnode.SqlMatchEdge.EdgeDirection;
import org.apache.geaflow.metrics.common.MetricNameFormatter;
import org.apache.geaflow.metrics.common.api.Histogram;

public class MatchEdgeOperator extends AbstractStepOperator<MatchEdgeFunction, VertexRecord, EdgeGroupRecord>
    implements FilteredFieldsOperator, LabeledStepOperator {

    private Histogram loadEdgeHg;
    private Histogram loadEdgeRt;

    private final boolean isOptionMatch;

    private Set<RexFieldAccess> fields;

    private EdgeProjectorUtil edgeProjector = null;

    @Override
    public StepOperator<VertexRecord, EdgeGroupRecord> withFilteredFields(Set<RexFieldAccess> fields) {
        this.fields = fields;
        return this;
    }

    public MatchEdgeOperator(long id, MatchEdgeFunction function) {
        super(id, function);
        isOptionMatch = function instanceof MatchEdgeFunctionImpl
            && ((MatchEdgeFunctionImpl) function).isOptionalMatchEdge();
    }

    @Override
    public void open(TraversalRuntimeContext context) {
        super.open(context);
        this.loadEdgeHg = metricGroup.histogram(MetricNameFormatter.loadEdgeCountRtName(getName()));
        this.loadEdgeRt = metricGroup.histogram(MetricNameFormatter.loadEdgeTimeRtName(getName()));
    }

    @Override
    public void processRecord(VertexRecord vertex) {
        long startTs = System.currentTimeMillis();
        EdgeGroup loadEdges = context.loadEdges(function.getEdgesFilter());
        loadEdgeRt.update(System.currentTimeMillis() - startTs);
        loadEdges = loadEdges.map(this::alignToOutputSchema);
        // filter by edge types if exists.
        EdgeGroup edgeGroup = loadEdges;
        if (!function.getEdgeTypes().isEmpty()) {
            edgeGroup = loadEdges.filter(edge ->

                function.getEdgeTypes().contains(edge.getBinaryLabel()));
        }
        Map<Object, ITreePath> targetTreePaths = new HashMap<>();

        // generate new paths.
        if (needAddToPath) {
            int numEdge = 0;
            for (RowEdge edge : edgeGroup) {
                if (edgeProjector == null) {
                    edgeProjector = new EdgeProjectorUtil(
                        graphSchema,
                        fields,
                        getOutputType()
                    );
                }
                if (fields != null && !fields.isEmpty()) {
                    edge = edgeProjector.projectEdge(edge);
                }

                // add edge to path.
                if (!targetTreePaths.containsKey(edge.getTargetId())) {
                    ITreePath newPath = vertex.getTreePath().extendTo(edge);
                    targetTreePaths.put(edge.getTargetId(), newPath);
                } else {
                    ITreePath treePath = targetTreePaths.get(edge.getTargetId());
                    treePath.getEdgeSet().addEdge(edge);
                }
                numEdge++;
            }
            if (numEdge == 0 && isOptionMatch) {
                ITreePath newPath = vertex.getTreePath().extendTo((RowEdge) null);
                targetTreePaths.put(null, newPath);
            }
            loadEdgeHg.update(numEdge);
        } else {
            if (!vertex.isPathEmpty()) { // inherit input path.
                int numEdge = 0;
                for (RowEdge edge : edgeGroup) {
                    targetTreePaths.put(edge.getTargetId(), vertex.getTreePath());
                    numEdge++;
                }
                if (numEdge == 0 && isOptionMatch) {
                    targetTreePaths.put(null, vertex.getTreePath());
                }
                loadEdgeHg.update(numEdge);
            }
        }
        EdgeGroupRecord edgeGroupRecord = EdgeGroupRecord.of(edgeGroup, targetTreePaths);
        collect(edgeGroupRecord);
    }

    @Override
    public String getLabel() {
        return function.getLabel();
    }

    @Override
    public StepOperator<VertexRecord, EdgeGroupRecord> copyInternal() {
        return new MatchEdgeOperator(id, function);
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
        str.append(getName());
        EdgeDirection direction = getFunction().getDirection();
        str.append("(").append(direction).append(")");
        String label = getLabel();
        str.append(" [").append(label).append("]");
        return str.toString();
    }
}
