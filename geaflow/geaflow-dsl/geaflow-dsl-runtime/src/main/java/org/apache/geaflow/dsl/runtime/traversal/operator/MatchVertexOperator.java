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
import org.apache.geaflow.dsl.common.data.*;
import org.apache.geaflow.dsl.common.data.StepRecord.StepRecordType;
import org.apache.geaflow.dsl.common.data.impl.VertexEdgeFactory;
import org.apache.geaflow.dsl.common.types.VertexType;
import org.apache.geaflow.dsl.runtime.function.graph.MatchVertexFunction;
import org.apache.geaflow.dsl.runtime.function.graph.MatchVertexFunctionImpl;
import org.apache.geaflow.dsl.runtime.traversal.TraversalRuntimeContext;
import org.apache.geaflow.dsl.runtime.traversal.data.EdgeGroup;
import org.apache.geaflow.dsl.runtime.traversal.data.EdgeGroupRecord;
import org.apache.geaflow.dsl.runtime.traversal.data.IdOnlyVertex;
import org.apache.geaflow.dsl.runtime.traversal.data.VertexRecord;
import org.apache.geaflow.dsl.runtime.traversal.path.ITreePath;
import org.apache.geaflow.dsl.runtime.util.VertexProjectorUtil;
import org.apache.geaflow.metrics.common.MetricNameFormatter;
import org.apache.geaflow.metrics.common.api.Histogram;

public class MatchVertexOperator extends AbstractStepOperator<MatchVertexFunction, StepRecord,
    VertexRecord> implements FilteredFieldsOperator, LabeledStepOperator {

    private Histogram loadVertexRt;

    private final boolean isOptionMatch;

    private Set<Object> idSet;

    private Set<RexFieldAccess> fields;

    private VertexProjectorUtil vertexProjector = null;

    @Override
    public StepOperator<StepRecord, VertexRecord> withFilteredFields(Set<RexFieldAccess> fields) {
        this.fields = fields;
        return this;
    }

    public MatchVertexOperator(long id, MatchVertexFunction function) {
        super(id, function);
        if (function instanceof MatchVertexFunctionImpl) {
            isOptionMatch = ((MatchVertexFunctionImpl) function).isOptionalMatchVertex();
            idSet = ((MatchVertexFunctionImpl) function).getIdSet();
        } else {
            isOptionMatch = false;
        }
    }

    @Override
    public void open(TraversalRuntimeContext context) {
        super.open(context);
        loadVertexRt = metricGroup.histogram(MetricNameFormatter.loadVertexTimeRtName(getName()));
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void processRecord(StepRecord record) {
        if (record.getType() == StepRecordType.VERTEX) {
            processVertex((VertexRecord) record);
        } else {
            EdgeGroupRecord edgeGroupRecord = (EdgeGroupRecord) record;
            processEdgeGroup(edgeGroupRecord);
        }
    }


    private void processVertex(VertexRecord vertexRecord) {
        RowVertex vertex = vertexRecord.getVertex();
        if (vertex instanceof IdOnlyVertex && needLoadVertex(vertex.getId())) {
            long startTs = System.currentTimeMillis();
            vertex = context.loadVertex(vertex.getId(),
                function.getVertexFilter(),
                graphSchema,
                addingVertexFieldTypes);
            loadVertexRt.update(System.currentTimeMillis() - startTs);

            if (vertexProjector == null) {
                vertexProjector = new VertexProjectorUtil(
                    graphSchema,
                    fields,
                    addingVertexFieldNames,
                    addingVertexFieldTypes
                );
            }
            if (fields != null && !fields.isEmpty()) {
                vertex = vertexProjector.projectVertex(vertex);
            }

            if (vertex == null && !isOptionMatch) {
                // load a non-exists vertex, just skip.
                return;
            }
        }

        if (vertex != null) {
            if (!function.getVertexTypes().isEmpty()
                && !function.getVertexTypes().contains(vertex.getBinaryLabel())) {
                // filter by the vertex types.
                return;
            }
            if (!idSet.isEmpty() && !idSet.contains(vertex.getId())) {
                return;
            }
            vertex = alignToOutputSchema(vertex);
        }

        ITreePath currentPath;
        if (needAddToPath) {
            currentPath = vertexRecord.getTreePath().extendTo(vertex);
        } else {
            currentPath = vertexRecord.getTreePath();
        }
        if (vertex == null) {
            vertex = VertexEdgeFactory.createVertex((VertexType) getOutputType());
        }
        collect(VertexRecord.of(vertex, currentPath));
    }

    private void processEdgeGroup(EdgeGroupRecord edgeGroupRecord) {
        EdgeGroup edgeGroup = edgeGroupRecord.getEdgeGroup();
        for (RowEdge edge : edgeGroup) {
            Object targetId = edge.getTargetId();
            // load targetId.
            RowVertex vertex = context.loadVertex(targetId, function.getVertexFilter(), graphSchema, addingVertexFieldTypes);
            if (vertex != null) {
                ITreePath treePath = edgeGroupRecord.getPathById(targetId);
                // set current vertex.
                context.setVertex(vertex);
                // process new vertex.
                processVertex(VertexRecord.of(vertex, treePath));
            } else if (isOptionMatch) {
                vertex = VertexEdgeFactory.createVertex((VertexType) getOutputType());
                ITreePath treePath = edgeGroupRecord.getPathById(targetId);
                // set current vertex.
                context.setVertex(vertex);
                // process new vertex.
                processVertex(VertexRecord.of(null, treePath));
            }
        }
    }

    private boolean needLoadVertex(Object vertexId) {
        // skip load virtual id.
        return !(vertexId instanceof VirtualId);
    }

    @Override
    public void close() {

    }

    @Override
    public StepOperator<StepRecord, VertexRecord> copyInternal() {
        return new MatchVertexOperator(id, function);
    }

    @Override
    public String getLabel() {
        return function.getLabel();
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
        str.append(getName());
        String label = getLabel();
        str.append(" [").append(label).append("]");
        return str.toString();
    }
}
