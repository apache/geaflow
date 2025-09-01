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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.dsl.common.data.Path;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.RowKey;
import org.apache.geaflow.dsl.common.data.StepRecord;
import org.apache.geaflow.dsl.common.data.impl.DefaultPath;
import org.apache.geaflow.dsl.common.data.impl.ObjectRow;
import org.apache.geaflow.dsl.common.types.EdgeType;
import org.apache.geaflow.dsl.common.types.ObjectType;
import org.apache.geaflow.dsl.common.types.VertexType;
import org.apache.geaflow.dsl.runtime.function.graph.StepAggExpressionFunctionImpl;
import org.apache.geaflow.dsl.runtime.function.graph.StepAggregateFunction;
import org.apache.geaflow.dsl.runtime.function.graph.StepKeyFunction;
import org.apache.geaflow.dsl.runtime.traversal.TraversalRuntimeContext;
import org.apache.geaflow.dsl.runtime.traversal.data.EdgeGroup;
import org.apache.geaflow.dsl.runtime.traversal.data.EdgeGroupRecord;
import org.apache.geaflow.dsl.runtime.traversal.data.IdOnlyVertex;
import org.apache.geaflow.dsl.runtime.traversal.data.ParameterRequest;
import org.apache.geaflow.dsl.runtime.traversal.data.VertexRecord;
import org.apache.geaflow.dsl.runtime.traversal.path.ITreePath;
import org.apache.geaflow.dsl.runtime.traversal.path.TreePaths;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.vertex.IVertex;

public class StepGlobalAggregateOperator extends AbstractStepOperator<StepAggregateFunction, VertexRecord, StepRecord> {

    private final StepKeyFunction groupByFunction;
    private Map<ParameterRequest, Map<RowKey, Row>> requestId2Path;
    private Map<ParameterRequest, Map<RowKey, Object>> requestId2Accumulators;
    private final int[] pathPruneIndices;
    private final IType<?>[] pathPruneTypes;
    private final IType<?>[] inputPathTypes;
    boolean isPathHeadVertexType;
    boolean isPathTailVertexType;
    private final IType<?>[] aggregateNodeTypes;
    private final IType<?>[] aggOutputTypes;

    public StepGlobalAggregateOperator(long id, StepKeyFunction keyFunction,
                                       StepAggregateFunction function) {
        super(id, function);
        this.groupByFunction = Objects.requireNonNull(keyFunction);
        this.pathPruneIndices = ((StepAggExpressionFunctionImpl) function).getPathPruneIndices();
        this.pathPruneTypes = ((StepAggExpressionFunctionImpl) function).getPathPruneTypes();
        this.inputPathTypes = ((StepAggExpressionFunctionImpl) function).getInputPathTypes();
        this.aggOutputTypes = ((StepAggExpressionFunctionImpl) function).getAggOutputTypes();
        assert pathPruneIndices.length > 0 && pathPruneTypes.length == pathPruneIndices.length;
        this.isPathHeadVertexType = pathPruneTypes[0] instanceof VertexType;
        this.isPathTailVertexType = pathPruneTypes[pathPruneTypes.length - 1] instanceof VertexType;
        this.aggregateNodeTypes = isPathHeadVertexType
            ? ((VertexType) inputPathTypes[pathPruneIndices[0]]).getValueTypes()
            : ((EdgeType) inputPathTypes[pathPruneIndices[0]]).getValueTypes();
    }

    @Override
    public void open(TraversalRuntimeContext context) {
        super.open(context);
        requestId2Path = new HashMap<>();
        requestId2Accumulators = new HashMap<>();
    }

    @Override
    protected void processRecord(VertexRecord record) {
        ParameterRequest request = context.getRequest();
        if (!requestId2Accumulators.containsKey(request)) {
            requestId2Path.put(request, new HashMap<>());
            requestId2Accumulators.put(request, new HashMap<>());
        }
        record.mapPath(path -> {
            RowKey key = groupByFunction.getKey(path);
            Map<RowKey, Row> key2Path = requestId2Path.get(request);
            Map<RowKey, Object> key2Acc = requestId2Accumulators.get(request);
            Path prunedPath = path.subPath(pathPruneIndices);
            Object partAccumulator;
            if (isPathHeadVertexType) {
                partAccumulator = ((IVertex<Object, Row>) prunedPath.getPathNodes().get(0))
                    .getValue().getField(aggregateNodeTypes.length, ObjectType.INSTANCE);
            } else {
                partAccumulator = ((IEdge<Object, Row>) prunedPath.getPathNodes().get(0))
                    .getValue().getField(aggregateNodeTypes.length, ObjectType.INSTANCE);
            }
            if (!key2Acc.containsKey(key)) {
                key2Acc.put(key, partAccumulator);
                key2Path.put(key, prunedPath);
            } else {
                Object accumulator = key2Acc.get(key);
                function.merge(accumulator, partAccumulator);
                key2Acc.put(key, accumulator);
            }
            return path;
        }, null);
    }

    @Override
    public void finish() {
        for (Map.Entry<ParameterRequest, Map<RowKey, Object>> entry : requestId2Accumulators.entrySet()) {
            ParameterRequest request = entry.getKey();
            context.setRequest(request);
            Map<RowKey, Object> key2Acc = entry.getValue();
            Map<RowKey, Row> key2Path = requestId2Path.get(request);
            for (Entry<RowKey, Object> rowKeyObjectEntry : key2Acc.entrySet()) {
                RowKey rowKey = rowKeyObjectEntry.getKey();
                Path path = (Path) key2Path.get(rowKey);
                Row[] values = new Row[pathPruneIndices.length];
                for (int i = 0; i < pathPruneIndices.length; i++) {
                    values[i] = path.getField(i, inputPathTypes[pathPruneIndices[i]]);
                }
                Row aggregateNodeValue;
                int aggregateNodeIndex = 0;
                if (isPathHeadVertexType) {
                    aggregateNodeValue = ((IVertex<Object, Row>) values[aggregateNodeIndex]).getValue();
                } else {
                    aggregateNodeValue = ((IEdge<Object, Row>) values[aggregateNodeIndex]).getValue();
                }
                Object[] aggregateNodeValues = new Object[aggregateNodeTypes.length
                    + rowKey.getKeys().length + aggOutputTypes.length];
                for (int i = 0; i < aggregateNodeTypes.length; i++) {
                    aggregateNodeValues[i] = aggregateNodeValue.getField(i, aggregateNodeTypes[i]);
                }
                int offset = aggregateNodeTypes.length;
                for (int j = 0; j < rowKey.getKeys().length; j++) {
                    aggregateNodeValues[offset + j] = rowKey.getKeys()[j];
                }
                offset = aggregateNodeTypes.length + rowKey.getKeys().length;
                Object accumulator = rowKeyObjectEntry.getValue();
                ObjectRow accumulatorValues = (ObjectRow) function.getValue(accumulator).getValue(ObjectType.INSTANCE);
                for (int j = 0; j < aggOutputTypes.length; j++) {
                    aggregateNodeValues[offset + j] = accumulatorValues.getField(j, aggOutputTypes[j]);
                }
                if (isPathHeadVertexType) {
                    values[aggregateNodeIndex] = (Row) ((IVertex<Object, Row>) values[aggregateNodeIndex])
                        .withValue(ObjectRow.create(aggregateNodeValues));
                } else {
                    values[aggregateNodeIndex] = (Row) ((IEdge<Object, Row>) values[aggregateNodeIndex])
                        .withValue(ObjectRow.create(aggregateNodeValues));
                }
                ITreePath globalAggPath = TreePaths.singletonPath(new DefaultPath(values));
                if (isPathTailVertexType) {
                    collect(VertexRecord.of(IdOnlyVertex.of(globalAggPath.getVertexId()), globalAggPath));
                } else {
                    Map<Object, ITreePath> targetId2TreePaths = new HashMap<>();
                    targetId2TreePaths.put(globalAggPath.getEdgeSet().getTargetId(), globalAggPath);
                    collect(EdgeGroupRecord.of(EdgeGroup.of(globalAggPath.getEdgeSet()),
                        targetId2TreePaths));
                }
            }
        }
        requestId2Accumulators.clear();
        requestId2Path.clear();
        super.finish();
    }

    @Override
    public StepOperator<VertexRecord, StepRecord> copyInternal() {
        return new StepGlobalAggregateOperator(id, groupByFunction, function);
    }
}
