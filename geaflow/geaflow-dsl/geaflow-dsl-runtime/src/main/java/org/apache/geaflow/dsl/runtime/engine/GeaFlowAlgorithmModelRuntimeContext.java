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

package org.apache.geaflow.dsl.runtime.engine;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.geaflow.api.graph.function.vc.VertexCentricTraversalFunction.VertexCentricTraversalFuncContext;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.dsl.common.algo.AlgorithmModelRuntimeContext;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.RowEdge;
import org.apache.geaflow.dsl.common.types.GraphSchema;
import org.apache.geaflow.infer.InferContextLease;
import org.apache.geaflow.infer.InferContextPool;
import org.apache.geaflow.model.graph.edge.EdgeDirection;

public class GeaFlowAlgorithmModelRuntimeContext extends GeaFlowAlgorithmRuntimeContext
    implements AlgorithmModelRuntimeContext<Object, Object> {

    public GeaFlowAlgorithmModelRuntimeContext(
        GeaFlowAlgorithmAggTraversalFunction traversalFunction,
        VertexCentricTraversalFuncContext<Object, Row, Row, Object, Row> traversalContext,
        GraphSchema graphSchema) {
        super(traversalFunction, traversalContext, graphSchema);
    }

    @Override
    public Object infer(Map<String, Object> payload) {
        List<Object> results = inferBatch(Collections.singletonList(payload));
        return results.isEmpty() ? null : results.get(0);
    }

    @Override
    public List<Object> inferBatch(List<Map<String, Object>> payloads) {
        try {
            try (InferContextLease<Object> inferContextLease = InferContextPool.borrow(getConfig())) {
                List<Object> results = new ArrayList<>(payloads.size());
                for (Map<String, Object> payload : payloads) {
                    results.add(inferContextLease.infer(payload));
                }
                return results;
            }
        } catch (Exception e) {
            throw new GeaflowRuntimeException(
                String.format("GCN model batch infer failed, vertexId=%s, batchSize=%s",
                    getVertexId(), payloads.size()), e);
        }
    }

    @Override
    public Row loadDynamicVertexValue(Object vertexId) {
        return null;
    }

    @Override
    public List<RowEdge> loadDynamicEdges(Object vertexId, EdgeDirection direction) {
        return Collections.emptyList();
    }

    @Override
    public void close() {
    }
}
