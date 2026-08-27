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

package org.apache.geaflow.dsl.common.algo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.RowEdge;
import org.apache.geaflow.model.graph.edge.EdgeDirection;

/**
 * Runtime context for model-backed algorithms that need inference and access to
 * the active vertex's dynamic value and edges in the current batch.
 *
 * @param <K> The type of vertex IDs.
 * @param <M> The type of messages that can be sent between vertices.
 */
public interface AlgorithmModelRuntimeContext<K, M> extends AlgorithmRuntimeContext<K, M> {

    Object infer(Map<String, Object> payload);

    default List<Object> inferBatch(List<Map<String, Object>> payloads) {
        List<Object> results = new ArrayList<>(payloads.size());
        for (Map<String, Object> payload : payloads) {
            results.add(infer(payload));
        }
        return results;
    }

    Row loadDynamicVertexValue(Object vertexId);

    List<RowEdge> loadDynamicEdges(Object vertexId, EdgeDirection direction);
}
