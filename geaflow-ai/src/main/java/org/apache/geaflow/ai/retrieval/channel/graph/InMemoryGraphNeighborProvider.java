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

package org.apache.geaflow.ai.retrieval.channel.graph;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.geaflow.ai.retrieval.model.graph.GraphEdgeRef;

/** Deterministic in-memory graph provider used by fixtures and artifact readers. */
public final class InMemoryGraphNeighborProvider implements GraphNeighborProvider {
    private final Map<String, List<GraphEdgeRef>> byEntity = new HashMap<>();

    public InMemoryGraphNeighborProvider(List<GraphEdgeRef> edges) {
        if (edges != null) {
            for (GraphEdgeRef edge : edges) {
                if (edge != null) {
                    add(edge.getSourceEntityId(), edge);
                    add(edge.getTargetEntityId(), edge);
                }
            }
        }
        for (List<GraphEdgeRef> values : byEntity.values()) {
            values.sort(Comparator.comparing(GraphEdgeRef::getEdgeId)
                .thenComparing(GraphEdgeRef::getSourceEntityId)
                .thenComparing(GraphEdgeRef::getTargetEntityId)
                .thenComparing(GraphEdgeRef::getLabel));
        }
    }

    private void add(String entityId, GraphEdgeRef edge) {
        byEntity.computeIfAbsent(entityId, ignored -> new ArrayList<>()).add(edge);
    }

    @Override
    public List<GraphEdgeRef> neighbors(String entityId) {
        List<GraphEdgeRef> result = byEntity.get(entityId);
        return result == null ? Collections.emptyList() : Collections.unmodifiableList(result);
    }
}
