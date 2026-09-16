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

package org.apache.geaflow.ai.operator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.geaflow.ai.graph.GraphEntity;
import org.apache.geaflow.ai.graph.GraphVertex;
import org.apache.geaflow.ai.graph.LocalMemoryGraphAccessor;
import org.apache.geaflow.ai.index.IndexStore;
import org.apache.geaflow.ai.index.vector.EmbeddingVector;
import org.apache.geaflow.ai.index.vector.IVector;
import org.apache.geaflow.ai.graph.io.EntityGroup;
import org.apache.geaflow.ai.graph.io.GraphSchema;
import org.apache.geaflow.ai.graph.io.MemoryGraph;
import org.apache.geaflow.ai.graph.io.Vertex;
import org.apache.geaflow.ai.graph.io.VertexGroup;
import org.apache.geaflow.ai.graph.io.VertexSchema;
import org.apache.geaflow.ai.search.VectorSearch;
import org.apache.geaflow.ai.subgraph.SubGraph;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * {@link EmbeddingOperator} may collect its global candidate set either by scanning the graph or by
 * asking the index store what it holds. Both must recall exactly the same entities.
 */
public class EmbeddingCandidateSetTest {

    private static final String LABEL = "doc";
    private static final int VERTEX_NUM = 500;
    private static final int DIM = 8;

    /** Index store backed by a fixed map, optionally able to enumerate its own content. */
    private static class MapIndexStore implements IndexStore {

        private final Map<GraphEntity, List<IVector>> data;
        private final boolean enumerable;

        MapIndexStore(Map<GraphEntity, List<IVector>> data, boolean enumerable) {
            this.data = data;
            this.enumerable = enumerable;
        }

        @Override
        public List<IVector> getEntityIndex(GraphEntity entity) {
            List<IVector> vectors = data.get(entity);
            return vectors == null ? Collections.emptyList() : vectors;
        }

        @Override
        public Collection<GraphEntity> getIndexedEntities() {
            return enumerable ? data.keySet() : null;
        }
    }

    private LocalMemoryGraphAccessor buildGraph() {
        GraphSchema schema = new GraphSchema();
        VertexSchema vertexSchema = new VertexSchema(LABEL, "id", Collections.singletonList("text"));
        schema.setName("embedding_graph");
        schema.addVertex(vertexSchema);
        List<Vertex> vertices = new ArrayList<>(VERTEX_NUM);
        for (int i = 0; i < VERTEX_NUM; i++) {
            vertices.add(new Vertex(LABEL, "id" + i, Collections.singletonList("text" + i)));
        }
        Map<String, EntityGroup> entities = new HashMap<>();
        entities.put(LABEL, new VertexGroup(vertexSchema, vertices));
        return new LocalMemoryGraphAccessor(new MemoryGraph(schema, entities));
    }

    /**
     * Builds embeddings for a deterministic subset of vertices, so the test also covers vertices
     * that carry no embedding at all and vertices absent from the store entirely.
     */
    private Map<GraphEntity, List<IVector>> buildIndexData(LocalMemoryGraphAccessor accessor) {
        Map<GraphEntity, List<IVector>> data = new LinkedHashMap<>();
        for (int i = 0; i < VERTEX_NUM; i++) {
            if (i % 3 == 2) {
                // Not indexed at all.
                continue;
            }
            GraphVertex vertex = accessor.getVertex(LABEL, "id" + i);
            Assertions.assertNotNull(vertex);
            if (i % 3 == 1) {
                // Present but with no vector, must be skipped by recall.
                data.put(vertex, Collections.emptyList());
                continue;
            }
            double[] vec = new double[DIM];
            for (int d = 0; d < DIM; d++) {
                vec[d] = Math.sin(i + d) + 1.5;
            }
            data.put(vertex, Collections.singletonList((IVector) new EmbeddingVector(vec)));
        }
        return data;
    }

    private static List<String> recall(LocalMemoryGraphAccessor accessor, IndexStore store,
                                       double[] query) {
        VectorSearch search = new VectorSearch(null, "session");
        search.addVector(new EmbeddingVector(query));
        List<SubGraph> result = new EmbeddingOperator(accessor, store).apply(null, search);
        List<String> ids = new ArrayList<>();
        for (SubGraph subGraph : result) {
            for (GraphEntity entity : subGraph.getGraphEntityList()) {
                ids.add(((GraphVertex) entity).getVertex().getId());
            }
        }
        return ids;
    }

    @Test
    public void testEnumeratedCandidatesMatchGraphScan() {
        LocalMemoryGraphAccessor accessor = buildGraph();
        Map<GraphEntity, List<IVector>> data = buildIndexData(accessor);
        IndexStore scanStore = new MapIndexStore(data, false);
        IndexStore enumerableStore = new MapIndexStore(data, true);

        for (int q = 0; q < 5; q++) {
            double[] query = new double[DIM];
            for (int d = 0; d < DIM; d++) {
                query[d] = Math.cos(q + d) + 1.5;
            }
            List<String> byScan = recall(accessor, scanStore, query);
            List<String> byEnumeration = recall(accessor, enumerableStore, query);
            Assertions.assertFalse(byScan.isEmpty(), "query " + q + " recalled nothing");
            Assertions.assertEquals(byScan, byEnumeration,
                "enumerating the index store must recall the same entities in the same order");
        }
    }
}
