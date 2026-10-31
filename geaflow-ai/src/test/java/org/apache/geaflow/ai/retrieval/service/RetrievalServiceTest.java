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

package org.apache.geaflow.ai.retrieval.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.geaflow.ai.GraphMemoryServer;
import org.apache.geaflow.ai.graph.LocalMemoryGraphAccessor;
import org.apache.geaflow.ai.graph.io.EntityGroup;
import org.apache.geaflow.ai.graph.io.GraphSchema;
import org.apache.geaflow.ai.graph.io.MemoryGraph;
import org.apache.geaflow.ai.graph.io.Vertex;
import org.apache.geaflow.ai.graph.io.VertexGroup;
import org.apache.geaflow.ai.graph.io.VertexSchema;
import org.apache.geaflow.ai.index.EntityAttributeIndexStore;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalBudget;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalErrorCode;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalException;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalRequest;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalResponse;
import org.apache.geaflow.ai.retrieval.config.RetrievalProperties;
import org.apache.geaflow.ai.service.ServerMemoryCache;
import org.apache.geaflow.ai.verbalization.SubgraphSemanticPromptFunction;
import org.junit.jupiter.api.Test;

class RetrievalServiceTest {

    @Test
    void retrievesScoredEvidenceWithoutCreatingSession() {
        ServerMemoryCache cache = new ServerMemoryCache();
        MemoryGraph graph = createGraph();
        GraphMemoryServer server = createServer(graph);
        cache.putGraph(graph);
        cache.putServer(server);

        RetrievalRequest request = new RetrievalRequest();
        request.setGraphName("week1-service-graph");
        request.setQuery("Confucius");
        request.setBudget(new RetrievalBudget(1, 3000, 10, 4096));

        RetrievalMetrics metrics = new RetrievalMetrics();
        RetrievalResponse response = new RetrievalService(cache, new RetrievalProperties(), metrics)
            .retrieve(request, "request-1");

        assertEquals("request-1", response.getRequestId());
        assertEquals("v1", response.getGraphVersion());
        assertEquals(1, response.getEvidence().size());
        assertTrue(response.getEvidence().get(0).getFinalScore() > 0);
        assertTrue(response.getEvidence().get(0).getStageScores().get("keyword").getRawScore() > 0);
        assertFalse(response.getEvidence().get(0).getStageScores().isEmpty());
        assertEquals(1, metrics.snapshot().getTotal());
        assertEquals(1, metrics.snapshot().getSuccess());
    }

    @Test
    void preservesUnboundedKeywordScoresWithoutInventingFusion() {
        MemoryGraph graph = createGraph();
        org.apache.geaflow.ai.graph.GraphVertex vertex = new LocalMemoryGraphAccessor(graph)
            .getVertex("chunk", "chunk-1");
        org.apache.geaflow.ai.retrieval.model.evidence.Evidence evidence = new EvidenceMapper()
            .map(Collections.singletonList(
                new org.apache.geaflow.ai.operator.GraphSearchStore.ScoredGraphEntity(vertex, 8.5f, 1)))
            .get(0);
        assertEquals(8.5, evidence.getStageScores().get("keyword").getRawScore());
        assertEquals(8.5, evidence.getFinalScore());
        org.junit.jupiter.api.Assertions.assertNull(evidence.getFusedScore());
    }

    @Test
    void distinguishesMissingGraph() {
        RetrievalRequest request = new RetrievalRequest();
        request.setGraphName("missing");
        request.setQuery("query");

        RetrievalException exception = org.junit.jupiter.api.Assertions.assertThrows(
            RetrievalException.class,
            () -> new RetrievalService(new ServerMemoryCache(), new RetrievalProperties())
                .retrieve(request, "request-2"));

        assertEquals(RetrievalErrorCode.GRAPH_NOT_FOUND, exception.getCode());
    }

    @Test
    void distinguishesLoadedGraphWithUnavailableIndex() {
        ServerMemoryCache cache = new ServerMemoryCache();
        cache.putGraph(createGraph());
        RetrievalRequest request = new RetrievalRequest();
        request.setGraphName("week1-service-graph");
        request.setQuery("query");

        RetrievalException exception = org.junit.jupiter.api.Assertions.assertThrows(
            RetrievalException.class,
            () -> new RetrievalService(cache, new RetrievalProperties())
                .retrieve(request, "request-not-ready"));

        assertEquals(RetrievalErrorCode.INDEX_NOT_READY, exception.getCode());
    }

    @Test
    void httpServiceRejectsCoreModesBeforeReadinessOrKeywordExecution() {
        RetrievalService service = new RetrievalService(new ServerMemoryCache(), new RetrievalProperties());
        for (String mode : new String[] {"BM25_ONLY", "VECTOR_ONLY", "GRAPH_ONLY", "HYBRID", "unknown"}) {
            RetrievalRequest request = new RetrievalRequest();
            request.setGraphName("week1-service-graph");
            request.setQuery("Confucius");
            request.setMode(mode);
            request.setQueryVector(java.util.Arrays.asList(1.0, 0.0));
            RetrievalException error = org.junit.jupiter.api.Assertions.assertThrows(RetrievalException.class,
                () -> service.retrieve(request));
            assertEquals(RetrievalErrorCode.UNSUPPORTED_OPTION, error.getCode());
        }
    }

    private static MemoryGraph createGraph() {
        VertexSchema schema = new VertexSchema("chunk", "id", Collections.singletonList("text"));
        GraphSchema graphSchema = new GraphSchema();
        graphSchema.setName("week1-service-graph");
        graphSchema.addVertex(schema);
        Vertex vertex = new Vertex("chunk", "chunk-1",
            Collections.singletonList("Confucius was a philosopher and teacher."));
        Map<String, EntityGroup> groups = new HashMap<>();
        groups.put("chunk", new VertexGroup(schema, new ArrayList<>(Collections.singletonList(vertex))));
        return new MemoryGraph(graphSchema, groups);
    }

    private static GraphMemoryServer createServer(MemoryGraph graph) {
        LocalMemoryGraphAccessor accessor = new LocalMemoryGraphAccessor(graph);
        EntityAttributeIndexStore indexStore = new EntityAttributeIndexStore();
        indexStore.initStore(new SubgraphSemanticPromptFunction(accessor));
        GraphMemoryServer server = new GraphMemoryServer();
        server.addGraphAccessor(accessor);
        server.addIndexStore(indexStore);
        return server;
    }
}
