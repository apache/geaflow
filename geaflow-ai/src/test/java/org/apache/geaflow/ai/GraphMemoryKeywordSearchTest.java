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

package org.apache.geaflow.ai;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.geaflow.ai.graph.LocalMemoryGraphAccessor;
import org.apache.geaflow.ai.graph.io.EntityGroup;
import org.apache.geaflow.ai.graph.io.GraphSchema;
import org.apache.geaflow.ai.graph.io.MemoryGraph;
import org.apache.geaflow.ai.graph.io.Vertex;
import org.apache.geaflow.ai.graph.io.VertexGroup;
import org.apache.geaflow.ai.graph.io.VertexSchema;
import org.apache.geaflow.ai.index.EntityAttributeIndexStore;
import org.apache.geaflow.ai.operator.GraphSearchStore.ScoredGraphEntity;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalErrorCode;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalException;
import org.apache.geaflow.ai.verbalization.SubgraphSemanticPromptFunction;
import org.junit.jupiter.api.Test;

class GraphMemoryKeywordSearchTest {

    @Test
    void maxCandidatesStopsScanningBeforeTailMatch() {
        GraphMemoryServer server = createServer();

        List<ScoredGraphEntity> hits = server.searchKeyword("stars", 10, 1, Long.MAX_VALUE);

        assertEquals(0, hits.size());
    }

    @Test
    void expiredDeadlineIsReportedAsTimeout() {
        GraphMemoryServer server = createServer();

        RetrievalException exception = assertThrows(RetrievalException.class,
            () -> server.searchKeyword("confucius", 1, 10, System.nanoTime() - 1));

        assertEquals(RetrievalErrorCode.RETRIEVAL_TIMEOUT, exception.getCode());
    }

    private static GraphMemoryServer createServer() {
        VertexSchema vertexSchema = new VertexSchema("chunk", "id",
            Collections.singletonList("text"));
        GraphSchema schema = new GraphSchema();
        schema.setName("keyword-search-test");
        schema.addVertex(vertexSchema);
        List<Vertex> vertices = Arrays.asList(
            new Vertex("chunk", "confucius-1", Collections.singletonList("Confucius taught ethics.")),
            new Vertex("chunk", "astronomy-1", Collections.singletonList("Astronomy studies stars.")));
        Map<String, EntityGroup> groups = new LinkedHashMap<>();
        groups.put("chunk", new VertexGroup(vertexSchema, new ArrayList<>(vertices)));
        MemoryGraph graph = new MemoryGraph(schema, groups);
        LocalMemoryGraphAccessor accessor = new LocalMemoryGraphAccessor(graph);
        EntityAttributeIndexStore indexStore = new EntityAttributeIndexStore();
        indexStore.initStore(new SubgraphSemanticPromptFunction(accessor));
        GraphMemoryServer server = new GraphMemoryServer();
        server.addGraphAccessor(accessor);
        server.addIndexStore(indexStore);
        return server;
    }
}
