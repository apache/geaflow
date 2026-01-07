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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.geaflow.ai.graph.LocalMemoryGraphAccessor;
import org.apache.geaflow.ai.graph.MemoryMutableGraph;
import org.apache.geaflow.ai.graph.io.*;
import org.apache.geaflow.ai.index.EntityAttributeIndexStore;
import org.apache.geaflow.ai.index.vector.KeywordVector;
import org.apache.geaflow.ai.search.VectorSearch;
import org.apache.geaflow.ai.verbalization.Context;
import org.apache.geaflow.ai.verbalization.SubgraphSemanticPromptFunction;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MutableGraphTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(MutableGraphTest.class);

    @Test
    public void testMutableGraph() {
        GraphSchema graphSchema = new GraphSchema();
        VertexSchema vertexSchema = new VertexSchema("chunk", "id",
            Collections.singletonList("text"));
        EdgeSchema edgeSchema = new EdgeSchema("relation", "srcId", "dstId",
            Collections.singletonList("rel"));
        graphSchema.addVertex(vertexSchema);
        graphSchema.addEdge(edgeSchema);
        Map<String, EntityGroup > entities = new HashMap<>();
        entities.put(vertexSchema.getName(), new VertexGroup(vertexSchema, new ArrayList<>()));
        entities.put(edgeSchema.getName(), new EdgeGroup(edgeSchema, new ArrayList<>()));
        MemoryGraph graph = new MemoryGraph(graphSchema, entities);

        LocalMemoryGraphAccessor graphAccessor = new LocalMemoryGraphAccessor(graph);
        MemoryMutableGraph memoryMutableGraph = new MemoryMutableGraph(graph);
        LOGGER.info("Success to init empty graph.");

        EntityAttributeIndexStore indexStore = new EntityAttributeIndexStore();
        indexStore.initStore(new SubgraphSemanticPromptFunction(graphAccessor));
        LOGGER.info("Success to init EntityAttributeIndexStore.");

        GraphMemoryServer server = new GraphMemoryServer();
        server.addGraphAccessor(graphAccessor);
        server.addIndexStore(indexStore);
        LOGGER.info("Success to init GraphMemoryServer.");

        memoryMutableGraph.addVertex(new Vertex(vertexSchema.getName(),
            "apple", Collections.singletonList("apple is a kind of fruit.")));
        memoryMutableGraph.addVertex(new Vertex(vertexSchema.getName(),
            "banana", Collections.singletonList("banana is a kind of fruit.")));
        memoryMutableGraph.addVertex(new Vertex(vertexSchema.getName(),
            "grape", Collections.singletonList("grape is a kind of fruit.")));

        String query = "How about apple?";
        String result = searchInGraph(server, graphAccessor, query, 1);
        LOGGER.info("query: {} result: {}", query, result);
        Assertions.assertTrue(result.contains("apple"));

        memoryMutableGraph.addVertex(new Vertex(vertexSchema.getName(),
            "red", Collections.singletonList("red is a kind of color.")));
        memoryMutableGraph.addVertex(new Vertex(vertexSchema.getName(),
            "yellow", Collections.singletonList("yellow is a kind of color.")));
        memoryMutableGraph.addVertex(new Vertex(vertexSchema.getName(),
            "purple", Collections.singletonList("purple is a kind of color.")));
        memoryMutableGraph.addEdge(new Edge(edgeSchema.getName(),
            "apple", "red", Collections.singletonList("apple is red.")));
        memoryMutableGraph.addEdge(new Edge(edgeSchema.getName(),
            "banana", "yellow", Collections.singletonList("apple is yellow.")));
        memoryMutableGraph.addEdge(new Edge(edgeSchema.getName(),
            "grape", "purple", Collections.singletonList("apple is purple.")));

        query = "What color is apple?";
        result = searchInGraph(server, graphAccessor, query, 3);
        LOGGER.info("query: {} result: {}", query, result);
        Assertions.assertTrue(result.contains("apple is red"));

        memoryMutableGraph.updateVertex(new Vertex(vertexSchema.getName(),
            "red", Collections.singletonList("red is not a kind of fruit.")));

        query = "How about red?";
        result = searchInGraph(server, graphAccessor, query, 1);
        LOGGER.info("query: {} result: {}", query, result);
        Assertions.assertTrue(result.contains("red is not a kind of fruit."));

        memoryMutableGraph.removeVertex(vertexSchema.getName(), "yellow");
        query = "How about yellow?";
        result = searchInGraph(server, graphAccessor, query, 1);
        LOGGER.info("query: {} result: {}", query, result);
        Assertions.assertFalse(result.contains("yellow is a kind of color."));

        memoryMutableGraph.removeEdge(new Edge(edgeSchema.getName(),
            "apple", "red", Collections.singletonList("apple is red.")));

        query = "What color is apple?";
        result = searchInGraph(server, graphAccessor, query, 3);
        LOGGER.info("query: {} result: {}", query, result);
        Assertions.assertFalse(result.contains("apple is red."));
        Assertions.assertTrue(result.contains("apple is a kind of fruit."));
        Assertions.assertTrue(result.contains("red is not a kind of fruit."));

    }

    private String searchInGraph(GraphMemoryServer server,
                                 LocalMemoryGraphAccessor graphAccessor,
                                 String query, int times) {
        String sessionId = server.createSession();
        Context context = null;
        for (int i = 0; i < times; i++) {
            VectorSearch search = new VectorSearch(null, sessionId);
            search.addVector(new KeywordVector(query));
            String searchResult = server.search(search);
            Assertions.assertNotNull(searchResult);
            context = server.verbalize(sessionId,
                new SubgraphSemanticPromptFunction(graphAccessor));
        }
        assert context != null;
        return context.toString();
    }
}
