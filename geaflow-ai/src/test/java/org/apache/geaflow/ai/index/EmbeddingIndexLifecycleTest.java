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

package org.apache.geaflow.ai.index;

import com.google.gson.Gson;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.geaflow.ai.common.model.EmbeddingResponse;
import org.apache.geaflow.ai.common.model.ModelConfig;
import org.apache.geaflow.ai.graph.GraphEntity;
import org.apache.geaflow.ai.graph.LocalMemoryGraphAccessor;
import org.apache.geaflow.ai.graph.io.Edge;
import org.apache.geaflow.ai.graph.io.EdgeGroup;
import org.apache.geaflow.ai.graph.io.EdgeSchema;
import org.apache.geaflow.ai.graph.io.EntityGroup;
import org.apache.geaflow.ai.graph.io.GraphSchema;
import org.apache.geaflow.ai.graph.io.MemoryGraph;
import org.apache.geaflow.ai.graph.io.Vertex;
import org.apache.geaflow.ai.graph.io.VertexGroup;
import org.apache.geaflow.ai.graph.io.VertexSchema;
import org.apache.geaflow.ai.verbalization.SubgraphSemanticPromptFunction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The index over successive runs, against a local embeddings endpoint so the whole path is
 * exercised without a model service: request and response serialisation, the on disk index file,
 * and what a later run decides to embed.
 *
 * <p>Chiefly this pins down what happens to an entity that yields no embeddable text. Such an
 * entity is registered with an empty vector list, which is also what makes it skippable within a
 * run, since {@code scanEdge} returns an edge from both of its endpoints. Nothing is written to the
 * index file for it, and {@code initStore} rebuilds its map from that file alone, so a later run
 * treats it as unseen and embeds it once its value carries meaning. That last point is the one worth
 * a test: the alternative, never revisiting it, would make the omission permanent.
 */
public class EmbeddingIndexLifecycleTest {

    private static final String LABEL = "chunk";
    private static final String EDGE_LABEL = "rel";
    private static final String MEANINGFUL = "learning without thought is labour lost";
    private static final String IGNORABLE = "2024-01-01";
    private static final String EDGE_TEXT = "the master teaches the disciple";
    private static final int DIMS = 4;

    private HttpServer server;
    private final List<String> requestBodies = new CopyOnWriteArrayList<>();

    @BeforeEach
    void startEndpoint() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/embeddings", this::handle);
        server.setExecutor(null);
        server.start();
    }

    @AfterEach
    void stopEndpoint() {
        if (server != null) {
            server.stop(0);
        }
    }

    private void handle(HttpExchange exchange) throws IOException {
        byte[] requestBytes = readAll(exchange);
        String body = new String(requestBytes, StandardCharsets.UTF_8);
        requestBodies.add(body);

        String[] inputs = new Gson().fromJson(body, Request.class).input;
        EmbeddingResponse response = new EmbeddingResponse();
        response.object = "list";
        response.model = "test-local";
        response.data = new ArrayList<>();
        for (int i = 0; i < inputs.length; i++) {
            EmbeddingResponse.EmbeddingVector vector = new EmbeddingResponse.EmbeddingVector();
            vector.index = i;
            vector.embedding = deterministicVector(inputs[i]);
            response.data.add(vector);
        }

        byte[] out = new Gson().toJson(response).getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json; charset=utf-8");
        exchange.sendResponseHeaders(200, out.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(out);
        }
    }

    private static byte[] readAll(HttpExchange exchange) throws IOException {
        java.io.ByteArrayOutputStream buffer = new java.io.ByteArrayOutputStream();
        byte[] chunk = new byte[4096];
        int read;
        while ((read = exchange.getRequestBody().read(chunk)) > 0) {
            buffer.write(chunk, 0, read);
        }
        return buffer.toByteArray();
    }

    /** Stable and non-zero, so a stored vector can be told apart from an absent one. */
    private static double[] deterministicVector(String text) {
        double[] vector = new double[DIMS];
        for (int i = 0; i < DIMS; i++) {
            vector[i] = ((text.hashCode() >> i) & 0xFF) / 255.0 + 0.1;
        }
        return vector;
    }

    private ModelConfig config() {
        return new ModelConfig("test-local",
            "http://127.0.0.1:" + server.getAddress().getPort(), "/v1/embeddings", "test-token");
    }

    @Test
    public void testIndexLifecycleAcrossRuns(@TempDir Path tempDir) throws Exception {
        String indexPath = tempDir.resolve("index.jsonl").toString();

        // First run. One vertex and the edge carry meaning, the other vertex is only a date.
        LocalMemoryGraphAccessor first = buildGraph(MEANINGFUL, IGNORABLE);
        EmbeddingIndexStore store = new EmbeddingIndexStore();
        store.initStore(first, new SubgraphSemanticPromptFunction(first), indexPath, config());

        Assertions.assertEquals(1, requestBodies.size(),
            "the two values that carry meaning belong to one batch, so one request");
        Assertions.assertFalse(store.getEntityIndex(first.getVertex(LABEL, "v1")).isEmpty(),
            "a value carrying meaning must hold a vector");
        Assertions.assertTrue(store.getEntityIndex(first.getVertex(LABEL, "v2")).isEmpty(),
            "a value that is only a date has nothing to embed");
        Assertions.assertEquals(2, indexLines(indexPath),
            "only the vertex and the edge that carry meaning are persisted");

        // Second run over an unchanged graph. Everything with vectors comes back from the file, so
        // the model is not consulted again.
        requestBodies.clear();
        LocalMemoryGraphAccessor second = buildGraph(MEANINGFUL, IGNORABLE);
        EmbeddingIndexStore reloaded = new EmbeddingIndexStore();
        reloaded.initStore(second, new SubgraphSemanticPromptFunction(second), indexPath, config());

        Assertions.assertEquals(0, requestBodies.size(),
            "an unchanged graph must not be re-embedded");
        Assertions.assertFalse(reloaded.getEntityIndex(second.getVertex(LABEL, "v1")).isEmpty(),
            "vectors must survive a rebuild from the index file");
        Assertions.assertEquals(2, indexLines(indexPath), "no duplicate records appended");

        // Third run, with the date replaced by text that carries meaning. Nothing was persisted for
        // that entity, so this run must embed it, and must leave the rest alone.
        requestBodies.clear();
        String nowMeaningful = "the master replied in the temple";
        LocalMemoryGraphAccessor third = buildGraph(MEANINGFUL, nowMeaningful);
        EmbeddingIndexStore afterChange = new EmbeddingIndexStore();
        afterChange.initStore(third, new SubgraphSemanticPromptFunction(third), indexPath, config());

        Assertions.assertEquals(1, requestBodies.size(),
            "exactly the entity that gained meaning is embedded");
        Assertions.assertTrue(requestBodies.get(0).contains(nowMeaningful),
            "the request must carry the newly meaningful text");
        Assertions.assertFalse(requestBodies.get(0).contains(MEANINGFUL),
            "an entity already in the index file must not be embedded again");
        Assertions.assertFalse(afterChange.getEntityIndex(third.getVertex(LABEL, "v2")).isEmpty(),
            "an entity that yielded nothing before must not be excluded for good");
        Assertions.assertEquals(3, indexLines(indexPath), "the new record is appended");
    }

    private long indexLines(String indexPath) throws IOException {
        return Files.readAllLines(java.nio.file.Paths.get(indexPath), StandardCharsets.UTF_8)
            .stream().filter(line -> !line.trim().isEmpty()).count();
    }

    private LocalMemoryGraphAccessor buildGraph(String v1Text, String v2Text) {
        GraphSchema schema = new GraphSchema();
        schema.setName("lifecycle");
        VertexSchema vs = new VertexSchema(LABEL, "id", Collections.singletonList("text"));
        EdgeSchema es = new EdgeSchema(EDGE_LABEL, "srcId", "dstId",
            Collections.singletonList("rel"));
        schema.addVertex(vs);
        schema.addEdge(es);

        List<Vertex> vertices = new ArrayList<>(Arrays.asList(
            new Vertex(LABEL, "v1", Collections.singletonList(v1Text)),
            new Vertex(LABEL, "v2", Collections.singletonList(v2Text))));
        List<Edge> edges = new ArrayList<>(Collections.singletonList(
            new Edge(EDGE_LABEL, "v1", "v2", Collections.singletonList(EDGE_TEXT))));
        Map<String, EntityGroup> entities = new HashMap<>();
        entities.put(LABEL, new VertexGroup(vs, vertices));
        entities.put(EDGE_LABEL, new EdgeGroup(es, edges));
        return new LocalMemoryGraphAccessor(new MemoryGraph(schema, entities));
    }

    /** Mirrors the request the client sends, enough of it to read the inputs back. */
    private static class Request {
        private String[] input;
    }
}
