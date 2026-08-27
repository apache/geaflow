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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.geaflow.ai.GraphMemoryServer;
import org.apache.geaflow.ai.common.model.EmbeddingResponse;
import org.apache.geaflow.ai.common.model.EmbeddingService;
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
import org.apache.geaflow.ai.index.vector.EmbeddingVector;
import org.apache.geaflow.ai.index.vector.IVector;
import org.apache.geaflow.ai.search.VectorSearch;
import org.apache.geaflow.ai.verbalization.SubgraphSemanticPromptFunction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * What happens to a stored vector when the value it was produced from changes.
 *
 * <p>A record is keyed by the entity key, which is the id and the label, so it survives a change of
 * value. Left at that, a changed entity counts as indexed and keeps the vector of a value it no
 * longer has: retrieval for the old value still reaches it, while its current value has no vector
 * anywhere. These tests pin down that a record is accepted only when it matches the text the entity
 * would be embedded from now.
 *
 * <p>The embeddings endpoint is local and answers with one hot vectors, one dimension per distinct
 * text, so that a stored vector says plainly which text produced it: cosine 1 against its own text
 * and 0 against any other. Values carry a digit because that is what the verbaliser keeps.
 */
public class EmbeddingIndexInvalidationTest {

    private static final String LABEL = "chunk";
    private static final String EDGE_LABEL = "rel";
    private static final String OLD_VALUE = "the bell rang 3 times in the old hall";
    private static final String NEW_VALUE = "the drum sounded 7 times in the new hall";
    private static final String OTHER_VALUE = "the gate stood open for 5 days";
    private static final String EDGE_VALUE = "the 2 halls face one another";
    private static final int DIMS = 32;

    private HttpServer server;
    private final List<String> requestBodies = new CopyOnWriteArrayList<>();
    private final Map<String, Integer> textDimensions = new LinkedHashMap<>();
    /** Set only by the test that needs to read while a build is in flight. */
    private volatile CountDownLatch arrivedAtEndpoint;
    private volatile CountDownLatch holdEndpoint;

    @BeforeEach
    void startEndpoint() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/embeddings", this::handle);
        // A thread of its own, so a handler that is held open does not hold up the test.
        server.setExecutor(Executors.newCachedThreadPool());
        server.start();
    }

    @AfterEach
    void stopEndpoint() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    public void testChangedValueIsEmbeddedAgainAndSupersedesTheStaleVector(@TempDir Path tempDir)
            throws Exception {
        String indexPath = tempDir.resolve("index.jsonl").toString();

        LocalMemoryGraphAccessor first = graphOf(OLD_VALUE, OTHER_VALUE);
        initStore(first, indexPath);
        Assertions.assertEquals(1, requestBodies.size(), "both values fit in one batch");
        Assertions.assertEquals(2, indexLines(indexPath), "one record per vertex");

        // Same id and label, different value. Nothing about the key changes, so this is the case
        // that used to go unnoticed.
        requestBodies.clear();
        LocalMemoryGraphAccessor second = graphOf(NEW_VALUE, OTHER_VALUE);
        EmbeddingIndexStore store = initStore(second, indexPath);

        Assertions.assertEquals(1, requestBodies.size(), "the changed vertex is embedded again");
        Assertions.assertTrue(requestBodies.get(0).contains(NEW_VALUE),
                "the request must carry the value the entity has now");
        Assertions.assertFalse(requestBodies.get(0).contains(OTHER_VALUE),
                "the unchanged vertex must come back from the file, not from the model");

        IVector stored = onlyVector(store, second, "v1");
        Assertions.assertEquals(1.0, stored.match(vectorOf(NEW_VALUE)), 1e-9,
                "the vector held for the vertex must be the one produced from its current value");
        Assertions.assertEquals(0.0, stored.match(vectorOf(OLD_VALUE)), 1e-9,
                "the vector produced from the value that is gone must not be what is held");

        // The record just written must in turn be recognised as current, or every run would embed
        // everything again, and append to a file it had nothing to add to.
        requestBodies.clear();
        byte[] before = Files.readAllBytes(Paths.get(indexPath));
        LocalMemoryGraphAccessor third = graphOf(NEW_VALUE, OTHER_VALUE);
        initStore(third, indexPath);
        Assertions.assertEquals(0, requestBodies.size(), "an unchanged graph is not embedded again");
        Assertions.assertArrayEquals(before, Files.readAllBytes(Paths.get(indexPath)),
                "and its file is left exactly as it was");
    }

    @Test
    public void testRecallFollowsTheCurrentValue(@TempDir Path tempDir) {
        String indexPath = tempDir.resolve("index.jsonl").toString();

        initStore(graphOf(OLD_VALUE, OTHER_VALUE), indexPath);
        LocalMemoryGraphAccessor changed = graphOf(NEW_VALUE, OTHER_VALUE);
        EmbeddingIndexStore store = initStore(changed, indexPath);

        // Asking for the value the graph no longer holds must reach nothing. Before, it reached the
        // vertex, and the context handed back described the new value, so the reason a vertex was
        // recalled and what the reader was then shown disagreed.
        Recall miss = recall(changed, store, OLD_VALUE);
        Assertions.assertTrue(miss.entities.isEmpty(),
                "a value that is no longer in the graph must not recall the vertex, context was: "
                        + miss.context);

        Recall hit = recall(changed, store, NEW_VALUE);
        Assertions.assertEquals(1, hit.entities.size(),
                "the value the vertex holds now must reach it");
        Assertions.assertTrue(hit.context.contains(NEW_VALUE),
                "and the context must be about that value, context was: " + hit.context);
    }

    @Test
    public void testChangedEdgeValueIsEmbeddedAgain(@TempDir Path tempDir) throws Exception {
        String indexPath = tempDir.resolve("index.jsonl").toString();

        initStore(graphWithEdge(OLD_VALUE, OTHER_VALUE, EDGE_VALUE), indexPath);
        Assertions.assertEquals(3, indexLines(indexPath), "two vertices and the edge between them");

        requestBodies.clear();
        String changedEdge = "the gate was locked at 8 in the evening";
        LocalMemoryGraphAccessor changed = graphWithEdge(OLD_VALUE, OTHER_VALUE, changedEdge);
        EmbeddingIndexStore store = initStore(changed, indexPath);

        Assertions.assertEquals(1, requestBodies.size(), "the edge is embedded again");
        Assertions.assertTrue(requestBodies.get(0).contains(changedEdge),
                "with the value it carries now");
        List<IVector> edgeVectors =
                store.getEntityIndex(changed.getEdge(EDGE_LABEL, "v1", "v2").get(0));
        Assertions.assertEquals(1, edgeVectors.size(),
                "one chunk, so one vector, and not the superseded one beside it");
        Assertions.assertEquals(1.0, edgeVectors.get(0).match(vectorOf(changedEdge)), 1e-9,
                "and it is the vector of the current value");
    }

    @Test
    public void testIdTakenOverByAnotherEntityDoesNotAdoptTheOldVector(@TempDir Path tempDir)
            throws Exception {
        String indexPath = tempDir.resolve("index.jsonl").toString();

        initStore(graphOf(OLD_VALUE), indexPath);
        Assertions.assertEquals(1, indexLines(indexPath), "one record for the one vertex");

        // The vertex is removed. Its record is not read, and not removed either, since from here a
        // deleted entity and a graph that scanned short look the same.
        requestBodies.clear();
        initStore(graphOf(), indexPath);
        Assertions.assertEquals(1, indexLines(indexPath), "the record is still in the file");
        Assertions.assertEquals(0, requestBodies.size(), "with nothing in the graph to embed");

        // Another entity takes the id over. This is what the record left in the file is dangerous
        // for, and the fingerprint is what stops it being adopted.
        requestBodies.clear();
        LocalMemoryGraphAccessor reused = graphOf(NEW_VALUE);
        EmbeddingIndexStore store = initStore(reused, indexPath);

        Assertions.assertEquals(1, requestBodies.size(), "the entity behind the id is embedded");
        IVector stored = onlyVector(store, reused, "v1");
        Assertions.assertEquals(1.0, stored.match(vectorOf(NEW_VALUE)), 1e-9,
                "and holds the vector of its own value");
        Assertions.assertEquals(0.0, stored.match(vectorOf(OLD_VALUE)), 1e-9,
                "not the one left behind by whatever held the id before");
    }

    @Test
    public void testValueThatComesBackIsNotEmbeddedAgain(@TempDir Path tempDir) throws Exception {
        String indexPath = tempDir.resolve("index.jsonl").toString();

        initStore(graphOf(OLD_VALUE), indexPath);
        initStore(graphOf(NEW_VALUE), indexPath);
        Assertions.assertEquals(2, indexLines(indexPath),
                "the record of the first value is still in the file, rejected but present");

        // Which is what keeps the file from growing with every change: a value that comes back is
        // matched by the record already there.
        requestBodies.clear();
        LocalMemoryGraphAccessor reverted = graphOf(OLD_VALUE);
        EmbeddingIndexStore store = initStore(reverted, indexPath);

        Assertions.assertEquals(0, requestBodies.size(),
                "a value the entity has held before needs no request");
        IVector stored = onlyVector(store, reverted, "v1");
        Assertions.assertEquals(1.0, stored.match(vectorOf(OLD_VALUE)), 1e-9,
                "and the vector held is the one of the value it has now");
        Assertions.assertEquals(2, indexLines(indexPath), "with nothing appended");
    }

    @Test
    public void testRecordWithoutFingerprintIsEmbeddedAgain(@TempDir Path tempDir) throws Exception {
        String indexPath = tempDir.resolve("index.jsonl").toString();

        // How the file looked before a record said which text it came from. The vector belongs to
        // some other text, which is precisely what cannot be established from the record itself.
        writeLines(indexPath, Collections.singletonList(
                new Gson().toJson(new EmbeddingService.EmbeddingResult(
                        "V" + "v1" + LABEL, vectorFor("something else entirely 9")))));

        LocalMemoryGraphAccessor accessor = graphOf(OLD_VALUE);
        EmbeddingIndexStore store = initStore(accessor, indexPath);

        Assertions.assertEquals(1, requestBodies.size(),
                "a record that cannot be shown to match the value is not taken on trust");
        Assertions.assertTrue(requestBodies.get(0).contains(OLD_VALUE), "the current value is sent");
        Assertions.assertEquals(1.0, onlyVector(store, accessor, "v1").match(vectorOf(OLD_VALUE)),
                1e-9, "the vector held is the one produced from the current value");

        List<EmbeddingService.EmbeddingResult> records = readRecords(indexPath);
        Assertions.assertEquals(2, records.size(), "the record it replaces is still in the file");
        Assertions.assertNotNull(records.get(1).contentHash,
                "and the one appended carries a fingerprint");

        // Which is to say the one without a fingerprint costs one round of embedding, once.
        requestBodies.clear();
        initStore(graphOf(OLD_VALUE), indexPath);
        Assertions.assertEquals(0, requestBodies.size(), "and is not paid for twice");
    }

    @Test
    public void testIndexIsFoundThroughAnyInstanceOfTheEntity(@TempDir Path tempDir) {
        String indexPath = tempDir.resolve("index.jsonl").toString();

        EmbeddingIndexStore fresh = new EmbeddingIndexStore();
        Assertions.assertTrue(fresh.getEntityIndex(
                        graphOf(OLD_VALUE).getVertex(LABEL, "v1")).isEmpty(),
                "a store that has not been built yet holds nothing, rather than failing");

        LocalMemoryGraphAccessor indexed = graphOf(OLD_VALUE);
        EmbeddingIndexStore store = initStore(indexed, indexPath);

        // A caller elsewhere asks with its own graph, so its own object for the same entity. What
        // the index is keyed by has to be the entity's key, not the object.
        LocalMemoryGraphAccessor elsewhere = graphOf(OLD_VALUE);
        GraphEntity fromElsewhere = elsewhere.getVertex(LABEL, "v1");
        Assertions.assertNotSame(indexed.getVertex(LABEL, "v1"), fromElsewhere,
                "the two graphs really do hand out different objects");
        Assertions.assertEquals(1.0, onlyVector(store, elsewhere, "v1").match(vectorOf(OLD_VALUE)),
                1e-9, "and the index is found through either of them");
    }

    @Test
    public void testReaderDuringABuildSeesTheIndexAsItWas(@TempDir Path tempDir) throws Exception {
        String indexPath = tempDir.resolve("index.jsonl").toString();

        LocalMemoryGraphAccessor first = graphOf(OLD_VALUE);
        EmbeddingIndexStore store = initStore(first, indexPath);
        Assertions.assertEquals(1.0, onlyVector(store, first, "v1").match(vectorOf(OLD_VALUE)),
                1e-9, "the first build leaves the vector of the value it was given");

        // Build again on the same store, with the endpoint held open, and read while it is half way
        // through. The index is assigned once at the end, so a reader is never shown a part of it.
        CountDownLatch reached = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        arrivedAtEndpoint = reached;
        holdEndpoint = release;
        LocalMemoryGraphAccessor changed = graphOf(NEW_VALUE);
        Thread build = new Thread(() -> store.initStore(changed,
                new SubgraphSemanticPromptFunction(changed), indexPath, config()));
        build.start();
        try {
            Assertions.assertTrue(reached.await(10, TimeUnit.SECONDS),
                    "the build should have reached the endpoint");
            Assertions.assertEquals(1.0,
                    onlyVector(store, first, "v1").match(vectorOf(OLD_VALUE)), 1e-9,
                    "a reader during the build still sees the index that was there before");
        } finally {
            release.countDown();
            build.join(20_000);
        }

        Assertions.assertEquals(1.0, onlyVector(store, changed, "v1").match(vectorOf(NEW_VALUE)),
                1e-9, "and sees the new one once the build is done");
    }

    private EmbeddingIndexStore initStore(LocalMemoryGraphAccessor accessor, String indexPath) {
        EmbeddingIndexStore store = new EmbeddingIndexStore();
        store.initStore(accessor, new SubgraphSemanticPromptFunction(accessor), indexPath, config());
        return store;
    }

    /** What asking for one text reaches, in a session of its own, and how it reads. */
    private Recall recall(LocalMemoryGraphAccessor accessor, EmbeddingIndexStore store,
                          String queryText) {
        GraphMemoryServer memoryServer = new GraphMemoryServer();
        memoryServer.addGraphAccessor(accessor);
        memoryServer.addIndexStore(store);
        String sessionId = memoryServer.createSession();
        VectorSearch search = new VectorSearch(null, sessionId);
        search.addVector(vectorOf(queryText));
        memoryServer.search(search);
        return new Recall(memoryServer.getSessionEntities(sessionId),
                memoryServer.verbalize(sessionId,
                        new SubgraphSemanticPromptFunction(accessor)).toString());
    }

    private static class Recall {
        private final List<GraphEntity> entities;
        private final String context;

        Recall(List<GraphEntity> entities, String context) {
            this.entities = entities;
            this.context = context;
        }
    }

    private IVector onlyVector(EmbeddingIndexStore store, LocalMemoryGraphAccessor accessor,
                               String id) {
        List<IVector> vectors = store.getEntityIndex(accessor.getVertex(LABEL, id));
        Assertions.assertEquals(1, vectors.size(), "one chunk, so one vector, for vertex " + id);
        return vectors.get(0);
    }

    private void handle(HttpExchange exchange) throws IOException {
        byte[] requestBytes = readAll(exchange);
        requestBodies.add(new String(requestBytes, StandardCharsets.UTF_8));
        if (arrivedAtEndpoint != null) {
            arrivedAtEndpoint.countDown();
            try {
                if (!holdEndpoint.await(20, TimeUnit.SECONDS)) {
                    throw new IOException("the test did not release the endpoint");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(e);
            }
        }

        String[] inputs = new Gson().fromJson(
                new String(requestBytes, StandardCharsets.UTF_8), Request.class).input;
        EmbeddingResponse response = new EmbeddingResponse();
        response.object = "list";
        response.model = "test-local";
        response.data = new ArrayList<>();
        for (int i = 0; i < inputs.length; i++) {
            EmbeddingResponse.EmbeddingVector vector = new EmbeddingResponse.EmbeddingVector();
            vector.index = i;
            vector.embedding = vectorFor(inputs[i]);
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
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        byte[] chunk = new byte[4096];
        int read;
        while ((read = exchange.getRequestBody().read(chunk)) > 0) {
            buffer.write(chunk, 0, read);
        }
        return buffer.toByteArray();
    }

    /** One dimension per distinct text: a vector matches its own text and nothing else. */
    private double[] vectorFor(String text) {
        int dimension;
        synchronized (textDimensions) {
            Integer known = textDimensions.get(text);
            if (known == null) {
                known = textDimensions.size();
                Assertions.assertTrue(known < DIMS, "more distinct texts than dimensions");
                textDimensions.put(text, known);
            }
            dimension = known;
        }
        double[] vector = new double[DIMS];
        vector[dimension] = 1.0;
        return vector;
    }

    private EmbeddingVector vectorOf(String text) {
        return new EmbeddingVector(vectorFor(text));
    }

    private ModelConfig config() {
        return new ModelConfig("test-local",
                "http://127.0.0.1:" + server.getAddress().getPort(), "/v1/embeddings", "test-token");
    }

    private long indexLines(String indexPath) throws IOException {
        return Files.readAllLines(Paths.get(indexPath), StandardCharsets.UTF_8)
                .stream().filter(line -> !line.trim().isEmpty()).count();
    }

    private List<EmbeddingService.EmbeddingResult> readRecords(String indexPath) throws IOException {
        List<EmbeddingService.EmbeddingResult> records = new ArrayList<>();
        Gson gson = new Gson();
        for (String line : Files.readAllLines(Paths.get(indexPath), StandardCharsets.UTF_8)) {
            if (!line.trim().isEmpty()) {
                records.add(gson.fromJson(line, EmbeddingService.EmbeddingResult.class));
            }
        }
        return records;
    }

    private void writeLines(String indexPath, List<String> lines) throws IOException {
        Files.write(Paths.get(indexPath), lines, StandardCharsets.UTF_8);
    }

    private LocalMemoryGraphAccessor graphOf(String... values) {
        return build(null, values);
    }

    /** The same vertices with one edge between the first two, carrying a value of its own. */
    private LocalMemoryGraphAccessor graphWithEdge(String v1Text, String v2Text, String edgeText) {
        return build(edgeText, v1Text, v2Text);
    }

    private LocalMemoryGraphAccessor build(String edgeText, String... values) {
        GraphSchema schema = new GraphSchema();
        schema.setName("invalidation");
        VertexSchema vertexSchema =
                new VertexSchema(LABEL, "id", Collections.singletonList("text"));
        schema.addVertex(vertexSchema);

        List<Vertex> vertices = new ArrayList<>();
        for (int i = 0; i < values.length; i++) {
            vertices.add(new Vertex(LABEL, "v" + (i + 1),
                    new ArrayList<>(Arrays.asList(values[i]))));
        }
        Map<String, EntityGroup> entities = new HashMap<>();
        entities.put(LABEL, new VertexGroup(vertexSchema, vertices));

        if (edgeText != null) {
            EdgeSchema edgeSchema = new EdgeSchema(EDGE_LABEL, "srcId", "dstId",
                    Collections.singletonList("text"));
            schema.addEdge(edgeSchema);
            List<Edge> edges = new ArrayList<>(Collections.singletonList(
                    new Edge(EDGE_LABEL, "v1", "v2", new ArrayList<>(
                            Collections.singletonList(edgeText)))));
            entities.put(EDGE_LABEL, new EdgeGroup(edgeSchema, edges));
        }
        return new LocalMemoryGraphAccessor(new MemoryGraph(schema, entities));
    }

    /** Mirrors the request the client sends, enough of it to read the inputs back. */
    private static class Request {
        private String[] input;
    }
}
