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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.geaflow.ai.graph.io.Vertex;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalResponse;
import org.apache.geaflow.ai.retrieval.codec.RetrievalApiJson;
import org.apache.geaflow.ai.retrieval.config.RetrievalProperties;
import org.apache.geaflow.ai.retrieval.support.HttpTestClient;
import org.apache.geaflow.ai.retrieval.support.HttpTestResponse;
import org.apache.geaflow.ai.retrieval.support.RetrievalTestFixture;
import org.junit.jupiter.api.Test;
import org.noear.solon.annotation.Inject;
import org.noear.solon.test.SolonTest;

@SolonTest(GeaFlowMemoryServer.class)
public class MemoryServerTest {

    @Inject
    private RetrievalProperties properties;

    @Test
    void legacyWorkflow() {
        RetrievalTestFixture fixture = new RetrievalTestFixture();
        Gson gson = new Gson();
        Map<String, String> graph = Collections.singletonMap("graphName", RetrievalTestFixture.GRAPH_NAME);
        String port = System.getProperty("server.port",
            System.getenv().getOrDefault("GEAFLOW_SERVER_PORT", "8080"));
        try (HttpTestClient client = new HttpTestClient("http://localhost:" + port)) {
            HttpTestResponse health = client.get("/health");
            success(health);
            assertTrue(health.getContentType().startsWith("application/json"));
            JsonObject body = new JsonParser().parse(health.getBody()).getAsJsonObject();
            assertEquals("UP", body.get("status").getAsString());
            assertEquals("geaflow-memory-server", body.get("service").getAsString());

            HttpTestResponse created = client.post("/graph/create", gson.toJson(fixture.graphSchema()), Collections.emptyMap());
            success(created);
            assertTrue(created.getBody().contains(RetrievalTestFixture.GRAPH_NAME));
            HttpTestResponse schema = client.post("/graph/addEntitySchema", gson.toJson(fixture.vertexSchema()), graph);
            success(schema);
            assertTrue(schema.getBody().contains("chunk"));
            HttpTestResponse graphSchema = client.post("/graph/getGraphSchema", "", graph);
            success(graphSchema);
            assertTrue(graphSchema.getBody().contains("week1-keyword-graph"));
            for (Vertex vertex : fixture.vertices()) {
                HttpTestResponse inserted = client.post("/graph/insertEntity", gson.toJson(vertex), graph);
                success(inserted);
                assertEquals("Success to add entities, num: 1", inserted.getBody());
            }

            HttpTestResponse context = client.post("/query/context", "", graph);
            success(context);
            assertFalse(context.getBody().trim().isEmpty());
            Map<String, String> session = Collections.singletonMap("sessionId", context.getBody());
            HttpTestResponse query = client.post("/query/exec", "Confucius", session);
            success(query);
            assertTrue(query.getBody().contains("Confucius"), query.getBody());
            HttpTestResponse result = client.post("/query/result", "", session);
            success(result);
            assertTrue(result.getBody().contains("confucius-1"), result.getBody());
            assertTrue(result.getBody().contains("Confucius taught ethics."), result.getBody());

            HttpTestResponse ready = client.get("/ready");
            assertEquals(503, ready.getStatus(), ready.getBody());
            assertTrue(ready.getBody().contains("GRAPH_NOT_LOADED"), ready.getBody());

            String request = "{\"graphName\":\"" + RetrievalTestFixture.GRAPH_NAME
                + "\",\"graphVersion\":\"v4\",\"indexVersion\":\"v4\","
                + "\"query\":\"Confucius\",\"budget\":{\"topK\":1,"
                + "\"timeoutMs\":3000,\"maxCandidates\":10,\"tokenBudget\":4096}}";
            HttpTestResponse retrieval = client.postWithHeaders("/api/v1/retrievals", request,
                Collections.singletonMap("X-Request-Id", "request-1"));
            assertEquals(200, retrieval.getStatus(), retrieval.getBody());
            assertEquals("request-1", retrieval.getHeaders().get("X-Request-Id"));
            RetrievalResponse response = RetrievalApiJson.parseResponse(retrieval.getBody());
            assertEquals("request-1", response.getRequestId());
            assertEquals("v4", response.getGraphVersion());
            assertEquals(1, response.getEvidence().size());
            assertTrue(response.getEvidence().get(0).getFinalScore() > 0);
            assertTrue(response.getEvidence().get(0).getStageScores().containsKey("keyword"));
            HttpTestResponse retrievalMetrics = client.get("/metrics/retrieval");
            assertEquals(200, retrievalMetrics.getStatus(), retrievalMetrics.getBody());
            assertTrue(retrievalMetrics.getBody().contains("\"candidateCounts\":"),
                retrievalMetrics.getBody());
            assertTrue(retrievalMetrics.getBody().contains("\"stageElapsedNanos\":"),
                retrievalMetrics.getBody());
            assertTrue(retrievalMetrics.getBody().contains("\"edgesExamined\":"),
                retrievalMetrics.getBody());
            assertTrue(retrievalMetrics.getBody().contains("\"tokensRetained\":"),
                retrievalMetrics.getBody());
            assertTrue(retrievalMetrics.getBody().contains("\"degradationReasons\":"),
                retrievalMetrics.getBody());
            assertTrue(retrievalMetrics.getBody().contains("\"replanReasons\":"),
                retrievalMetrics.getBody());

            int previousMaxTopK = properties.getMaxTopK();
            try {
                properties.setMaxTopK(200);
                String largerRequest = request.replace("\"topK\":1", "\"topK\":120")
                    .replace("\"maxCandidates\":10", "\"maxCandidates\":120");
                HttpTestResponse larger = client.post("/api/v1/retrievals", largerRequest);
                assertEquals(200, larger.getStatus(), larger.getBody());
                assertEquals(120, RetrievalApiJson.parseResponse(larger.getBody(), properties)
                    .getEffectiveBudget().getTopK());
                properties.setApiToken("http-test-token");
                properties.setRemoteAccessEnabled(true);
                assertEquals(401, client.post("/api/v1/retrievals", request).getStatus());
                assertEquals(403, client.postWithHeaders("/api/v1/retrievals", request,
                    Collections.singletonMap("Authorization", "Bearer wrong")).getStatus());
                assertEquals(403, client.postWithHeaders("/api/v1/retrievals", request,
                    Collections.singletonMap("Authorization", "Basic http-test-token")).getStatus());
                HttpTestResponse authorized = client.postWithHeaders("/api/v1/retrievals", request,
                    Collections.singletonMap("Authorization", "Bearer http-test-token"));
                assertEquals(200, authorized.getStatus(), authorized.getBody());
                assertEquals(401, client.post("/api/v1/retrievals", "{").getStatus());
                assertEquals(401, client.post("/graph/getGraphSchema", "", graph).getStatus());
                assertEquals(403, client.postWithHeaders("/query/result", "",
                    Collections.singletonMap("Authorization", "Bearer wrong")).getStatus());
                assertEquals(401, client.get("/health").getStatus());
                assertEquals(200, client.getWithHeaders("/health",
                    Collections.singletonMap("Authorization", "Bearer http-test-token")).getStatus());
            } finally {
                properties.setMaxTopK(previousMaxTopK);
                properties.setRemoteAccessEnabled(false);
                properties.setApiToken(null);
            }

            for (String mode : new String[] {"BM25_ONLY", "VECTOR_ONLY", "GRAPH_ONLY", "HYBRID"}) {
                String unsupportedRequest = request.replace("\"query\":", "\"mode\":\"" + mode
                    + "\",\"queryVector\":[1,0],\"query\":");
                HttpTestResponse unsupported = client.post("/api/v1/retrievals", unsupportedRequest);
                boolean rejectsVector = "BM25_ONLY".equals(mode) || "GRAPH_ONLY".equals(mode);
                assertEquals(rejectsVector ? 400 : 503, unsupported.getStatus(), unsupported.getBody());
                assertTrue(unsupported.getBody().contains(rejectsVector
                    ? "UNSUPPORTED_OPTION" : "INDEX_NOT_READY"), unsupported.getBody());
            }

            HttpTestResponse noMatch = client.post("/api/v1/retrievals",
                request.replace("Confucius", "not-present"));
            assertEquals(200, noMatch.getStatus(), noMatch.getBody());
            assertEquals(0, RetrievalApiJson.parseResponse(noMatch.getBody()).getEvidence().size());

            HttpTestResponse missing = client.post("/api/v1/retrievals",
                request.replace(RetrievalTestFixture.GRAPH_NAME, "missing-graph"));
            assertEquals(404, missing.getStatus(), missing.getBody());
            assertTrue(missing.getBody().contains("GRAPH_NOT_FOUND"), missing.getBody());

            HttpTestResponse malformed = client.post("/api/v1/retrievals", "{");
            assertEquals(400, malformed.getStatus(), malformed.getBody());
            assertTrue(malformed.getBody().contains("Malformed JSON request"), malformed.getBody());

            HttpTestResponse metrics = client.get("/metrics/retrieval");
            assertEquals(200, metrics.getStatus(), metrics.getBody());
            assertTrue(metrics.getBody().contains("\"success\":"), metrics.getBody());
        }
    }

    @Test
    void fixtureIsDeterministicAndIndependent() {
        Gson gson = new Gson();
        RetrievalTestFixture first = new RetrievalTestFixture();
        RetrievalTestFixture second = new RetrievalTestFixture();
        assertEquals(gson.toJson(first.graphSchema()), gson.toJson(second.graphSchema()));
        assertEquals(gson.toJson(first.vertexSchema()), gson.toJson(second.vertexSchema()));
        List<Vertex> firstVertices = first.vertices();
        List<Vertex> secondVertices = second.vertices();
        assertEquals(gson.toJson(firstVertices), gson.toJson(secondVertices));
        assertEquals(RetrievalTestFixture.CONFUCIUS_ID, firstVertices.get(0).getId());
        assertEquals(RetrievalTestFixture.ASTRONOMY_ID, firstVertices.get(1).getId());
        assertNotSame(first.graphSchema(), second.graphSchema());
        assertNotSame(first.vertexSchema(), second.vertexSchema());
        assertNotSame(firstVertices, secondVertices);
        assertNotSame(firstVertices.get(0), secondVertices.get(0));
        first.graphSchema().setName("changed");
        assertEquals(RetrievalTestFixture.GRAPH_NAME, second.graphSchema().getName());
    }

    @Test
    void httpContractCoversEveryModeAndExecutionMode() {
        String base = "{\"graphName\":\"" + RetrievalTestFixture.GRAPH_NAME
            + "\",\"graphVersion\":\"v4\",\"indexVersion\":\"v4\","
            + "\"query\":\"Confucius\",\"budget\":{\"topK\":1,\"timeoutMs\":3000,"
            + "\"maxCandidates\":10,\"tokenBudget\":4096}}";
        Map<String, String> headers = Collections.singletonMap("X-Request-Id", "request-contract");
        String port = System.getProperty("server.port",
            System.getenv().getOrDefault("GEAFLOW_SERVER_PORT", "8080"));
        try (HttpTestClient client = new HttpTestClient("http://localhost:" + port)) {
            // KEYWORD is the legacy alias of BM25_ONLY and must stay callable over HTTP. The
            // graph may or may not be loaded depending on test order, so accept both the success
            // path and the typed not-ready/not-found path, but never an untyped 5xx.
            for (String mode : new String[] {"KEYWORD", "BM25_ONLY"}) {
                HttpTestResponse response = client.postWithHeaders("/api/v1/retrievals",
                    withField(base, "mode", mode), headers);
                assertEquals("request-contract", response.getHeaders().get("X-Request-Id"));
                assertNotEquals(500, response.getStatus(), response.getBody());
                if (response.getStatus() == 200) {
                    assertEquals(mode, RetrievalApiJson.parseResponse(response.getBody())
                        .getTrace().getSelectedMode().name());
                } else {
                    assertTrue(response.getBody().contains("INDEX_NOT_READY")
                        || response.getBody().contains("GRAPH_NOT_FOUND"), response.getBody());
                }
            }
            // GRAPH/VECTOR/HYBRID need indexes the keyword-only fixture does not have. They must
            // fail with a typed client/index code and never surface a 5xx.
            for (String mode : new String[] {"GRAPH_ONLY", "VECTOR_ONLY", "HYBRID"}) {
                String body = withField(base, "mode", mode)
                    .replace("\"graphVersion\":", "\"queryVector\":[1,0],\"graphVersion\":");
                HttpTestResponse response = client.postWithHeaders("/api/v1/retrievals", body, headers);
                assertEquals("request-contract", response.getHeaders().get("X-Request-Id"));
                assertTrue(response.getStatus() == 400 || response.getStatus() == 404
                    || response.getStatus() == 503,
                    mode + " -> " + response.getStatus() + " " + response.getBody());
                assertTrue(response.getBody().contains("UNSUPPORTED_OPTION")
                    || response.getBody().contains("INDEX_NOT_READY")
                    || response.getBody().contains("GRAPH_NOT_FOUND"), response.getBody());
            }
            // The execution-mode field must be accepted by the HTTP contract for every value.
            for (String execution : new String[] {"SEQUENTIAL", "PARALLEL", "CASCADED"}) {
                String body = withField(withField(base, "mode", "KEYWORD"),
                    "executionMode", execution);
                HttpTestResponse response = client.postWithHeaders("/api/v1/retrievals", body, headers);
                assertNotEquals(500, response.getStatus(), response.getBody());
                if (response.getStatus() == 200) {
                    assertEquals(execution, RetrievalApiJson.parseResponse(response.getBody())
                        .getTrace().getExecutionMode().name());
                }
            }
        }
    }

    @Test
    void readyResponseIsAlwaysValidJsonForSpecialCharacterGraphNames() {
        String quoted = GeaFlowMemoryServer.readyBody(false, "graph-\"quoted\"", "v\\1",
            "GRAPH_NOT_LOADED");
        assertValidReadyJson(quoted, "graph-\"quoted\"", "v\\1");
        String unicode = GeaFlowMemoryServer.readyBody(true, "图\u0001", null, "READY");
        assertValidReadyJson(unicode, "图\u0001", null);
    }

    private static void assertValidReadyJson(String body, String graphName, String graphVersion) {
        JsonObject parsed = new JsonParser().parse(body).getAsJsonObject();
        assertEquals(graphName, parsed.get("graphName").getAsString());
        if (graphVersion == null) {
            assertTrue(parsed.get("graphVersion").isJsonNull(), body);
        } else {
            assertEquals(graphVersion, parsed.get("graphVersion").getAsString());
        }
        assertTrue(parsed.has("status"), body);
        assertTrue(parsed.has("reason"), body);
    }

    private static String withField(String request, String field, String value) {
        return request.replace("\"query\":", "\"" + field + "\":\"" + value + "\",\"query\":");
    }

    private static void success(HttpTestResponse response) {
        assertEquals(200, response.getStatus(), response.getBody());
        assertNotNull(response.getHeaders().get("Content-Type"));
        assertNotNull(response.getBody());
    }
}
