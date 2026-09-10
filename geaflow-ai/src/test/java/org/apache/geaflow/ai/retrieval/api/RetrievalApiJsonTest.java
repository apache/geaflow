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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.geaflow.ai.retrieval.api;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.geaflow.ai.retrieval.api.model.ExecutionMode;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalBudget;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalError;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalErrorCode;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalRequest;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalResponse;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalTrace;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalMode;
import org.apache.geaflow.ai.retrieval.api.model.TraceStage;
import org.apache.geaflow.ai.retrieval.codec.RetrievalApiJson;
import org.apache.geaflow.ai.retrieval.model.evidence.Evidence;
import org.apache.geaflow.ai.retrieval.model.evidence.EvidenceKind;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** JSON contract tests independent of Solon and HTTP. */
public class RetrievalApiJsonTest {

    @Test
    public void parsesMinimalAndFullRequests() {
        RetrievalRequest minimal = RetrievalApiJson.parseRequest(
            "{\"graphName\":\"graph\",\"query\":\"confucius\"}");
        Assertions.assertEquals("graph", minimal.getGraphName());
        Assertions.assertNull(minimal.getBudget());

        RetrievalRequest full = RetrievalApiJson.parseRequest(
            "{\"graphName\":\"graph\",\"query\":\"confucius\","
                + "\"queryVector\":[],\"mode\":\"KEYWORD\","
                + "\"executionMode\":\"SEQUENTIAL\","
                + "\"budget\":{\"topK\":10,\"timeoutMs\":3000,"
                + "\"maxCandidates\":100,\"tokenBudget\":4096},"
                + "\"futureField\":true}");
        Assertions.assertEquals("KEYWORD", full.getMode());
        Assertions.assertEquals(Integer.valueOf(100), full.getBudget().getMaxCandidates());
        Assertions.assertEquals(Collections.emptyList(), full.getQueryVector());
    }

    @Test
    public void checkedInExamplesAreValidContractDocuments() throws IOException {
        RetrievalRequest request = RetrievalApiJson.parseRequest(read("retrieval/api/request-full.json"));
        RetrievalResponse response = RetrievalApiJson.parseResponse(
            read("retrieval/api/response-empty.json"));
        RetrievalError error = RetrievalApiJson.parseError(read("retrieval/api/error-index-not-ready.json"));

        Assertions.assertEquals("week1-keyword-graph", request.getGraphName());
        Assertions.assertTrue(response.getEvidence().isEmpty());
        Assertions.assertEquals(RetrievalErrorCode.INDEX_NOT_READY, error.getCode());
    }

    @Test
    public void rejectsWrongTypesMalformedJsonAndDuplicateFields() {
        Assertions.assertThrows(RuntimeException.class,
            () -> RetrievalApiJson.parseRequest("{\"graphName\":1,\"query\":\"x\"}"));
        Assertions.assertThrows(RuntimeException.class,
            () -> RetrievalApiJson.parseRequest("{\"graphName\":\"g\",\"query\":\"x\","
                + "\"budget\":{\"topK\":1.5}}"));
        Assertions.assertThrows(RuntimeException.class,
            () -> RetrievalApiJson.parseRequest("{\"graphName\":\"g\",\"query\":\"x\","
                + "\"budget\":1}"));
        Assertions.assertThrows(RuntimeException.class,
            () -> RetrievalApiJson.parseRequest("{\"graphName\":\"g\",\"query\":\"x\","
                + "\"queryVector\":[null]}"));
        Assertions.assertThrows(RuntimeException.class,
            () -> RetrievalApiJson.parseRequest("{\"graphName\":\"g\",\"query\":\"x\"} trailing"));
        Assertions.assertThrows(RuntimeException.class,
            () -> RetrievalApiJson.parseRequest("{\"graphName\":\"g\",\"graphName\":\"x\","
                + "\"query\":\"q\"}"));
        StringBuilder oversized = new StringBuilder("{\"graphName\":\"g\",\"query\":\"");
        for (int i = 0; i < 1024 * 1024; i++) {
            oversized.append('x');
        }
        oversized.append("\"}");
        Assertions.assertThrows(RuntimeException.class,
            () -> RetrievalApiJson.parseRequest(oversized.toString()));
        StringBuilder unicodeOversized = new StringBuilder("{\"graphName\":\"g\",\"query\":\"");
        for (int i = 0; i < 350 * 1024; i++) {
            unicodeOversized.append('\u4e2d');
        }
        unicodeOversized.append("\"}");
        Assertions.assertThrows(RuntimeException.class,
            () -> RetrievalApiJson.parseRequest(unicodeOversized.toString()));
    }

    @Test
    public void responseKeepsRequiredCollectionsAndRoundTrips() {
        RetrievalResponse response = new RetrievalResponse();
        response.setRequestId("req-1");
        response.setGraphName("graph");
        response.setGraphVersion("v1");
        response.setEvidence(Collections.singletonList(new Evidence("e-1", EvidenceKind.CHUNK,
            "text", null, null, null, null, null, null, 1)));
        RetrievalTrace trace = new RetrievalTrace();
        trace.setTraceVersion("v1");
        trace.setOriginalQuery("confucius");
        trace.setSelectedMode(RetrievalMode.KEYWORD);
        trace.setExecutionMode(ExecutionMode.SEQUENTIAL);
        trace.setStages(Collections.singletonList(new TraceStage("keyword", "COMPLETED", null)));
        trace.setStopReason("COMPLETED");
        response.setTrace(trace);
        response.setEffectiveBudget(new RetrievalBudget(10, 3000, 100, 4096));

        String json = RetrievalApiJson.toJson(response);
        RetrievalResponse restored = RetrievalApiJson.parseResponse(json);

        Assertions.assertEquals("req-1", restored.getRequestId());
        Assertions.assertEquals(1, restored.getEvidence().size());
        Assertions.assertNotNull(restored.getPaths());
        Assertions.assertNotNull(restored.getSources());
        Assertions.assertNotNull(restored.getDegradedChannels());
        Assertions.assertEquals("v1", restored.getTrace().getTraceVersion());
        Assertions.assertEquals(Integer.valueOf(4096),
            restored.getEffectiveBudget().getTokenBudget());
    }

    @Test
    public void emptyResponseUsesArraysInsteadOfNullCollections() {
        String json = "{\"requestId\":\"req-2\",\"graphName\":\"graph\","
            + "\"graphVersion\":\"v1\",\"evidence\":[],\"paths\":[],"
            + "\"sources\":[],\"degradedChannels\":[],"
            + "\"trace\":{\"traceVersion\":\"v1\","
            + "\"originalQuery\":\"none\",\"selectedMode\":\"KEYWORD\","
            + "\"executionMode\":\"SEQUENTIAL\",\"stages\":[]},"
            + "\"effectiveBudget\":{\"topK\":10,\"timeoutMs\":3000,"
            + "\"maxCandidates\":100,\"tokenBudget\":4096}}";
        RetrievalResponse response = RetrievalApiJson.parseResponse(json);

        Assertions.assertNotNull(response.getEvidence());
        Assertions.assertNotNull(response.getPaths());
        Assertions.assertNotNull(response.getSources());
        Assertions.assertNotNull(response.getDegradedChannels());
        Assertions.assertTrue(response.getEvidence().isEmpty());
    }

    @Test
    public void rejectsResponseMissingRequiredSections() {
        Assertions.assertThrows(RuntimeException.class, () -> RetrievalApiJson.parseResponse(
            "{\"requestId\":\"r\",\"graphName\":\"g\",\"graphVersion\":\"v1\"}"));
    }

    @Test
    public void rejectsResponseTraceWithoutStages() {
        String json = "{\"requestId\":\"r\",\"graphName\":\"g\","
            + "\"graphVersion\":\"v1\",\"evidence\":[],\"paths\":[],"
            + "\"sources\":[],\"degradedChannels\":[],"
            + "\"trace\":{\"traceVersion\":\"v1\","
            + "\"originalQuery\":\"q\",\"selectedMode\":\"KEYWORD\","
            + "\"executionMode\":\"SEQUENTIAL\"},"
            + "\"effectiveBudget\":{\"topK\":1,\"timeoutMs\":1,"
            + "\"maxCandidates\":1,\"tokenBudget\":1}}";
        Assertions.assertThrows(RuntimeException.class, () -> RetrievalApiJson.parseResponse(json));
    }

    @Test
    public void rejectsUnsupportedTraceModesAndUnboundedEffectiveBudget() {
        String parallel = responseJson("PARALLEL", "SEQUENTIAL", 10, 3000, 100, 4096);
        Assertions.assertThrows(RuntimeException.class,
            () -> RetrievalApiJson.parseResponse(parallel));

        String overLimit = responseJson("KEYWORD", "SEQUENTIAL", 101, 3000, 100, 4096);
        Assertions.assertThrows(RuntimeException.class,
            () -> RetrievalApiJson.parseResponse(overLimit));
    }

    @Test
    public void rejectsBlankRequiredResponseAndErrorFields() {
        String blankGraph = responseJson("KEYWORD", "SEQUENTIAL", 10, 3000, 100, 4096)
            .replace("\"graphName\":\"graph\"", "\"graphName\":\" \"");
        Assertions.assertThrows(RuntimeException.class,
            () -> RetrievalApiJson.parseResponse(blankGraph));
        Assertions.assertThrows(RuntimeException.class, () -> RetrievalApiJson.parseError(
            "{\"requestId\":\" \",\"code\":\"INTERNAL_ERROR\","
                + "\"message\":\"x\",\"retriable\":false}"));
    }

    @Test
    public void rejectsIncompleteResponseSerialization() {
        RetrievalResponse response = new RetrievalResponse();
        Assertions.assertThrows(RuntimeException.class, () -> RetrievalApiJson.toJson(response));
    }

    @Test
    public void errorCodeAndRetriableFlagAreStable() {
        RetrievalError error = new RetrievalError("req-3", RetrievalErrorCode.INDEX_NOT_READY,
            "not ready");
        RetrievalError restored = RetrievalApiJson.parseError(RetrievalApiJson.toJson(error));

        Assertions.assertEquals(RetrievalErrorCode.INDEX_NOT_READY, restored.getCode());
        Assertions.assertTrue(restored.isRetriable());
        Assertions.assertEquals(503, restored.getCode().getHttpStatus());
        Assertions.assertThrows(RuntimeException.class, () -> RetrievalApiJson.parseError(
            "{\"requestId\":\"r\",\"code\":\"INDEX_NOT_READY\","
                + "\"message\":\"x\",\"retriable\":false}"));
    }

    @Test
    public void documentsEveryErrorCodeMapping() {
        Map<RetrievalErrorCode, Integer> statuses = new HashMap<>();
        statuses.put(RetrievalErrorCode.INVALID_REQUEST, 400);
        statuses.put(RetrievalErrorCode.UNSUPPORTED_OPTION, 400);
        statuses.put(RetrievalErrorCode.GRAPH_NOT_FOUND, 404);
        statuses.put(RetrievalErrorCode.INDEX_NOT_READY, 503);
        statuses.put(RetrievalErrorCode.RETRIEVAL_TIMEOUT, 504);
        statuses.put(RetrievalErrorCode.INTERNAL_ERROR, 500);

        Assertions.assertEquals(RetrievalErrorCode.values().length, statuses.size());
        for (Map.Entry<RetrievalErrorCode, Integer> entry : statuses.entrySet()) {
            RetrievalErrorCode code = entry.getKey();
            Assertions.assertEquals(entry.getValue().intValue(), code.getHttpStatus());
            Assertions.assertEquals(code == RetrievalErrorCode.INDEX_NOT_READY
                || code == RetrievalErrorCode.RETRIEVAL_TIMEOUT, code.isRetriable());
            RetrievalError parsed = RetrievalApiJson.parseError(RetrievalApiJson.toJson(
                new RetrievalError("request", code, "message")));
            Assertions.assertEquals(code, parsed.getCode());
        }
    }

    @Test
    public void outputUsesDocumentedCamelCaseNames() {
        RetrievalRequest request = new RetrievalRequest();
        request.setGraphName("graph");
        request.setQuery("q");
        request.setBudget(new RetrievalBudget(1, 2, 3, 4));
        JsonObject json = new JsonParser().parse(RetrievalApiJson.toJson(request)).getAsJsonObject();

        Assertions.assertTrue(json.has("graphName"));
        Assertions.assertTrue(json.has("query"));
        Assertions.assertTrue(json.has("budget"));
        Assertions.assertTrue(json.getAsJsonObject("budget").has("maxCandidates"));
        Assertions.assertFalse(json.has("graph_name"));
    }

    private static String read(String resource) throws IOException {
        InputStream stream = RetrievalApiJsonTest.class.getClassLoader()
            .getResourceAsStream(resource);
        Assertions.assertNotNull(stream);
        try (Reader reader = new InputStreamReader(stream, StandardCharsets.UTF_8)) {
            StringBuilder result = new StringBuilder();
            char[] buffer = new char[1024];
            int count;
            while ((count = reader.read(buffer)) >= 0) {
                result.append(buffer, 0, count);
            }
            return result.toString();
        }
    }

    private static String responseJson(String mode, String executionMode, int topK,
                                       int timeoutMs, int maxCandidates, int tokenBudget) {
        return "{\"requestId\":\"r\",\"graphName\":\"graph\","
            + "\"graphVersion\":\"v1\",\"evidence\":[],\"paths\":[],"
            + "\"sources\":[],\"degradedChannels\":[],\"trace\":{"
            + "\"traceVersion\":\"v1\",\"originalQuery\":\"q\","
            + "\"selectedMode\":\"" + mode + "\",\"executionMode\":\""
            + executionMode + "\",\"stages\":[]},\"effectiveBudget\":{"
            + "\"topK\":" + topK + ",\"timeoutMs\":" + timeoutMs
            + ",\"maxCandidates\":" + maxCandidates + ",\"tokenBudget\":"
            + tokenBudget + "}}";
    }
}
