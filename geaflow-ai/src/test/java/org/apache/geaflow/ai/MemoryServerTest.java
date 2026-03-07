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

import com.google.gson.Gson;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import okhttp3.*;
import org.apache.geaflow.ai.common.config.Constants;
import org.apache.geaflow.ai.graph.io.*;
import org.junit.jupiter.api.*;
import org.noear.solon.test.SolonTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SolonTest(GeaFlowMemoryServer.class)
public class MemoryServerTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(MemoryServerTest.class);
    private static final String BASE_URL = "http://localhost:8080";
    private static final String GRAPH_NAME = "Confucius";
    private static OkHttpClient client;

    @BeforeEach
    void setUp() {
        LOGGER.info("Setting up test environment...");
        if (client == null) {
            OkHttpClient.Builder builder = new OkHttpClient.Builder();
            builder.callTimeout(Constants.HTTP_CALL_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            builder.connectTimeout(Constants.HTTP_CONNECT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            builder.readTimeout(Constants.HTTP_READ_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            builder.writeTimeout(Constants.HTTP_WRITE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            client = builder.build();
        }
        LOGGER.info("Test HTTP client initialized, base URL: {}", BASE_URL);
    }

    @AfterEach
    void tearDown() {
        LOGGER.info("Cleaning up test environment...");
    }

    private String get(String useApi) {
        String url = BASE_URL + useApi;
        Request request = new Request.Builder().url(url).get().build();
        try (okhttp3.Response response = client.newCall(request).execute()) {
            return response.body().string();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String post(String useApi, String bodyJson) {
        return post(useApi, bodyJson, Collections.emptyMap());
    }

    private String post(String useApi, String bodyJson, Map<String, String> queryParams) {
        RequestBody requestBody = RequestBody.create(
            MediaType.parse("application/json; charset=utf-8"), bodyJson);
        String url = BASE_URL + useApi;
        HttpUrl.Builder urlBuilder = HttpUrl.parse(url).newBuilder();
        if (queryParams != null) {
            for (Map.Entry<String, String> entry : queryParams.entrySet()) {
                urlBuilder.addQueryParameter(entry.getKey(), entry.getValue());
            }
        }
        Request request = new Request.Builder().url(urlBuilder.build()).post(requestBody).build();
        try (okhttp3.Response response = client.newCall(request).execute()) {
            return response.body().string();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testMain() throws Exception {
        testServerHealth();
        testCreateGraph();
        testAddEntities();
        testQueries();
    }

    void testServerHealth() throws Exception {
        LOGGER.info("Testing server health endpoint...");
        String api = "/";
        String response = get(api);
        LOGGER.info("API: {} Response: {}", api, response);
        api = "/health";
        response = get(api);
        LOGGER.info("API: {} Response: {}", api, response);
        api = "/api/test";
        response = get(api);
        LOGGER.info("API: {} Response: {}", api, response);
    }

    void testCreateGraph() throws Exception {
        LOGGER.info("Testing server create graph...");
        Gson gson = new Gson();
        String api = "/graph/create";
        GraphSchema testGraph = new GraphSchema();
        String graphName = GRAPH_NAME;
        testGraph.setName(graphName);
        String response = post(api, gson.toJson(testGraph));
        LOGGER.info("API: {} Response: {}", api, response);

        api = "/graph/getGraphSchema";
        Map<String, String> queryParams = new HashMap<>();
        queryParams.put("graphName", graphName);
        response = post(api, "", queryParams);
        LOGGER.info("API: {} Response: {}", api, response);
        Assertions.assertEquals(gson.toJson(testGraph), response);

        VertexSchema vertexSchema = new VertexSchema("chunk", "id",
            Collections.singletonList("text"));
        EdgeSchema edgeSchema = new EdgeSchema("relation", "srcId", "dstId",
            Collections.singletonList("rel"));
        testGraph.addVertex(vertexSchema);
        testGraph.addEdge(edgeSchema);

        api = "/graph/addEntitySchema";
        queryParams = new HashMap<>();
        queryParams.put("graphName", graphName);
        response = post(api, gson.toJson(vertexSchema), queryParams);
        LOGGER.info("API: {} Response: {}", api, response);

        api = "/graph/addEntitySchema";
        queryParams = new HashMap<>();
        queryParams.put("graphName", graphName);
        response = post(api, gson.toJson(edgeSchema), queryParams);
        LOGGER.info("API: {} Response: {}", api, response);

        api = "/graph/getGraphSchema";
        queryParams = new HashMap<>();
        queryParams.put("graphName", graphName);
        response = post(api, "", queryParams);
        LOGGER.info("API: {} Response: {}", api, response);
        Assertions.assertEquals(gson.toJson(testGraph), response);
    }

    void testAddEntities() throws Exception {
        LOGGER.info("Testing server add entities...");
        Gson gson = new Gson();
        String graphName = GRAPH_NAME;

        String api = "/graph/getGraphSchema";
        Map<String, String> queryParams = new HashMap<>();
        queryParams.put("graphName", graphName);
        String response = post(api, "", queryParams);
        LOGGER.info("API: {} Response: {}", api, response);

        TextFileReader textFileReader = new TextFileReader(10000);
        textFileReader.readFile("text/Confucius");
        List<String> chunks = IntStream.range(0, textFileReader.getRowCount())
            .mapToObj(textFileReader::getRow)
            .map(String::trim).collect(Collectors.toList());
        for(String chunk : chunks) {
            String vid = UUID.randomUUID().toString().replace("-", "");
            Vertex chunkVertex = new Vertex("chunk", vid, Collections.singletonList(chunk));
            api = "/graph/insertEntity";
            queryParams = new HashMap<>();
            queryParams.put("graphName", graphName);
            response = post(api, gson.toJson(chunkVertex), queryParams);
            LOGGER.info("API: {} Response: {}", api, response);
        }
    }

    void testQueries() throws Exception {
        LOGGER.info("Testing server queries...");
        String graphName = GRAPH_NAME;
        String sessionId = null;
        String api = "/query/context";
        Map<String, String> queryParams = new HashMap<>();
        queryParams.put("graphName", graphName);
        String response = post(api, "", queryParams);
        LOGGER.info("API: {} Response: {}", api, response);
        Assertions.assertNotNull(response);
        sessionId = response;

        api = "/query/exec";
        queryParams = new HashMap<>();
        queryParams.put("sessionId", sessionId);
        queryParams.put("query", "Who is Confucius?");
        response = post(api, "", queryParams);
        LOGGER.info("API: {} Response: {}", api, response);
        Assertions.assertNotNull(response);

        api = "/query/result";
        queryParams = new HashMap<>();
        queryParams.put("sessionId", sessionId);
        response = post(api, "", queryParams);
        LOGGER.info("API: {} Response: {}", api, response);
        Assertions.assertNotNull(response);

        api = "/query/exec";
        queryParams = new HashMap<>();
        queryParams.put("sessionId", sessionId);
        queryParams.put("query", "What did he say?");
        response = post(api, "", queryParams);
        LOGGER.info("API: {} Response: {}", api, response);
        Assertions.assertNotNull(response);

        api = "/query/result";
        queryParams = new HashMap<>();
        queryParams.put("sessionId", sessionId);
        response = post(api, "", queryParams);
        LOGGER.info("API: {} Response: {}", api, response);
        Assertions.assertNotNull(response);
    }

}
