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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.geaflow.ai.common.util.SeDeUtil;
import org.apache.geaflow.ai.graph.*;
import org.apache.geaflow.ai.graph.io.*;
import org.apache.geaflow.ai.index.EntityAttributeIndexStore;
import org.apache.geaflow.ai.index.vector.KeywordVector;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalError;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalException;
import org.apache.geaflow.ai.retrieval.codec.RetrievalApiJson;
import org.apache.geaflow.ai.retrieval.config.RetrievalProperties;
import org.apache.geaflow.ai.retrieval.service.RetrievalMetrics;
import org.apache.geaflow.ai.retrieval.service.RetrievalService;
import org.apache.geaflow.ai.search.VectorSearch;
import org.apache.geaflow.ai.service.ServerMemoryCache;
import org.apache.geaflow.ai.verbalization.Context;
import org.apache.geaflow.ai.verbalization.SubgraphSemanticPromptFunction;
import org.noear.solon.Solon;
import org.noear.solon.annotation.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Controller
public class GeaFlowMemoryServer {

    private static final Logger LOGGER = LoggerFactory.getLogger(GeaFlowMemoryServer.class);

    private static final String SERVER_NAME = "geaflow-memory-server";
    private static final int DEFAULT_PORT = 8080;
    private static final Pattern SAFE_REQUEST_ID = Pattern.compile("[A-Za-z0-9._:-]{1,128}");

    private static final ServerMemoryCache FALLBACK_CACHE = new ServerMemoryCache();
    private static final RetrievalMetrics FALLBACK_METRICS = new RetrievalMetrics();

    @Inject
    private ServerMemoryCache cache;

    @Inject
    private RetrievalMetrics metrics;

    @Inject
    private RetrievalProperties retrievalProperties;

    @Inject
    private RetrievalService retrievalService;

    private static final RetrievalService FALLBACK_SERVICE = new RetrievalService(
        FALLBACK_CACHE, new RetrievalProperties(), FALLBACK_METRICS);

    public static void main(String[] args) {
        System.setProperty("solon.app.name", SERVER_NAME);
        int port = configuredPort();
        System.setProperty("server.port", String.valueOf(port));
        Solon.start(GeaFlowMemoryServer.class, args, app -> {
            app.cfg().loadAdd("application.yml");
            app.cfg().put("server.port", port);
            LOGGER.info("Starting {} on port {}", SERVER_NAME, port);
            app.get("/", ctx -> {
                ctx.output("GeaFlow AI Server is running...");
            });
        });
    }

    private static int configuredPort() {
        String environmentPort = System.getenv("GEAFLOW_SERVER_PORT");
        if (environmentPort == null || environmentPort.trim().isEmpty()) {
            return DEFAULT_PORT;
        }
        try {
            return Integer.parseInt(environmentPort.trim());
        } catch (NumberFormatException ignored) {
            LOGGER.warn("Ignoring invalid GEAFLOW_SERVER_PORT: {}", environmentPort);
            return DEFAULT_PORT;
        }
    }

    @Get
    @Mapping("/api/test")
    public String test() {
        return "GeaFlow Memory Server is working!";
    }

    @Post
    @Mapping("/api/v1/retrievals")
    public String retrieve(org.noear.solon.core.handle.Context ctx, @Body String input) {
        String requestId = resolveRequestId(ctx.header("X-Request-Id"));
        long startedAt = System.nanoTime();
        ctx.headerSet("X-Request-Id", requestId);
        ctx.contentType("application/json; charset=utf-8");
        try {
            String body = RetrievalApiJson.toJson(
                runtimeRetrievalService().retrieve(RetrievalApiJson.parseRequest(input), requestId));
            ctx.status(200);
            return body;
        } catch (RetrievalException exception) {
            ctx.status(exception.getCode().getHttpStatus());
            RetrievalError error = new RetrievalError(requestId, exception.getCode(),
                exception.getMessage());
            return RetrievalApiJson.toJson(error);
        } catch (com.google.gson.JsonParseException exception) {
            ctx.status(400);
            runtimeMetrics().recordFailure(
                org.apache.geaflow.ai.retrieval.api.model.RetrievalErrorCode.INVALID_REQUEST,
                elapsedMs(startedAt));
            RetrievalError error = new RetrievalError(requestId,
                org.apache.geaflow.ai.retrieval.api.model.RetrievalErrorCode.INVALID_REQUEST,
                "Malformed JSON request");
            return RetrievalApiJson.toJson(error);
        } catch (RuntimeException exception) {
            ctx.status(500);
            runtimeMetrics().recordFailure(
                org.apache.geaflow.ai.retrieval.api.model.RetrievalErrorCode.INTERNAL_ERROR,
                elapsedMs(startedAt));
            RetrievalError error = new RetrievalError(requestId,
                org.apache.geaflow.ai.retrieval.api.model.RetrievalErrorCode.INTERNAL_ERROR,
                "internal retrieval error");
            return RetrievalApiJson.toJson(error);
        }
    }

    static String resolveRequestId(String requestedId) {
        if (requestedId == null) {
            return java.util.UUID.randomUUID().toString();
        }
        String trimmed = requestedId.trim();
        if (!SAFE_REQUEST_ID.matcher(trimmed).matches()) {
            return java.util.UUID.randomUUID().toString();
        }
        return trimmed;
    }

    private static long elapsedMs(long startedAt) {
        return (System.nanoTime() - startedAt) / 1_000_000L;
    }

    @Get
    @Mapping("/health")
    public String health(org.noear.solon.core.handle.Context ctx) {
        ctx.status(200);
        ctx.contentType("application/json; charset=utf-8");
        return "{\"status\":\"UP\",\"service\":\"" + SERVER_NAME + "\"}";
    }

    @Get
    @Mapping("/ready")
    public String ready(org.noear.solon.core.handle.Context ctx) {
        RetrievalProperties properties = properties();
        ServerMemoryCache.ReadinessStatus status;
        try {
            properties.validateConfiguration();
            status = runtimeCache().keywordReadiness(properties.getReadyGraphName());
        } catch (RetrievalException exception) {
            status = new ServerMemoryCache.ReadinessStatus(false, null, null,
                "INVALID_CONFIGURATION");
        }
        ctx.status(status.isReady() ? 200 : 503);
        ctx.contentType("application/json; charset=utf-8");
        return "{\"status\":\"" + (status.isReady() ? "READY" : "NOT_READY")
            + "\",\"graphName\":\"" + String.valueOf(status.getGraphName())
            + "\",\"graphVersion\":\"" + String.valueOf(status.getGraphVersion())
            + "\",\"reason\":\"" + status.getReason() + "\"}";
    }

    @Get
    @Mapping("/metrics/retrieval")
    public String metrics(org.noear.solon.core.handle.Context ctx) {
        RetrievalMetrics.Snapshot snapshot = runtimeMetrics().snapshot();
        ctx.status(200);
        ctx.contentType("application/json; charset=utf-8");
        return "{\"total\":" + snapshot.getTotal() + ",\"success\":"
            + snapshot.getSuccess() + ",\"timeout\":" + snapshot.getTimeout()
            + ",\"failure\":" + snapshot.getFailure() + ",\"totalElapsedMs\":"
            + snapshot.getTotalElapsedMs() + "}";
    }

    private RetrievalProperties properties() {
        return retrievalProperties == null ? new RetrievalProperties() : retrievalProperties;
    }

    private ServerMemoryCache runtimeCache() {
        return cache == null ? FALLBACK_CACHE : cache;
    }

    private RetrievalMetrics runtimeMetrics() {
        return metrics == null ? FALLBACK_METRICS : metrics;
    }

    private RetrievalService runtimeRetrievalService() {
        return retrievalService == null ? FALLBACK_SERVICE : retrievalService;
    }

    @Post
    @Mapping("/graph/create")
    public String createGraph(@Body String input) {
        GraphSchema graphSchema = SeDeUtil.deserializeGraphSchema(input);
        String graphName = graphSchema.getName();
        if (graphName == null || runtimeCache().getGraphByName(graphName) != null) {
            throw new RuntimeException("Cannot create graph name: " + graphName);
        }
        Map<String, EntityGroup> entities = new HashMap<>();
        for (VertexSchema vertexSchema : graphSchema.getVertexSchemaList()) {
            entities.put(vertexSchema.getName(), new VertexGroup(vertexSchema, new ArrayList<>()));
        }
        for (EdgeSchema edgeSchema : graphSchema.getEdgeSchemaList()) {
            entities.put(edgeSchema.getName(), new EdgeGroup(edgeSchema, new ArrayList<>()));
        }
        MemoryGraph graph = new MemoryGraph(graphSchema, entities);
        LocalMemoryGraphAccessor graphAccessor = new LocalMemoryGraphAccessor(graph);
        LOGGER.info("Success to init empty graph.");

        EntityAttributeIndexStore indexStore = new EntityAttributeIndexStore();
        indexStore.initStore(new SubgraphSemanticPromptFunction(graphAccessor));
        LOGGER.info("Success to init EntityAttributeIndexStore.");

        GraphMemoryServer server = new GraphMemoryServer();
        server.addGraphAccessor(graphAccessor);
        server.addIndexStore(indexStore);
        LOGGER.info("Success to init GraphMemoryServer.");
        runtimeCache().putGraph(graph);
        runtimeCache().putServer(server);

        LOGGER.info("Success to init graph. SCHEMA: {}", graphSchema);
        return "createGraph has been called, graphName: " + graphName;
    }

    @Post
    @Mapping("/graph/addEntitySchema")
    public String addSchema(@Param("graphName") String graphName,
                            @Body String input) {
        Graph graph = runtimeCache().getGraphByName(graphName);
        if (graph == null) {
            throw new RuntimeException("Graph not exist.");
        }
        if (!(graph instanceof MemoryGraph)) {
            throw new RuntimeException("Graph cannot modify.");
        }
        MemoryMutableGraph memoryMutableGraph = new MemoryMutableGraph((MemoryGraph) graph);
        Schema schema = SeDeUtil.deserializeEntitySchema(input);
        String schemaName = schema.getName();
        if (schema instanceof VertexSchema) {
            memoryMutableGraph.addVertexSchema((VertexSchema) schema);
        } else if (schema instanceof EdgeSchema) {
            memoryMutableGraph.addEdgeSchema((EdgeSchema) schema);
        } else {
            throw new RuntimeException("Cannot add schema: " + input);
        }
        runtimeCache().markGraphUpdated(graphName);
        return "addSchema has been called, schemaName: " + schemaName;
    }

    @Post
    @Mapping("/graph/getGraphSchema")
    public String getSchema(@Param("graphName") String graphName) {
        Graph graph = runtimeCache().getGraphByName(graphName);
        if (graph == null) {
            throw new RuntimeException("Graph not exist.");
        }
        if (!(graph instanceof MemoryGraph)) {
            throw new RuntimeException("Graph cannot modify.");
        }
        return SeDeUtil.serializeGraphSchema(graph.getGraphSchema());
    }

    @Post
    @Mapping("/graph/insertEntity")
    public String addEntity(@Param("graphName") String graphName,
                            @Body String input) {
        Graph graph = runtimeCache().getGraphByName(graphName);
        if (graph == null) {
            throw new RuntimeException("Graph not exist.");
        }
        if (!(graph instanceof MemoryGraph)) {
            throw new RuntimeException("Graph cannot modify.");
        }
        MemoryMutableGraph memoryMutableGraph = new MemoryMutableGraph((MemoryGraph) graph);
        List<GraphEntity> graphEntities = SeDeUtil.deserializeEntities(input);

        for (GraphEntity entity : graphEntities) {
            if (entity instanceof GraphVertex) {
                memoryMutableGraph.addVertex(((GraphVertex) entity).getVertex());
            } else {
                memoryMutableGraph.addEdge(((GraphEdge) entity).getEdge());
            }
        }
        GraphMemoryServer insertServer = runtimeCache().getServerByName(graphName);
        if (insertServer == null || insertServer.getGraphAccessors().isEmpty()) {
            throw new RuntimeException("Server or graph accessor not available for graph: " + graphName);
        }
        runtimeCache().getConsolidateServer().executeConsolidateTask(
            insertServer.getGraphAccessors().get(0), memoryMutableGraph);
        runtimeCache().markGraphUpdated(graphName);
        return "Success to add entities, num: " + graphEntities.size();
    }

    @Post
    @Mapping("/graph/delEntity")
    public String deleteEntity(@Param("graphName") String graphName,
                               @Body String input) {
        Graph graph = runtimeCache().getGraphByName(graphName);
        if (graph == null) {
            throw new RuntimeException("Graph not exist.");
        }
        if (!(graph instanceof MemoryGraph)) {
            throw new RuntimeException("Graph cannot modify.");
        }
        MemoryMutableGraph memoryMutableGraph = new MemoryMutableGraph((MemoryGraph) graph);
        List<GraphEntity> graphEntities = SeDeUtil.deserializeEntities(input);
        for (GraphEntity entity : graphEntities) {
            if (entity instanceof GraphVertex) {
                memoryMutableGraph.removeVertex(entity.getLabel(),
                    ((GraphVertex) entity).getVertex().getId());
            } else {
                memoryMutableGraph.removeEdge(((GraphEdge) entity).getEdge());
            }
        }
        runtimeCache().markGraphUpdated(graphName);
        return "Success to remove entities, num: " + graphEntities.size();
    }

    @Post
    @Mapping("/query/context")
    public String createContext(@Param("graphName") String graphName) {
        GraphMemoryServer server = runtimeCache().getServerByName(graphName);
        if (server == null) {
            throw new RuntimeException("Server not exist.");
        }
        String sessionId = server.createSession();
        runtimeCache().putSession(server, sessionId);
        return sessionId;
    }

    @Post
    @Mapping("/query/exec")
    public String execQuery(@Param("sessionId") String sessionId,
                            @Body String query) {
        String graphName = runtimeCache().getGraphNameBySession(sessionId);
        if (graphName == null) {
            throw new RuntimeException("Graph not exist.");
        }
        GraphMemoryServer server = runtimeCache().getServerByName(graphName);
        VectorSearch search = new VectorSearch(null, sessionId);
        search.addVector(new KeywordVector(query));
        server.search(search);
        if (server.getGraphAccessors().isEmpty()) {
            throw new RuntimeException("No graph accessor available for session: " + sessionId);
        }
        Context context = server.verbalize(sessionId,
            new SubgraphSemanticPromptFunction(server.getGraphAccessors().get(0)));
        return context.toString();
    }

    @Post
    @Mapping("/query/result")
    public String getResult(@Param("sessionId") String sessionId) {
        String graphName = runtimeCache().getGraphNameBySession(sessionId);
        if (graphName == null) {
            throw new RuntimeException("Graph not exist.");
        }
        GraphMemoryServer server = runtimeCache().getServerByName(graphName);
        List<GraphEntity> result = server.getSessionEntities(sessionId);
        return result.toString();
    }
}
