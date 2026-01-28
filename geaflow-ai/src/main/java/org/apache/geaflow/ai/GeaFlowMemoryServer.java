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
import org.apache.geaflow.ai.common.util.SeDeUtil;
import org.apache.geaflow.ai.graph.*;
import org.apache.geaflow.ai.graph.io.*;
import org.apache.geaflow.ai.index.EntityAttributeIndexStore;
import org.apache.geaflow.ai.index.vector.KeywordVector;
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

    private static final ServerMemoryCache CACHE = new ServerMemoryCache();

    public static void main(String[] args) {
        System.setProperty("solon.app.name", SERVER_NAME);
        Solon.start(GeaFlowMemoryServer.class, args, app -> {
            app.cfg().loadAdd("application.yml");
            int port = app.cfg().getInt("server.port", DEFAULT_PORT);
            LOGGER.info("Starting {} on port {}", SERVER_NAME, port);
            app.get("/", ctx -> {
                ctx.output("GeaFlow AI Server is running...");
            });
            app.get("/health", ctx -> {
                ctx.output("{\"status\":\"UP\",\"service\":\"" + SERVER_NAME + "\"}");
            });
        });
    }

    @Get
    @Mapping("/api/test")
    public String test() {
        return "GeaFlow Memory Server is working!";
    }

    @Post
    @Mapping("/graph/create")
    public String createGraph(@Body String input) {
        GraphSchema graphSchema = SeDeUtil.deserializeGraphSchema(input);
        String graphName = graphSchema.getName();
        if (graphName == null || CACHE.getGraphByName(graphName) != null) {
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
        CACHE.putGraph(graph);
        LocalMemoryGraphAccessor graphAccessor = new LocalMemoryGraphAccessor(graph);
        LOGGER.info("Success to init empty graph.");

        EntityAttributeIndexStore indexStore = new EntityAttributeIndexStore();
        indexStore.initStore(new SubgraphSemanticPromptFunction(graphAccessor));
        LOGGER.info("Success to init EntityAttributeIndexStore.");

        GraphMemoryServer server = new GraphMemoryServer();
        server.addGraphAccessor(graphAccessor);
        server.addIndexStore(indexStore);
        LOGGER.info("Success to init GraphMemoryServer.");
        CACHE.putServer(server);

        LOGGER.info("Success to init graph. SCHEMA: {}", graphSchema);
        return "createGraph has been called, graphName: " + graphName;
    }

    @Post
    @Mapping("/graph/addEntitySchema")
    public String addSchema(@Param("graphName") String graphName,
                            @Body String input) {
        Graph graph = CACHE.getGraphByName(graphName);
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
            throw new RuntimeException("Cannt add schema: " + input);
        }
        return "addSchema has been called, schemaName: " + schemaName;
    }

    @Post
    @Mapping("/graph/getGraphSchema")
    public String getSchema(@Param("graphName") String graphName) {
        Graph graph = CACHE.getGraphByName(graphName);
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
        Graph graph = CACHE.getGraphByName(graphName);
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
        CACHE.getConsolidateServer().executeConsolidateTask(
            CACHE.getServerByName(graphName).getGraphAccessors().get(0), memoryMutableGraph,
            CACHE.getServerByName(graphName).getIndexStores());
        return "Success to add entities, num: " + graphEntities.size();
    }

    @Post
    @Mapping("/graph/delEntity")
    public String deleteEntity(@Param("graphName") String graphName,
                               @Body String input) {
        Graph graph = CACHE.getGraphByName(graphName);
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
        return "Success to remove entities, num: " + graphEntities.size();
    }

    @Post
    @Mapping("/query/context")
    public String createContext(@Param("graphName") String graphName) {
        GraphMemoryServer server = CACHE.getServerByName(graphName);
        if (server == null) {
            throw new RuntimeException("Server not exist.");
        }
        String sessionId = server.createSession();
        CACHE.putSession(server, sessionId);
        return sessionId;
    }

    @Post
    @Mapping("/query/exec")
    public String execQuery(@Param("sessionId") String sessionId,
                            @Body String query) {
        String graphName = CACHE.getGraphNameBySession(sessionId);
        if (graphName == null) {
            throw new RuntimeException("Graph not exist.");
        }
        GraphMemoryServer server = CACHE.getServerByName(graphName);
        VectorSearch search = new VectorSearch(null, sessionId);
        search.addVector(new KeywordVector(query));
        server.search(search);
        Context context = server.verbalize(sessionId,
            new SubgraphSemanticPromptFunction(server.getGraphAccessors().get(0)));
        return context.toString();
    }

    @Post
    @Mapping("/query/result")
    public String getResult(@Param("sessionId") String sessionId) {
        String graphName = CACHE.getGraphNameBySession(sessionId);
        if (graphName == null) {
            throw new RuntimeException("Graph not exist.");
        }
        GraphMemoryServer server = CACHE.getServerByName(graphName);
        List<GraphEntity> result = server.getSessionEntities(sessionId);
        return result.toString();
    }
}
