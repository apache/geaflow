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

package org.apache.geaflow.ai.service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.geaflow.ai.GraphMemoryServer;
import org.apache.geaflow.ai.consolidate.ConsolidateServer;
import org.apache.geaflow.ai.graph.Graph;
import org.noear.solon.annotation.Component;

@Component
public class ServerMemoryCache {

    private final Map<String, Graph> name2Graph = new ConcurrentHashMap<>();
    private final Map<String, GraphMemoryServer> name2Server = new ConcurrentHashMap<>();
    private final Map<String, String> session2GraphName = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> graphVersions = new ConcurrentHashMap<>();
    private final ConsolidateServer consolidateServer = new ConsolidateServer();

    public void putGraph(Graph g) {
        String name = g.getGraphSchema().getName();
        name2Graph.put(name, g);
        graphVersions.putIfAbsent(name, new AtomicLong(1L));
    }

    public void putServer(GraphMemoryServer server) {
        if (server.getGraphAccessors().isEmpty()) {
            throw new RuntimeException("Cannot register server without graph accessor");
        }
        String name = server.getGraphAccessors().get(0).getGraphSchema().getName();
        name2Server.put(name, server);
        graphVersions.putIfAbsent(name, new AtomicLong(1L));
    }

    public void putSession(GraphMemoryServer server, String sessionId) {
        if (server.getGraphAccessors().isEmpty()) {
            throw new RuntimeException("Cannot register session without graph accessor");
        }
        session2GraphName.put(sessionId,
            server.getGraphAccessors().get(0).getGraphSchema().getName());
    }

    public Graph getGraphByName(String name) {
        return name2Graph.get(name);
    }

    public GraphMemoryServer getServerByName(String name) {
        return name2Server.get(name);
    }

    public String getGraphNameBySession(String sessionId) {
        return session2GraphName.get(sessionId);
    }

    public ConsolidateServer getConsolidateServer() {
        return consolidateServer;
    }

    public String getGraphVersion(String graphName) {
        AtomicLong version = graphVersions.get(graphName);
        return version == null ? null : "v" + version.get();
    }

    public String markGraphUpdated(String graphName) {
        AtomicLong version = graphVersions.get(graphName);
        if (version == null) {
            throw new IllegalArgumentException("Unknown graph: " + graphName);
        }
        return "v" + version.incrementAndGet();
    }

    public ReadinessStatus keywordReadiness(String graphName) {
        if (name2Graph.get(graphName) == null) {
            return new ReadinessStatus(false, graphName, getGraphVersion(graphName), "GRAPH_NOT_LOADED");
        }
        if (name2Server.get(graphName) == null) {
            return new ReadinessStatus(false, graphName, getGraphVersion(graphName), "SERVER_NOT_LOADED");
        }
        GraphMemoryServer server = name2Server.get(graphName);
        if (server.getGraphAccessors().isEmpty()) {
            return new ReadinessStatus(false, graphName, getGraphVersion(graphName), "GRAPH_ACCESSOR_NOT_READY");
        }
        for (org.apache.geaflow.ai.index.IndexStore store : server.getIndexStores()) {
            if (store instanceof org.apache.geaflow.ai.index.EntityAttributeIndexStore
                && ((org.apache.geaflow.ai.index.EntityAttributeIndexStore) store).isInitialized()) {
                return new ReadinessStatus(true, graphName, getGraphVersion(graphName), "READY");
            }
        }
        return new ReadinessStatus(false, graphName, getGraphVersion(graphName), "KEYWORD_INDEX_NOT_READY");
    }

    public static class ReadyStatus {
        private final boolean ready;
        private final String graphName;
        private final String graphVersion;
        private final String reason;

        public ReadyStatus(boolean ready, String reason) {
            this(ready, null, null, reason);
        }

        public ReadyStatus(boolean ready, String graphName, String graphVersion, String reason) {
            this.ready = ready;
            this.graphName = graphName;
            this.graphVersion = graphVersion;
            this.reason = reason;
        }

        public boolean isReady() {
            return ready;
        }

        public String getReason() {
            return reason;
        }

        public String getGraphName() {
            return graphName;
        }

        public String getGraphVersion() {
            return graphVersion;
        }
    }

    public static final class ReadinessStatus extends ReadyStatus {

        public static ReadinessStatus ready(String graphName, String graphVersion) {
            return new ReadinessStatus(true, graphName, graphVersion, "READY");
        }

        public static ReadinessStatus notReady(String reason) {
            return new ReadinessStatus(false, null, null, reason);
        }

        public ReadinessStatus(boolean ready, String graphName, String graphVersion, String reason) {
            super(ready, graphName, graphVersion, reason);
        }
    }
}
