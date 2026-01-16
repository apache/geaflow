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

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.geaflow.ai.GraphMemoryServer;
import org.apache.geaflow.ai.graph.Graph;

public class ServerMemoryCache {

    private final Map<String, Graph> name2Graph = new LinkedHashMap<>();
    private final Map<String, GraphMemoryServer> name2Server = new LinkedHashMap<>();
    private final Map<String, String> session2GraphName = new HashMap<>();

    public void putGraph(Graph g) {
        name2Graph.put(g.getGraphSchema().getName(), g);
    }

    public void putServer(GraphMemoryServer server) {
        name2Server.put(server.getGraphAccessors().get(0)
            .getGraphSchema().getName(), server);
    }

    public void putSession(GraphMemoryServer server, String sessionId) {
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
}
