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

import org.apache.geaflow.ai.graph.GraphAccessor;
import org.apache.geaflow.ai.index.IndexStore;
import org.apache.geaflow.ai.operator.SearchOperator;
import org.apache.geaflow.ai.operator.SessionOperator;
import org.apache.geaflow.ai.search.VectorSearch;
import org.apache.geaflow.ai.session.SessionManagement;
import org.apache.geaflow.ai.subgraph.SubGraph;
import org.apache.geaflow.ai.verbalization.Context;
import org.apache.geaflow.ai.verbalization.VerbalizationFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class GraphMemoryServer {

    private final SessionManagement sessionManagement = SessionManagement.INSTANCE;
    private final List<GraphAccessor> graphAccessors = new ArrayList<>();
    private final List<IndexStore> indexStores = new ArrayList<>();

    public void addGraphAccessor(GraphAccessor graph) {
        if (graph != null) {
            graphAccessors.add(graph);
        }
    }

    public void addIndexStore(IndexStore indexStore) {
        if (indexStore != null) {
            indexStores.add(indexStore);
        }
    }

    public String createSession() {
        String sessionId = sessionManagement.createSession();
        if (sessionId == null) {
            throw new RuntimeException("Cannot create new session");
        }
        return sessionId;
    }

    public String search(VectorSearch search) {
        String sessionId = search.getSessionId();
        if (sessionId == null || sessionId.isEmpty()) {
            throw new RuntimeException("Session id is empty");
        }
        if (!sessionManagement.sessionExists(sessionId)) {
            sessionManagement.createSession(sessionId);
        }

        SessionOperator op = new SessionOperator(graphAccessors.get(0), indexStores.get(0));
        SessionOperator casts_op = new SessionOperator(graphAccessors.get(0), indexStores.get(0));
        SessionOperator lucene_op = new SessionOperator(graphAccessors.get(0), indexStores.get(0));
        SessionOperator op3 = new SessionOperator(graphAccessors.get(0), indexStores.get(0));
        applySearch(sessionId, op, search);
        return sessionId;
    }

    public void applySearch(String sessionId, SearchOperator operator, VectorSearch search) {
        //获取session
        SessionManagement manager = SessionManagement.INSTANCE;
        if (!manager.sessionExists(sessionId)) {
            return;
        }
        //应用算子于管理器的子图列表
        List<SubGraph> result = operator.apply(manager.getSubGraph(sessionId), search);
        manager.setSubGraph(sessionId, result);
    }

    public Context verbalize(String sessionId, VerbalizationFunction verbalizationFunction) {
        List<SubGraph> subGraphList = sessionManagement.getSubGraph(sessionId);
        List<String> subGraphStringList = new ArrayList<>(subGraphList.size());
        for (SubGraph subGraph : subGraphList) {
            subGraphStringList.add(verbalizationFunction.verbalize(subGraph));
        }
        subGraphStringList = subGraphStringList.stream().sorted().collect(Collectors.toList());
        StringBuilder stringBuilder = new StringBuilder();
        for (String subGraph : subGraphStringList) {
            stringBuilder.append(subGraph).append("\n");
        }
        return new Context(stringBuilder.toString());
    }

}
