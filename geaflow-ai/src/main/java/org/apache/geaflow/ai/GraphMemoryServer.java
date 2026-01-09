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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.geaflow.ai.graph.GraphAccessor;
import org.apache.geaflow.ai.graph.GraphEntity;
import org.apache.geaflow.ai.index.EmbeddingIndexStore;
import org.apache.geaflow.ai.index.EntityAttributeIndexStore;
import org.apache.geaflow.ai.index.IndexStore;
import org.apache.geaflow.ai.operator.EmbeddingOperator;
import org.apache.geaflow.ai.operator.SearchOperator;
import org.apache.geaflow.ai.operator.SessionOperator;
import org.apache.geaflow.ai.search.VectorSearch;
import org.apache.geaflow.ai.session.SessionManagement;
import org.apache.geaflow.ai.subgraph.SubGraph;
import org.apache.geaflow.ai.verbalization.Context;
import org.apache.geaflow.ai.verbalization.VerbalizationFunction;

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

        for (IndexStore indexStore : indexStores) {
            if (indexStore instanceof EntityAttributeIndexStore) {
                SessionOperator searchOperator = new SessionOperator(graphAccessors.get(0), indexStore);
                applySearch(sessionId, searchOperator, search);
            }
            if (indexStore instanceof EmbeddingIndexStore) {
                EmbeddingOperator embeddingOperator = new EmbeddingOperator(graphAccessors.get(0), indexStore);
                applySearch(sessionId, embeddingOperator, search);
            }
        }
        return sessionId;
    }

    private void applySearch(String sessionId, SearchOperator operator, VectorSearch search) {
        SessionManagement manager = SessionManagement.INSTANCE;
        if (!manager.sessionExists(sessionId)) {
            return;
        }
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
        stringBuilder.append(verbalizationFunction.verbalizeGraphSchema());
        return new Context(stringBuilder.toString());
    }

    public List<GraphEntity> getSessionEntities(String sessionId) {
        List<SubGraph> subGraphList = sessionManagement.getSubGraph(sessionId);
        List<String> subGraphStringList = new ArrayList<>(subGraphList.size());
        Set<GraphEntity> entitySet = new HashSet<>();
        for (SubGraph subGraph : subGraphList) {
            entitySet.addAll(subGraph.getGraphEntityList());
        }
        List<GraphEntity> results = new ArrayList<>(entitySet);
        return results;
    }

}
