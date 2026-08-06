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
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.geaflow.ai.graph.GraphAccessor;
import org.apache.geaflow.ai.graph.GraphEntity;
import org.apache.geaflow.ai.graph.VertexVersionWindow;
import org.apache.geaflow.ai.index.EmbeddingIndexStore;
import org.apache.geaflow.ai.index.EntityAttributeIndexStore;
import org.apache.geaflow.ai.index.IndexStore;
import org.apache.geaflow.ai.operator.EmbeddingOperator;
import org.apache.geaflow.ai.operator.ResidentSearchIndex;
import org.apache.geaflow.ai.operator.SearchOperator;
import org.apache.geaflow.ai.operator.SessionOperator;
import org.apache.geaflow.ai.search.VectorSearch;
import org.apache.geaflow.ai.session.SessionManagement;
import org.apache.geaflow.ai.subgraph.SubGraph;
import org.apache.geaflow.ai.verbalization.Context;
import org.apache.geaflow.ai.verbalization.VerbalizationFunction;

public class GraphMemoryServer {

    private final SessionManagement sessionManagement = new SessionManagement();
    private final List<GraphAccessor> graphAccessors = new ArrayList<>();
    private final List<IndexStore> indexStores = new ArrayList<>();

    /**
     * Keyword indexes kept alive across queries, one per keyword index store. Without this the
     * global keyword index would be rebuilt from a full graph scan on every single query.
     */
    private final Map<IndexStore, ResidentSearchIndex> residentIndexes =
        Collections.synchronizedMap(new IdentityHashMap<>());

    public void addGraphAccessor(GraphAccessor graph) {
        if (graph != null) {
            graphAccessors.add(graph);
        }
    }

    public List<GraphAccessor> getGraphAccessors() {
        return graphAccessors;
    }

    public void addIndexStore(IndexStore indexStore) {
        if (indexStore != null) {
            indexStores.add(indexStore);
            if (indexStore instanceof EntityAttributeIndexStore) {
                residentIndexes.put(indexStore, new ResidentSearchIndex());
            }
        }
    }

    public List<IndexStore> getIndexStores() {
        return indexStores;
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

        if (graphAccessors.isEmpty()) {
            throw new RuntimeException("No graph accessor available");
        }
        for (IndexStore indexStore : indexStores) {
            if (indexStore instanceof EntityAttributeIndexStore) {
                SessionOperator searchOperator = new SessionOperator(graphAccessors.get(0),
                    indexStore, residentIndexes.get(indexStore));
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
        SessionManagement manager = sessionManagement;
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

    /**
     * Captures the vertex version before a batch of graph writes. Pass the sealed window to
     * {@link #onEntitiesUpserted} / {@link #onEntitiesRemoved} so the derived structures can tell
     * whether the reported entities really are everything that changed.
     */
    public VertexVersionWindow openVertexVersionWindow() {
        return VertexVersionWindow.open(graphAccessors.isEmpty() ? null : graphAccessors.get(0));
    }

    /**
     * Applies written entities to the derived structures in place. Handles both new and rewritten
     * entities, so callers do not need to distinguish them.
     *
     * <p>Memoized verbalizations need no explicit invalidation here: every entry carries the source
     * version it was computed from, so the write itself makes the affected entries stale.
     *
     * @param window version range the batch covers, obtained from
     *     {@link #openVertexVersionWindow()} and sealed after the writes
     */
    public void onEntitiesUpserted(List<GraphEntity> entities, VertexVersionWindow window) {
        if (entities == null || entities.isEmpty() || graphAccessors.isEmpty()) {
            return;
        }
        for (IndexStore indexStore : indexStores) {
            if (!(indexStore instanceof EntityAttributeIndexStore)) {
                continue;
            }
            ResidentSearchIndex residentIndex = residentIndexes.get(indexStore);
            if (residentIndex != null) {
                residentIndex.onEntitiesUpserted(graphAccessors.get(0), entities, indexStore,
                    window);
            }
        }
    }

    /**
     * Applies removed entities to the derived structures in place.
     */
    public void onEntitiesRemoved(List<GraphEntity> entities, VertexVersionWindow window) {
        if (entities == null || entities.isEmpty() || graphAccessors.isEmpty()) {
            return;
        }
        for (IndexStore indexStore : indexStores) {
            if (!(indexStore instanceof EntityAttributeIndexStore)) {
                continue;
            }
            ResidentSearchIndex residentIndex = residentIndexes.get(indexStore);
            if (residentIndex != null) {
                residentIndex.onEntitiesRemoved(graphAccessors.get(0), entities, window);
            }
        }
    }

    /**
     * Drops the derived structures wholesale. Used for changes that cannot be expressed per entity,
     * such as a schema change altering how every entity is verbalized.
     */
    public void onSchemaChanged() {
        for (IndexStore indexStore : indexStores) {
            if (!(indexStore instanceof EntityAttributeIndexStore)) {
                continue;
            }
            ((EntityAttributeIndexStore) indexStore).invalidateCache();
            ResidentSearchIndex residentIndex = residentIndexes.get(indexStore);
            if (residentIndex != null) {
                residentIndex.invalidate();
            }
        }
    }

    public List<GraphEntity> getSessionEntities(String sessionId) {
        List<SubGraph> subGraphList = sessionManagement.getSubGraph(sessionId);
        Set<GraphEntity> entitySet = new HashSet<>();
        for (SubGraph subGraph : subGraphList) {
            entitySet.addAll(subGraph.getGraphEntityList());
        }
        return new ArrayList<>(entitySet);
    }

}
