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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import org.apache.geaflow.ai.graph.GraphAccessor;
import org.apache.geaflow.ai.graph.GraphEntity;
import org.apache.geaflow.ai.graph.GraphVertex;
import org.apache.geaflow.ai.index.EmbeddingIndexStore;
import org.apache.geaflow.ai.index.EntityAttributeIndexStore;
import org.apache.geaflow.ai.index.IndexStore;
import org.apache.geaflow.ai.operator.EmbeddingOperator;
import org.apache.geaflow.ai.operator.GraphSearchStore;
import org.apache.geaflow.ai.operator.GraphSearchStore.ScoredGraphEntity;
import org.apache.geaflow.ai.operator.SearchOperator;
import org.apache.geaflow.ai.operator.SessionOperator;
import org.apache.geaflow.ai.search.VectorSearch;
import org.apache.geaflow.ai.session.SessionManagement;
import org.apache.geaflow.ai.subgraph.SubGraph;
import org.apache.geaflow.ai.verbalization.Context;
import org.apache.geaflow.ai.verbalization.VerbalizationFunction;

public class GraphMemoryServer implements AutoCloseable {

    private final SessionManagement sessionManagement = new SessionManagement();
    private final List<GraphAccessor> graphAccessors = new ArrayList<>();
    private final List<IndexStore> indexStores = new ArrayList<>();
    private final ReadWriteLock keywordStoreLock = new ReentrantReadWriteLock();
    private GraphSearchStore keywordSearchStore;
    private GraphAccessor keywordSearchAccessor;
    private EntityAttributeIndexStore keywordSearchIndex;
    private int keywordSearchStoreBuildCount;
    private boolean closed;

    public void addGraphAccessor(GraphAccessor graph) {
        if (graph != null) {
            keywordStoreLock.writeLock().lock();
            try {
                closeKeywordSearchStore();
                graphAccessors.add(graph);
            } finally {
                keywordStoreLock.writeLock().unlock();
            }
        }
    }

    public List<GraphAccessor> getGraphAccessors() {
        return graphAccessors;
    }

    public void addIndexStore(IndexStore indexStore) {
        if (indexStore != null) {
            keywordStoreLock.writeLock().lock();
            try {
                closeKeywordSearchStore();
                indexStores.add(indexStore);
            } finally {
                keywordStoreLock.writeLock().unlock();
            }
        }
    }

    public List<IndexStore> getIndexStores() {
        return indexStores;
    }

    /** Executes bounded keyword retrieval without creating a session. */
    public List<ScoredGraphEntity> searchKeyword(String query, int topK, int maxCandidates,
                                                 long deadlineNanos) {
        if (graphAccessors.isEmpty()) {
            throw new IllegalStateException("No graph accessor available");
        }
        EntityAttributeIndexStore keywordIndex = findKeywordIndex();
        if (keywordIndex == null) {
            throw new IllegalStateException("Keyword index is not available");
        }
        GraphAccessor accessor = graphAccessors.get(0);
        GraphSearchStore store = acquireKeywordSearchStore(accessor, keywordIndex, deadlineNanos);
        try {
            if (System.nanoTime() > deadlineNanos) {
                throw retrievalTimeout();
            }
            return store.searchScored(query, accessor, topK, maxCandidates, deadlineNanos);
        } finally {
            keywordStoreLock.readLock().unlock();
        }
    }

    /** Invalidates the cached keyword index after graph data or schemas change. */
    public void invalidateKeywordSearchIndex() {
        keywordStoreLock.writeLock().lock();
        try {
            closeKeywordSearchStore();
        } finally {
            keywordStoreLock.writeLock().unlock();
        }
    }

    @Override
    public void close() {
        keywordStoreLock.writeLock().lock();
        try {
            closed = true;
            closeKeywordSearchStore();
        } finally {
            keywordStoreLock.writeLock().unlock();
        }
    }

    public int getKeywordSearchStoreBuildCount() {
        keywordStoreLock.readLock().lock();
        try {
            return keywordSearchStoreBuildCount;
        } finally {
            keywordStoreLock.readLock().unlock();
        }
    }

    private GraphSearchStore acquireKeywordSearchStore(GraphAccessor accessor,
                                                        EntityAttributeIndexStore keywordIndex,
                                                        long deadlineNanos) {
        lockUntilDeadline(keywordStoreLock.readLock(), deadlineNanos);
        if (closed) {
            keywordStoreLock.readLock().unlock();
            throw new IllegalStateException("graph memory server is closed");
        }
        if (keywordSearchStore != null && keywordSearchAccessor == accessor
            && keywordSearchIndex == keywordIndex) {
            return keywordSearchStore;
        }
        keywordStoreLock.readLock().unlock();
        lockUntilDeadline(keywordStoreLock.writeLock(), deadlineNanos);
        try {
            if (closed) {
                throw new IllegalStateException("graph memory server is closed");
            }
            if (keywordSearchStore == null || keywordSearchAccessor != accessor
                || keywordSearchIndex != keywordIndex) {
                closeKeywordSearchStore();
                keywordSearchStore = buildKeywordSearchStore(accessor, keywordIndex, deadlineNanos);
                keywordSearchAccessor = accessor;
                keywordSearchIndex = keywordIndex;
                keywordSearchStoreBuildCount++;
            }
            keywordStoreLock.readLock().lock();
            return keywordSearchStore;
        } finally {
            keywordStoreLock.writeLock().unlock();
        }
    }

    private static void lockUntilDeadline(Lock lock, long deadlineNanos) {
        long remaining = deadlineNanos - System.nanoTime();
        try {
            if (remaining <= 0 || !lock.tryLock(remaining, TimeUnit.NANOSECONDS)) {
                throw retrievalTimeout();
            }
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw retrievalTimeout();
        }
    }

    private static GraphSearchStore buildKeywordSearchStore(GraphAccessor accessor,
                                                            EntityAttributeIndexStore keywordIndex,
                                                            long deadlineNanos) {
        if (!keywordIndex.isInitialized()) {
            throw new IllegalStateException("Keyword index is not initialized");
        }
        GraphSearchStore store = new GraphSearchStore();
        boolean complete = false;
        try {
            java.util.Iterator<GraphVertex> vertices = accessor.scanVertex();
            while (vertices.hasNext()) {
                if (System.nanoTime() > deadlineNanos) {
                    throw retrievalTimeout();
                }
                GraphVertex vertex = vertices.next();
                List<org.apache.geaflow.ai.index.vector.IVector> vectors = keywordIndex.getEntityIndex(vertex);
                if (vectors != null && !vectors.isEmpty()) {
                    store.indexVertex(vertex, vectors);
                }
            }
            if (System.nanoTime() > deadlineNanos) {
                throw retrievalTimeout();
            }
            store.finishWriting();
            complete = true;
            return store;
        } finally {
            if (!complete) {
                store.close();
            }
        }
    }

    private EntityAttributeIndexStore findKeywordIndex() {
        for (IndexStore indexStore : indexStores) {
            if (indexStore instanceof EntityAttributeIndexStore) {
                return (EntityAttributeIndexStore) indexStore;
            }
        }
        return null;
    }

    private void closeKeywordSearchStore() {
        if (keywordSearchStore != null) {
            final GraphSearchStore previous = keywordSearchStore;
            keywordSearchStore = null;
            keywordSearchAccessor = null;
            keywordSearchIndex = null;
            previous.close();
        }
    }

    private static org.apache.geaflow.ai.retrieval.api.model.RetrievalException retrievalTimeout() {
        return new org.apache.geaflow.ai.retrieval.api.model.RetrievalException(
            org.apache.geaflow.ai.retrieval.api.model.RetrievalErrorCode.RETRIEVAL_TIMEOUT,
            "retrieval deadline exceeded");
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

    public List<GraphEntity> getSessionEntities(String sessionId) {
        List<SubGraph> subGraphList = sessionManagement.getSubGraph(sessionId);
        Set<GraphEntity> entitySet = new HashSet<>();
        for (SubGraph subGraph : subGraphList) {
            entitySet.addAll(subGraph.getGraphEntityList());
        }
        return new ArrayList<>(entitySet);
    }

}
