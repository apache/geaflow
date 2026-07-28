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

package org.apache.geaflow.ai.operator;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.geaflow.ai.graph.GraphAccessor;
import org.apache.geaflow.ai.graph.GraphEntity;
import org.apache.geaflow.ai.graph.GraphVertex;
import org.apache.geaflow.ai.index.IndexStore;
import org.apache.geaflow.ai.index.vector.IVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A graph scoped keyword index that survives across queries and is maintained incrementally.
 *
 * <p>Without it, every global search would build a throw-away {@link GraphSearchStore} from a full
 * graph scan, paying index construction cost on the query path and discarding the result.
 *
 * <p><b>Maintenance model.</b> Follows the standard inverted index approach rather than
 * invalidate-and-rebuild: the index is built once, then writes are applied in place —
 * {@code upsert} maps to Lucene's update-by-term (标记删除 + 新增) and {@code remove} maps to
 * delete-by-term (per segment bitset). Cost is proportional to the change, not to graph size.
 * Because updates are keyed by {@code ModelUtils.getGraphEntityKey}, they are idempotent, so
 * callers do not need to supply an exact delta.
 *
 * <p><b>Document set equivalence.</b> The index contains exactly what a per-query global index
 * would contain: every vertex whose {@link IndexStore} entry is non-empty. Edges are excluded,
 * matching {@code searchWithGlobalGraph}, so recall is unchanged.
 *
 * <p><b>Version guard.</b> Validity is tracked against {@link GraphAccessor#getVertexVersion()}.
 * In-place maintenance keeps the accepted version in step, so the guard exists only to catch graph
 * mutations made outside this class (for example directly through {@code MemoryMutableGraph}),
 * which force a rebuild rather than serving stale results. Edge writes do not invalidate anything,
 * since the document set depends on vertices only.
 */
public class ResidentSearchIndex {

    private static final Logger LOGGER = LoggerFactory.getLogger(ResidentSearchIndex.class);

    private final Object lock = new Object();

    private GraphSearchStore store;
    private Set<GraphEntity> indexedEntities = new HashSet<>();
    private boolean globalIndexBuilt = false;
    private long builtVersion = GraphAccessor.VERSION_UNSUPPORTED;

    private long buildCount = 0L;
    private long upsertCount = 0L;
    private long removeCount = 0L;

    /**
     * Builds the full graph keyword index if it is absent or has gone stale.
     */
    public void ensureGlobalIndex(GraphAccessor graphAccessor, IndexStore indexStore) {
        synchronized (lock) {
            ensureGlobalIndexLocked(graphAccessor, indexStore);
        }
    }

    /**
     * Ensures the index is valid and searches it atomically.
     *
     * <p>Doing both under one lock matters: with two separate calls a concurrent write could
     * invalidate the index in between, leaving the query to fail on a missing index.
     */
    public List<GraphEntity> searchWithIndex(GraphAccessor graphAccessor, IndexStore indexStore,
                                             String query) {
        synchronized (lock) {
            ensureGlobalIndexLocked(graphAccessor, indexStore);
            return store.search(query, graphAccessor);
        }
    }

    /**
     * Applies written entities to the index in place, without rebuilding it.
     *
     * <p>Safe for both new and rewritten entities. No-op before the first build: the entities will
     * be picked up by it.
     */
    public void onEntitiesUpserted(GraphAccessor graphAccessor, List<GraphEntity> entities,
                                   IndexStore indexStore) {
        applyWrite(graphAccessor, entities, indexStore, false);
    }

    /**
     * Applies removed entities to the index in place, without rebuilding it.
     */
    public void onEntitiesRemoved(GraphAccessor graphAccessor, List<GraphEntity> entities) {
        applyWrite(graphAccessor, entities, null, true);
    }

    private void applyWrite(GraphAccessor graphAccessor, List<GraphEntity> entities,
                            IndexStore indexStore, boolean removed) {
        if (entities == null || entities.isEmpty()) {
            return;
        }
        synchronized (lock) {
            if (!globalIndexBuilt) {
                return;
            }
            boolean changed = false;
            for (GraphEntity entity : entities) {
                // Only vertices are part of this index; edges merely advance the accepted version.
                if (!(entity instanceof GraphVertex)) {
                    continue;
                }
                if (removed) {
                    store.removeEntity(entity);
                    indexedEntities.remove(entity);
                    removeCount++;
                    changed = true;
                    continue;
                }
                List<IVector> vectors = indexStore.getEntityIndex(entity);
                if (vectors == null || vectors.isEmpty()) {
                    // An entity without index content is not a document; drop any previous one.
                    if (indexedEntities.remove(entity)) {
                        store.removeEntity(entity);
                        changed = true;
                    }
                    continue;
                }
                store.upsertVertex((GraphVertex) entity, vectors);
                indexedEntities.add(entity);
                upsertCount++;
                changed = true;
            }
            if (changed) {
                // One refresh per batch rather than per entity: each refresh opens a new segment.
                store.refresh();
            }
            builtVersion = graphAccessor.getVertexVersion();
        }
    }

    /**
     * Drops the index so that the next query rebuilds it. Needed for changes that are not expressed
     * per entity, such as a schema change altering how every entity is verbalized.
     */
    public void invalidate() {
        synchronized (lock) {
            invalidateLocked();
        }
    }

    public List<GraphEntity> search(String query, GraphAccessor graphAccessor) {
        synchronized (lock) {
            if (store == null) {
                return Collections.emptyList();
            }
            return store.search(query, graphAccessor);
        }
    }

    public boolean isGlobalIndexBuilt() {
        synchronized (lock) {
            return globalIndexBuilt;
        }
    }

    /**
     * Number of full graph builds. Stays at 1 for a workload whose writes all go through
     * {@link #onEntitiesUpserted} / {@link #onEntitiesRemoved}; used by tests to prove the index is
     * neither rebuilt per query nor per write.
     */
    public long getBuildCount() {
        synchronized (lock) {
            return buildCount;
        }
    }

    public long getUpsertCount() {
        synchronized (lock) {
            return upsertCount;
        }
    }

    public long getRemoveCount() {
        synchronized (lock) {
            return removeCount;
        }
    }

    public int getIndexedEntityNum() {
        synchronized (lock) {
            return indexedEntities.size();
        }
    }

    private void ensureGlobalIndexLocked(GraphAccessor graphAccessor, IndexStore indexStore) {
        long version = graphAccessor.getVertexVersion();
        if (globalIndexBuilt) {
            if (version != GraphAccessor.VERSION_UNSUPPORTED && version == builtVersion) {
                return;
            }
            // Either the graph changed outside this class, or it cannot report changes at all.
            // Both force a rebuild, which degrades to per-query rebuild rather than stale results.
            invalidateLocked();
        }
        final long start = System.currentTimeMillis();
        store = new GraphSearchStore();
        indexedEntities = new HashSet<>();
        for (Iterator<GraphVertex> it = graphAccessor.scanVertex(); it.hasNext(); ) {
            GraphVertex vertex = it.next();
            List<IVector> vectors = indexStore.getEntityIndex(vertex);
            if (vectors == null || vectors.isEmpty() || !indexedEntities.add(vertex)) {
                continue;
            }
            // Plain add during the build: the scan yields each vertex once, so no term lookup for
            // duplicate removal is needed and build cost stays as low as possible.
            store.indexVertex(vertex, vectors);
        }
        store.refresh();
        globalIndexBuilt = true;
        builtVersion = version;
        buildCount++;
        LOGGER.info("Built resident keyword index, entities: {}, vertexVersion: {}, cost: {} ms",
            indexedEntities.size(), version, System.currentTimeMillis() - start);
    }

    private void invalidateLocked() {
        if (store != null) {
            try {
                store.close();
            } catch (Throwable e) {
                LOGGER.warn("Ignore error on closing resident keyword index", e);
            }
        }
        store = null;
        indexedEntities = new HashSet<>();
        globalIndexBuilt = false;
        builtVersion = GraphAccessor.VERSION_UNSUPPORTED;
    }
}
