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
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.geaflow.ai.graph.GraphAccessor;
import org.apache.geaflow.ai.graph.GraphEntity;
import org.apache.geaflow.ai.graph.GraphVertex;
import org.apache.geaflow.ai.graph.VertexVersionWindow;
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
 * invalidate-and-rebuild: the index is built once, then writes are applied in place. An upsert maps
 * to Lucene's update-by-term (tombstone plus insert) and a remove maps to delete-by-term (a bit in
 * a per segment bitset). Cost is proportional to the change, not to graph size. Because both are
 * keyed by {@code ModelUtils.getGraphEntityKey}, they are idempotent, so callers do not need to
 * supply an exact delta.
 *
 * <p><b>Document set equivalence.</b> The index contains exactly what a per-query global index
 * would contain: every vertex whose {@link IndexStore} entry is non-empty. Edges are excluded,
 * matching {@code searchWithGlobalGraph}, so recall is unchanged.
 *
 * <p><b>Version guard.</b> Validity is tracked against {@link GraphAccessor#getVertexVersion()}.
 * A write batch is applied in place only when its {@link VertexVersionWindow} proves it describes
 * every vertex level change since the version this index last accepted. Anything else, including
 * mutations made outside the reporting path (for example directly through
 * {@code MemoryMutableGraph}), forces a rebuild rather than serving stale results. Edge writes do
 * not invalidate anything, since the document set depends on vertices only.
 *
 * <p><b>Concurrency.</b> Searches take the read lock and run concurrently; building, invalidating
 * and applying writes take the write lock.
 */
public class ResidentSearchIndex {

    private static final Logger LOGGER = LoggerFactory.getLogger(ResidentSearchIndex.class);

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private volatile GraphSearchStore store;
    private volatile boolean globalIndexBuilt = false;
    private volatile long builtVersion = GraphAccessor.VERSION_UNSUPPORTED;

    private volatile long buildCount = 0L;
    private volatile long upsertCount = 0L;
    private volatile long removeCount = 0L;

    /**
     * Builds the full graph keyword index if it is absent or has gone stale.
     */
    public void ensureGlobalIndex(GraphAccessor graphAccessor, IndexStore indexStore) {
        lock.writeLock().lock();
        try {
            ensureGlobalIndexLocked(graphAccessor, indexStore);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Ensures the index is valid and searches it.
     *
     * <p>The fast path holds only the read lock, so concurrent queries do not serialize. Validation
     * and search happen under the same lock acquisition: with two separate calls a concurrent write
     * could invalidate the index in between, leaving the query to fail on a missing index.
     */
    public List<GraphEntity> searchWithIndex(GraphAccessor graphAccessor, IndexStore indexStore,
                                             String query) {
        lock.readLock().lock();
        try {
            if (isUsableLocked(graphAccessor)) {
                return store.search(query, graphAccessor);
            }
        } finally {
            lock.readLock().unlock();
        }
        lock.writeLock().lock();
        try {
            ensureGlobalIndexLocked(graphAccessor, indexStore);
            return store.search(query, graphAccessor);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Applies written entities to the index in place, without rebuilding it.
     *
     * <p>Safe for both new and rewritten entities. No-op before the first build: the entities will
     * be picked up by it.
     *
     * @param window version range the batch claims to cover, see {@link VertexVersionWindow}
     */
    public void onEntitiesUpserted(GraphAccessor graphAccessor, List<GraphEntity> entities,
                                   IndexStore indexStore, VertexVersionWindow window) {
        applyWrite(graphAccessor, entities, indexStore, false, window);
    }

    /**
     * Applies removed entities to the index in place, without rebuilding it.
     */
    public void onEntitiesRemoved(GraphAccessor graphAccessor, List<GraphEntity> entities,
                                  VertexVersionWindow window) {
        applyWrite(graphAccessor, entities, null, true, window);
    }

    private void applyWrite(GraphAccessor graphAccessor, List<GraphEntity> entities,
                            IndexStore indexStore, boolean removed, VertexVersionWindow window) {
        if (entities == null || entities.isEmpty()) {
            return;
        }
        lock.writeLock().lock();
        try {
            if (!globalIndexBuilt) {
                return;
            }
            if (window == null || !window.covers(builtVersion)) {
                // The batch cannot be proven to describe everything that changed, so applying it
                // would leave the index quietly missing whatever else happened. Rebuild instead.
                LOGGER.info("Resident keyword index cannot accept a write batch, window: {}, "
                    + "accepted version: {}; rebuilding on next query", window, builtVersion);
                invalidateLocked();
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
                    removeCount++;
                    changed = true;
                    continue;
                }
                List<IVector> vectors = indexStore.getEntityIndex(entity);
                if (vectors == null || vectors.isEmpty()) {
                    // An entity without index content is not a document; drop any previous one.
                    // Delete by term is idempotent, so there is no need to track what was indexed.
                    store.removeEntity(entity);
                    changed = true;
                    continue;
                }
                store.upsertVertex((GraphVertex) entity, vectors);
                upsertCount++;
                changed = true;
            }
            if (changed) {
                // One refresh per batch rather than per entity: each refresh opens a new segment.
                store.refresh();
            }
            builtVersion = window.getTo();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Drops the index so that the next query rebuilds it. Needed for changes that are not expressed
     * per entity, such as a schema change altering how every entity is verbalized.
     */
    public void invalidate() {
        lock.writeLock().lock();
        try {
            invalidateLocked();
        } finally {
            lock.writeLock().unlock();
        }
    }

    public List<GraphEntity> search(String query, GraphAccessor graphAccessor) {
        lock.readLock().lock();
        try {
            if (store == null) {
                return Collections.emptyList();
            }
            return store.search(query, graphAccessor);
        } finally {
            lock.readLock().unlock();
        }
    }

    public boolean isGlobalIndexBuilt() {
        return globalIndexBuilt;
    }

    /**
     * Number of full graph builds. Stays at 1 for a workload whose writes all go through
     * {@link #onEntitiesUpserted} / {@link #onEntitiesRemoved}; used by tests to prove the index is
     * neither rebuilt per query nor per write.
     */
    public long getBuildCount() {
        return buildCount;
    }

    public long getUpsertCount() {
        return upsertCount;
    }

    public long getRemoveCount() {
        return removeCount;
    }

    /**
     * Number of documents currently in the index, read from Lucene rather than tracked separately.
     */
    public int getIndexedEntityNum() {
        // Write lock rather than read: reading the document count may have to open a reader, which
        // mutates the store.
        lock.writeLock().lock();
        try {
            return store == null ? 0 : store.getDocNum();
        } finally {
            lock.writeLock().unlock();
        }
    }

    private boolean isUsableLocked(GraphAccessor graphAccessor) {
        if (!globalIndexBuilt || store == null) {
            return false;
        }
        long version = graphAccessor.getVertexVersion();
        return version != GraphAccessor.VERSION_UNSUPPORTED && version == builtVersion;
    }

    private void ensureGlobalIndexLocked(GraphAccessor graphAccessor, IndexStore indexStore) {
        long version = graphAccessor.getVertexVersion();
        if (globalIndexBuilt) {
            if (version != GraphAccessor.VERSION_UNSUPPORTED && version == builtVersion) {
                return;
            }
            // Either the graph changed outside the reporting path, or it cannot report changes at
            // all. Both force a rebuild, which degrades to per-query rebuild rather than stale
            // results.
            invalidateLocked();
        }
        final long start = System.currentTimeMillis();
        GraphSearchStore built = new GraphSearchStore();
        // Deduplication is only needed while scanning; unlike the index itself this set is not
        // retained, so a resident index costs no per vertex heap of its own.
        Set<GraphEntity> seen = new HashSet<>();
        for (Iterator<GraphVertex> it = graphAccessor.scanVertex(); it.hasNext(); ) {
            GraphVertex vertex = it.next();
            List<IVector> vectors = indexStore.getEntityIndex(vertex);
            if (vectors == null || vectors.isEmpty() || !seen.add(vertex)) {
                continue;
            }
            // Plain add during the build: the scan yields each vertex once, so no term lookup for
            // duplicate removal is needed and build cost stays as low as possible.
            built.indexVertex(vertex, vectors);
        }
        built.refresh();
        store = built;
        globalIndexBuilt = true;
        builtVersion = version;
        buildCount++;
        LOGGER.info("Built resident keyword index, entities: {}, vertexVersion: {}, cost: {} ms",
            seen.size(), version, System.currentTimeMillis() - start);
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
        globalIndexBuilt = false;
        builtVersion = GraphAccessor.VERSION_UNSUPPORTED;
    }
}
