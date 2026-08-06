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

package org.apache.geaflow.ai.index;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import org.apache.geaflow.ai.common.config.Constants;
import org.apache.geaflow.ai.graph.GraphAccessor;
import org.apache.geaflow.ai.graph.GraphEdge;
import org.apache.geaflow.ai.graph.GraphEntity;
import org.apache.geaflow.ai.graph.GraphVertex;
import org.apache.geaflow.ai.index.vector.IVector;
import org.apache.geaflow.ai.index.vector.KeywordVector;
import org.apache.geaflow.ai.subgraph.SubGraph;
import org.apache.geaflow.ai.verbalization.VerbalizationFunction;

/**
 * Derives keyword vectors from an entity by verbalizing it.
 *
 * <p>Verbalization is deterministic for a given entity, but it is not free: it builds a
 * {@link SubGraph}, renders a prompt and allocates intermediate strings. Since retrieval calls
 * {@link #getEntityIndex} once per candidate entity on every query, the results are memoized.
 *
 * <p>Each entry carries the source version it was computed from, so a write invalidates only the
 * entries it actually affects. Comparing a single version for the whole cache would be simpler but
 * would throw away everything memoized so far on every write, which is exactly what the write heavy
 * paths do (consolidate issues roughly thirty edge writes per inserted entity).
 *
 * <p><b>No locking.</b> The memoized function is pure and its result is immutable, so the cache needs
 * no mutual exclusion to be correct: a lookup is one {@link ConcurrentHashMap#get}, and a miss
 * computes outside the map and publishes with {@link ConcurrentHashMap#put}. Two threads racing on
 * the same entity may both compute it, but for the same version they compute the same thing, so the
 * loser only wasted work and either published value is equally valid. A stale entry needs no
 * explicit removal either, the put replaces it.
 *
 * <p>The price is that the size bound is approximate and eviction is not LRU: entries are dropped in
 * map iteration order once the bound is exceeded. For a memoization of a pure function a wrong
 * eviction costs one recompute, which is an acceptable price for not having to hold a monitor across
 * every lookup. A real eviction policy would need a proper cache library; see the module docs.
 */
public class EntityAttributeIndexStore implements IndexStore {

    /** Fraction of the bound dropped per eviction pass, so eviction does not run on every put. */
    private static final int EVICTION_BATCH_DIVISOR = 16;

    private VerbalizationFunction verbFunc;

    private final ConcurrentHashMap<GraphEntity, CachedIndex> verbalizationCache =
        new ConcurrentHashMap<>();

    private final LongAdder cacheHit = new LongAdder();
    private final LongAdder cacheMiss = new LongAdder();

    public void initStore(VerbalizationFunction func) {
        if (func != null) {
            this.verbFunc = func;
        }
        invalidateCache();
    }

    @Override
    public List<IVector> getEntityIndex(GraphEntity entity) {
        if (entity == null) {
            return Collections.emptyList();
        }
        long version = sourceVersionOf(entity);
        if (version == GraphAccessor.VERSION_UNSUPPORTED) {
            // The source cannot tell us when it changes, so memoizing would risk stale results.
            return computeEntityIndex(entity);
        }
        CachedIndex cached = verbalizationCache.get(entity);
        if (cached != null && cached.version == version) {
            cacheHit.increment();
            return cached.vectors;
        }
        // Computed outside the map: verbalization is the expensive part and must not block others.
        // A stale entry needs no explicit removal, the put below replaces it.
        List<IVector> computed = computeEntityIndex(entity);
        cacheMiss.increment();
        verbalizationCache.put(entity, new CachedIndex(computed, version));
        enforceBound();
        return computed;
    }

    /**
     * Keeps the cache near its configured bound. Approximate on purpose: {@code size()} on a
     * concurrent map is an estimate, and several threads may evict at once. Overshooting slightly is
     * acceptable, holding a lock to be exact is not.
     */
    private void enforceBound() {
        int max = Constants.ENTITY_ATTRIBUTE_INDEX_CACHE_MAX_SIZE;
        int size = verbalizationCache.size();
        if (size <= max) {
            return;
        }
        int toDrop = size - max + Math.max(1, max / EVICTION_BATCH_DIVISOR);
        Iterator<GraphEntity> it = verbalizationCache.keySet().iterator();
        while (toDrop-- > 0 && it.hasNext()) {
            it.next();
            it.remove();
        }
    }

    /**
     * The version an entity's verbalization depends on.
     *
     * <p>A vertex is verbalized from itself and the schema, so it only has to watch the vertex
     * version and survives edge writes. An edge is verbalized together with both of its endpoints,
     * so it has to watch every change.
     */
    private long sourceVersionOf(GraphEntity entity) {
        return entity instanceof GraphVertex
            ? verbFunc.getSourceVertexVersion()
            : verbFunc.getSourceVersion();
    }

    private List<IVector> computeEntityIndex(GraphEntity entity) {
        String verbalization;
        if (entity instanceof GraphVertex) {
            verbalization = verbFunc.verbalize(new SubGraph().addVertex((GraphVertex) entity));
        } else {
            verbalization = verbFunc.verbalize(new SubGraph().addEdge((GraphEdge) entity));
        }
        KeywordVector keywordVector = new KeywordVector(verbalization);
        List<IVector> results = new ArrayList<>(1);
        results.add(keywordVector);
        return Collections.unmodifiableList(results);
    }

    /**
     * Drops all memoized verbalizations. Version stamps already keep stale entries from being
     * served, so this is only needed for changes the version does not describe, such as replacing
     * the verbalization function itself.
     */
    public void invalidateCache() {
        verbalizationCache.clear();
    }

    public void invalidateCache(GraphEntity entity) {
        if (entity == null) {
            return;
        }
        verbalizationCache.remove(entity);
    }

    public long getCacheHit() {
        return cacheHit.sum();
    }

    public long getCacheMiss() {
        return cacheMiss.sum();
    }

    public int getCacheSize() {
        return verbalizationCache.size();
    }

    /**
     * A memoized verbalization together with the source version it was computed from. Stamping each
     * entry, rather than the cache as a whole, is what lets a single write invalidate one entry
     * instead of discarding everything memoized so far.
     */
    private static final class CachedIndex {

        private final List<IVector> vectors;
        private final long version;

        private CachedIndex(List<IVector> vectors, long version) {
            this.vectors = vectors;
            this.version = version;
        }
    }
}
