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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
 * {@link #getEntityIndex} once per candidate entity on every query, the results are memoized in a
 * bounded LRU cache. The cache must be invalidated whenever the underlying entity content changes.
 */
public class EntityAttributeIndexStore implements IndexStore {

    private VerbalizationFunction verbFunc;

    private final int cacheMaxSize = Constants.ENTITY_ATTRIBUTE_INDEX_CACHE_MAX_SIZE;

    private final Map<GraphEntity, List<IVector>> verbalizationCache =
        new LinkedHashMap<GraphEntity, List<IVector>>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<GraphEntity, List<IVector>> eldest) {
                return size() > cacheMaxSize;
            }
        };

    private long cachedVersion = GraphAccessor.VERSION_UNSUPPORTED;
    private long cacheHit = 0L;
    private long cacheMiss = 0L;

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
        long version = verbFunc.getSourceVersion();
        if (version == GraphAccessor.VERSION_UNSUPPORTED) {
            // The source cannot tell us when it changes, so memoizing would risk stale results.
            return computeEntityIndex(entity);
        }
        synchronized (verbalizationCache) {
            if (version != cachedVersion) {
                verbalizationCache.clear();
                cachedVersion = version;
            } else {
                List<IVector> cached = verbalizationCache.get(entity);
                if (cached != null) {
                    cacheHit++;
                    return cached;
                }
            }
        }
        List<IVector> computed = computeEntityIndex(entity);
        synchronized (verbalizationCache) {
            if (version == cachedVersion) {
                cacheMiss++;
                verbalizationCache.put(entity, computed);
            }
        }
        return computed;
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
     * Drops all memoized verbalizations. Must be called when graph content changes, because
     * entity identity ({@code label} + {@code id}) does not cover property values.
     */
    public void invalidateCache() {
        synchronized (verbalizationCache) {
            verbalizationCache.clear();
        }
    }

    public void invalidateCache(GraphEntity entity) {
        if (entity == null) {
            return;
        }
        synchronized (verbalizationCache) {
            verbalizationCache.remove(entity);
        }
    }

    public long getCacheHit() {
        synchronized (verbalizationCache) {
            return cacheHit;
        }
    }

    public long getCacheMiss() {
        synchronized (verbalizationCache) {
            return cacheMiss;
        }
    }

    public int getCacheSize() {
        synchronized (verbalizationCache) {
            return verbalizationCache.size();
        }
    }
}
