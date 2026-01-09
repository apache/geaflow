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

package org.apache.geaflow.context.core.cache;

import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.geaflow.context.api.result.ContextSearchResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * LRU cache for Context Memory query results.
 * Caches hybrid search results to improve performance.
 */
public class QueryCache {

  private static final Logger LOGGER = LoggerFactory.getLogger(QueryCache.class);

  private final int maxSize;
  private final long ttlMillis;
  private final LinkedHashMap<String, CacheEntry> cache;

  /**
   * Constructor.
   *

   * @param maxSize Maximum cache size (number of entries)
   * @param ttlMillis Time-to-live in milliseconds
   */
  public QueryCache(int maxSize, long ttlMillis) {
    this.maxSize = maxSize;
    this.ttlMillis = ttlMillis;
    this.cache = new LinkedHashMap<String, CacheEntry>(maxSize, 0.75f, true) {
      @Override
      protected boolean removeEldestEntry(Map.Entry<String, CacheEntry> eldest) {
        return size() > maxSize;
      }
    };
  }

  /**
   * Get cached result.
   *

   * @param key Cache key
   * @return Cached result or null if not found
   */
  public ContextSearchResult get(String key) {
    CacheEntry entry = cache.get(key);

    if (entry == null) {
      return null;
    }

    // Check TTL
    if (System.currentTimeMillis() - entry.createdTime > ttlMillis) {
      cache.remove(key);
      LOGGER.debug("Cache entry expired: {}", key);
      return null;
    }

    entry.accessCount++;
    LOGGER.debug("Cache hit: {}", key);
    return entry.result;
  }

  /**
   * Put result in cache.
   *

   * @param key Cache key
   * @param result Search result to cache
   */
  public void put(String key, ContextSearchResult result) {
    if (cache.size() >= maxSize) {
      LOGGER.debug("Cache is full, evicting oldest entry");
    }

    cache.put(key, new CacheEntry(result));
    LOGGER.debug("Cached result: {}", key);
  }

  /**
   * Clear all cache entries.
   */
  public void clear() {
    cache.clear();
    LOGGER.info("Query cache cleared");
  }

  /**
   * Get cache size.
   *

   * @return Current number of entries in cache
   */
  public int size() {
    return cache.size();
  }

  /**
   * Get cache memory size in bytes (approximate).
   *

   * @return Approximate memory size
   */
  public long getMemorySize() {
    long size = 0;
    for (CacheEntry entry : cache.values()) {
      // Rough estimate: 100 bytes per entity
      size += entry.result.getEntities().size() * 100;
      // Rough estimate: 200 bytes per relation
      size += entry.result.getRelations().size() * 200;
    }
    return size;
  }

  /**
   * Cache entry with metadata.
   */
  private static class CacheEntry {

    private final ContextSearchResult result;
    private final long createdTime;
    private long accessCount = 1;

    CacheEntry(ContextSearchResult result) {
      this.result = result;
      this.createdTime = System.currentTimeMillis();
    }
  }
}
