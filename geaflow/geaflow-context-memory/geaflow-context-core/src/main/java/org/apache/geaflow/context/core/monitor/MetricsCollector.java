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

package org.apache.geaflow.context.core.monitor;

import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Prometheus metrics collector for Context Memory.
 * Tracks key performance metrics like QPS, latency, cache hit rate, etc.
 */
public class MetricsCollector {

  private static final Logger LOGGER = LoggerFactory.getLogger(MetricsCollector.class);

  // Query metrics
  private final AtomicLong totalQueries = new AtomicLong(0);
  private final AtomicLong totalQueryTime = new AtomicLong(0);
  private final AtomicLong vectorSearchCount = new AtomicLong(0);
  private final AtomicLong graphSearchCount = new AtomicLong(0);
  private final AtomicLong hybridSearchCount = new AtomicLong(0);

  // Cache metrics
  private final AtomicLong cacheHits = new AtomicLong(0);
  private final AtomicLong cacheMisses = new AtomicLong(0);
  private final AtomicLong cacheSize = new AtomicLong(0);

  // Ingestion metrics
  private final AtomicLong totalEpisodes = new AtomicLong(0);
  private final AtomicLong totalEntities = new AtomicLong(0);
  private final AtomicLong totalRelations = new AtomicLong(0);

  // Error metrics
  private final AtomicLong queryErrors = new AtomicLong(0);
  private final AtomicLong ingestionErrors = new AtomicLong(0);

  // Storage metrics
  private final AtomicLong storageSizeBytes = new AtomicLong(0);

  /**
   * Record a query execution.
   *
   * @param executionTimeMs Execution time in milliseconds
   * @param searchType Type of search (VECTOR, GRAPH, HYBRID)
   */
  public void recordQuery(long executionTimeMs, String searchType) {
    totalQueries.incrementAndGet();
    totalQueryTime.addAndGet(executionTimeMs);

    switch (searchType.toUpperCase()) {
      case "VECTOR":
        vectorSearchCount.incrementAndGet();
        break;
      case "GRAPH":
        graphSearchCount.incrementAndGet();
        break;
      case "HYBRID":
        hybridSearchCount.incrementAndGet();
        break;
      default:
        break;
    }

    LOGGER.debug("Query recorded: {} ms, type: {}", executionTimeMs, searchType);
  }

  /**
   * Record a cache hit.
   */
  public void recordCacheHit() {
    cacheHits.incrementAndGet();
  }

  /**
   * Record a cache miss.
   */
  public void recordCacheMiss() {
    cacheMisses.incrementAndGet();
  }

  /**
   * Set current cache size.
   *
   * @param size Cache size in bytes
   */
  public void setCacheSize(long size) {
    cacheSize.set(size);
  }

  /**
   * Record episode ingestion.
   *

   * @param numEntities Number of entities in episode
   * @param numRelations Number of relations in episode
   */
  public void recordEpisodeIngestion(int numEntities, int numRelations) {
    totalEpisodes.incrementAndGet();
    totalEntities.addAndGet(numEntities);
    totalRelations.addAndGet(numRelations);
  }

  /**
   * Record query error.
   */
  public void recordQueryError() {
    queryErrors.incrementAndGet();
  }

  /**
   * Record ingestion error.
   */
  public void recordIngestionError() {
    ingestionErrors.incrementAndGet();
  }

  /**
   * Set storage size.
   *

   * @param sizeBytes Storage size in bytes
   */
  public void setStorageSize(long sizeBytes) {
    storageSizeBytes.set(sizeBytes);
  }

  /**
   * Get QPS (Queries Per Second).
   *

   * @return Current QPS
   */
  public double getQPS() {
    long queries = totalQueries.get();
    // Simplified: return queries per second assuming 1 second window
    return queries > 0 ? queries : 0;
  }

  /**
   * Get average query latency in milliseconds.
   *

   * @return Average latency
   */
  public double getAverageLatency() {
    long queries = totalQueries.get();
    if (queries == 0) {
      return 0;
    }
    return (double) totalQueryTime.get() / queries;
  }

  /**
   * Get cache hit rate.
   *

   * @return Hit rate as percentage (0-100)
   */
  public double getCacheHitRate() {
    long hits = cacheHits.get();
    long misses = cacheMisses.get();
    long total = hits + misses;

    if (total == 0) {
      return 0;
    }

    return (double) hits * 100 / total;
  }

  /**
   * Get current metrics summary.
   *

   * @return Metrics summary as string
   */
  public String getSummary() {
    return String.format(
        "Metrics{qps=%.2f, avgLatency=%.2f ms, cacheHitRate=%.2f%%, "
            + "totalQueries=%d, totalEpisodes=%d, totalEntities=%d, "
            + "cacheHits=%d, cacheMisses=%d, errors=%d}",
        getQPS(),
        getAverageLatency(),
        getCacheHitRate(),
        totalQueries.get(),
        totalEpisodes.get(),
        totalEntities.get(),
        cacheHits.get(),
        cacheMisses.get(),
        queryErrors.get() + ingestionErrors.get());
  }

  // Getters for all metrics
  public long getTotalQueries() {
    return totalQueries.get();
  }

  public long getVectorSearchCount() {
    return vectorSearchCount.get();
  }

  public long getGraphSearchCount() {
    return graphSearchCount.get();
  }

  public long getHybridSearchCount() {
    return hybridSearchCount.get();
  }

  public long getCacheHits() {
    return cacheHits.get();
  }

  public long getCacheMisses() {
    return cacheMisses.get();
  }

  public long getTotalEpisodes() {
    return totalEpisodes.get();
  }

  public long getTotalEntities() {
    return totalEntities.get();
  }

  public long getTotalRelations() {
    return totalRelations.get();
  }

  public long getQueryErrors() {
    return queryErrors.get();
  }

  public long getIngestionErrors() {
    return ingestionErrors.get();
  }

  public long getStorageSize() {
    return storageSizeBytes.get();
  }
}
