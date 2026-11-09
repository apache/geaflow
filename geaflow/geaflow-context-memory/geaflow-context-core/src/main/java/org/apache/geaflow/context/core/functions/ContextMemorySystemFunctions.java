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

package org.apache.geaflow.context.core.functions;

import org.apache.geaflow.context.api.engine.ContextMemoryEngine;
import org.apache.geaflow.context.api.query.ContextQuery;
import org.apache.geaflow.context.api.query.ContextQuery.RetrievalStrategy;
import org.apache.geaflow.context.api.result.ContextSearchResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * System functions library for Context Memory DSL.
 * Provides high-level query functions like hybrid_search, temporal_search, etc.
 */
public class ContextMemorySystemFunctions {

  private static final Logger LOGGER = LoggerFactory.getLogger(
      ContextMemorySystemFunctions.class);

  private static ContextMemoryEngine engine;

  /**
   * Initialize the system functions with engine.
   *

   * @param contextEngine The ContextMemoryEngine
   */
  public static void initialize(ContextMemoryEngine contextEngine) {
    engine = contextEngine;
    LOGGER.info("ContextMemorySystemFunctions initialized");
  }

  /**
   * Hybrid search combining vector, graph, and keyword search.
   *

   * @param query The query text
   * @param maxHops Maximum hops for graph traversal
   * @param vectorThreshold Vector similarity threshold
   * @return The search result
   * @throws Exception if search fails
   */
  public static ContextSearchResult hybridSearch(String query, int maxHops,
      double vectorThreshold) throws Exception {
    validateEngine();

    ContextQuery contextQuery = ContextQuery.builder()
        .queryText(query)
        .strategy(RetrievalStrategy.HYBRID)
        .maxHops(maxHops)
        .vectorThreshold(vectorThreshold)
        .build();

    ContextSearchResult result = engine.search(contextQuery);
    LOGGER.info("Hybrid search for '{}': {} entities, {} relations",
        query, result.getEntities().size(), result.getRelations().size());
    return result;
  }

  /**
   * Vector-only search.
   *

   * @param query The query text
   * @param vectorThreshold The similarity threshold
   * @return The search result
   * @throws Exception if search fails
   */
  public static ContextSearchResult vectorSearch(String query, double vectorThreshold)
      throws Exception {
    validateEngine();

    ContextQuery contextQuery = ContextQuery.builder()
        .queryText(query)
        .strategy(RetrievalStrategy.VECTOR_ONLY)
        .vectorThreshold(vectorThreshold)
        .build();

    return engine.search(contextQuery);
  }

  /**
   * Graph-only search.
   *

   * @param entityId The starting entity ID
   * @param maxHops Maximum hops for traversal
   * @return The search result
   * @throws Exception if search fails
   */
  public static ContextSearchResult graphSearch(String entityId, int maxHops)
      throws Exception {
    validateEngine();

    ContextQuery contextQuery = ContextQuery.builder()
        .queryText(entityId)
        .strategy(RetrievalStrategy.GRAPH_ONLY)
        .maxHops(maxHops)
        .build();

    return engine.search(contextQuery);
  }

  /**
   * Keyword-only search.
   *

   * @param keyword The keyword to search
   * @return The search result
   * @throws Exception if search fails
   */
  public static ContextSearchResult keywordSearch(String keyword) throws Exception {
    validateEngine();

    ContextQuery contextQuery = ContextQuery.builder()
        .queryText(keyword)
        .strategy(RetrievalStrategy.KEYWORD_ONLY)
        .build();

    return engine.search(contextQuery);
  }

  /**
   * Temporal search with time range.
   *

   * @param query The query text
   * @param startTime Start time in milliseconds
   * @param endTime End time in milliseconds
   * @return The search result
   * @throws Exception if search fails
   */
  public static ContextSearchResult temporalSearch(String query, long startTime, long endTime)
      throws Exception {
    validateEngine();

    ContextQuery.TemporalFilter timeFilter = new ContextQuery.TemporalFilter();
    timeFilter.setStartTime(startTime);
    timeFilter.setEndTime(endTime);

    ContextQuery contextQuery = ContextQuery.builder()
        .queryText(query)
        .strategy(RetrievalStrategy.HYBRID)
        .timeRange(timeFilter)
        .maxHops(2)
        .build();

    ContextSearchResult result = engine.search(contextQuery);
    LOGGER.info("Temporal search for '{}' ({}-{}): {} results",
        query, startTime, endTime, result.getEntities().size());
    return result;
  }

  /**
   * Calculate relevance score for an entity.
   *

   * @param entityId The entity ID
   * @param queryVector The query vector
   * @return The relevance score
   */
  public static double calculateRelevance(String entityId, float[] queryVector) {
    if (queryVector == null || queryVector.length == 0) {
      return 0.0;
    }

    // Simplified relevance calculation
    // In production, would use actual vector similarity
    return Math.random();
  }

  /**
   * Validate that engine is initialized.
   *

   * @throws IllegalStateException if engine not initialized
   */
  private static void validateEngine() {
    if (engine == null) {
      throw new IllegalStateException("ContextMemoryEngine not initialized");
    }
  }
}
