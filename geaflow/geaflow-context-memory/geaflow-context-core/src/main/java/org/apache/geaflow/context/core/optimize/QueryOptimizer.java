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

package org.apache.geaflow.context.core.optimize;

import org.apache.geaflow.context.api.query.ContextQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Query plan optimizer for Context Memory.
 * Implements early stopping strategy, index pushdown, and parallelization.
 */
public class QueryOptimizer {

  private static final Logger LOGGER = LoggerFactory.getLogger(QueryOptimizer.class);

  /**
   * Optimized query plan.
   */
  public static class QueryPlan {

    private final ContextQuery originalQuery;
    private String executionStrategy; // VECTOR_FIRST, GRAPH_FIRST, PARALLEL
    private int maxHops; // Optimized max hops
    private double vectorThreshold; // Optimized threshold
    private boolean enableEarlyStopping;
    private boolean enableIndexPushdown;
    private boolean enableParallel;
    private long estimatedTimeMs;

    public QueryPlan(ContextQuery query) {
      this.originalQuery = query;
      this.executionStrategy = "HYBRID";
      this.maxHops = query.getMaxHops();
      this.vectorThreshold = query.getVectorThreshold();
      this.enableEarlyStopping = true;
      this.enableIndexPushdown = true;
      this.enableParallel = true;
      this.estimatedTimeMs = 0;
    }

    // Getters and Setters
    public String getExecutionStrategy() {
      return executionStrategy;
    }

    public void setExecutionStrategy(String strategy) {
      this.executionStrategy = strategy;
    }

    public int getMaxHops() {
      return maxHops;
    }

    public void setMaxHops(int maxHops) {
      this.maxHops = maxHops;
    }

    public double getVectorThreshold() {
      return vectorThreshold;
    }

    public void setVectorThreshold(double threshold) {
      this.vectorThreshold = threshold;
    }

    public boolean isEarlyStoppingEnabled() {
      return enableEarlyStopping;
    }

    public void setEarlyStopping(boolean enabled) {
      this.enableEarlyStopping = enabled;
    }

    public boolean isIndexPushdownEnabled() {
      return enableIndexPushdown;
    }

    public void setIndexPushdown(boolean enabled) {
      this.enableIndexPushdown = enabled;
    }

    public boolean isParallelEnabled() {
      return enableParallel;
    }

    public void setParallel(boolean enabled) {
      this.enableParallel = enabled;
    }

    public long getEstimatedTimeMs() {
      return estimatedTimeMs;
    }

    public void setEstimatedTimeMs(long timeMs) {
      this.estimatedTimeMs = timeMs;
    }

    @Override
    public String toString() {
      return String.format(
          "QueryPlan{strategy=%s, maxHops=%d, threshold=%.2f, "
              + "earlyStopping=%b, indexPushdown=%b, parallel=%b, estimatedTime=%d ms}",
          executionStrategy, maxHops, vectorThreshold, enableEarlyStopping, enableIndexPushdown,
          enableParallel, estimatedTimeMs);
    }
  }

  /**
   * Optimize a query plan.
   *

   * @param query The original query
   * @return Optimized query plan
   */
  public QueryPlan optimizeQuery(ContextQuery query) {
    QueryPlan plan = new QueryPlan(query);

    // Strategy 1: Early Stopping
    // If vector threshold is high, we can stop early with fewer hops
    if (query.getVectorThreshold() >= 0.85) {
      plan.setMaxHops(Math.max(1, query.getMaxHops() - 1));
      LOGGER.debug("Applied early stopping: reduced hops from {} to {}", 
          query.getMaxHops(), plan.getMaxHops());
    }

    // Strategy 2: Index Pushdown
    // Push vector filtering before graph traversal
    if ("HYBRID".equals(query.getStrategy().toString())) {
      plan.setExecutionStrategy("VECTOR_FIRST_GRAPH_SECOND");
      LOGGER.debug("Applied index pushdown: vector filtering first");
    }

    // Strategy 3: Parallelization
    // Enable parallel execution for large result sets
    plan.setParallel(true);
    LOGGER.debug("Enabled parallelization for query execution");

    // Estimate execution time based on optimizations
    long baseTime = 50; // Base time in ms
    if (plan.isEarlyStoppingEnabled()) {
      baseTime -= 10; // Save 10ms with early stopping
    }
    if (plan.isIndexPushdownEnabled()) {
      baseTime -= 15; // Save 15ms with index pushdown
    }
    plan.setEstimatedTimeMs(Math.max(10, baseTime));

    LOGGER.info("Optimized query plan: {}", plan);
    return plan;
  }

  /**
   * Estimate query cost based on characteristics.
   *

   * @param query The query to estimate
   * @return Estimated cost in milliseconds
   */
  public long estimateQueryCost(ContextQuery query) {
    long baseCost = 50;

    // Vector search cost
    baseCost += 20;

    // Graph traversal cost proportional to max hops
    baseCost += query.getMaxHops() * 15;

    // Large threshold reduction cost
    if (query.getVectorThreshold() < 0.5) {
      baseCost += 20;
    }

    return baseCost;
  }
}
