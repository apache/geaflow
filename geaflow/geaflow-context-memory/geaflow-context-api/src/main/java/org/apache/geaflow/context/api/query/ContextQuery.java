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

package org.apache.geaflow.context.api.query;

import java.io.Serializable;

/**
 * ContextQuery represents a hybrid retrieval query for context memory.
 * Supports vector similarity, graph traversal, and keyword-based retrieval.
 */
public class ContextQuery implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Natural language query text.
     */
    private String queryText;

    /**
     * Retrieval strategy.
     */
    private RetrievalStrategy strategy;

    /**
     * Maximum graph traversal hops.
     */
    private int maxHops;

    /**
     * Temporal filter for time-based queries.
     */
    private TemporalFilter timeRange;

    /**
     * Vector similarity threshold (0.0 - 1.0).
     */
    private double vectorThreshold;

    /**
     * Maximum number of results.
     */
    private int topK;

    /**
     * Enumeration for retrieval strategies.
     */
    public enum RetrievalStrategy {
        HYBRID,           // Combine vector and graph retrieval
        VECTOR_ONLY,      // Vector similarity search only
        GRAPH_ONLY,       // Graph traversal only
        KEYWORD_ONLY      // Keyword search only
    }

    /**
     * Default constructor.
     */
    public ContextQuery() {
        this.strategy = RetrievalStrategy.HYBRID;
        this.maxHops = 2;
        this.vectorThreshold = 0.7;
        this.topK = 10;
    }

    /**
     * Constructor with query text.
     */
    public ContextQuery(String queryText) {
        this();
        this.queryText = queryText;
    }

    /**
     * Create a new builder for ContextQuery.
     *
     * @return A new builder instance
     */
    public static Builder builder() {
        return new Builder();
    }

    // Getters and Setters
    public String getQueryText() {
        return queryText;
    }

    public void setQueryText(String queryText) {
        this.queryText = queryText;
    }

    public RetrievalStrategy getStrategy() {
        return strategy;
    }

    public void setStrategy(RetrievalStrategy strategy) {
        this.strategy = strategy;
    }

    public int getMaxHops() {
        return maxHops;
    }

    public void setMaxHops(int maxHops) {
        this.maxHops = maxHops;
    }

    public TemporalFilter getTimeRange() {
        return timeRange;
    }

    public void setTimeRange(TemporalFilter timeRange) {
        this.timeRange = timeRange;
    }

    public double getVectorThreshold() {
        return vectorThreshold;
    }

    public void setVectorThreshold(double vectorThreshold) {
        this.vectorThreshold = vectorThreshold;
    }

    public int getTopK() {
        return topK;
    }

    public void setTopK(int topK) {
        this.topK = topK;
    }

    /**
     * TemporalFilter for time-based filtering.
     */
    public static class TemporalFilter implements Serializable {

        private static final long serialVersionUID = 1L;

        private long startTime;
        private long endTime;
        private FilterType filterType;

        public enum FilterType {
            EVENT_TIME,    // Filter by event occurrence time
            INGEST_TIME    // Filter by ingestion time
        }

        public TemporalFilter() {
            this.filterType = FilterType.EVENT_TIME;
        }

        public TemporalFilter(long startTime, long endTime) {
            this();
            this.startTime = startTime;
            this.endTime = endTime;
        }

        public static TemporalFilter last30Days() {
            long endTime = System.currentTimeMillis();
            long startTime = endTime - (30L * 24 * 60 * 60 * 1000);
            return new TemporalFilter(startTime, endTime);
        }

        public static TemporalFilter last7Days() {
            long endTime = System.currentTimeMillis();
            long startTime = endTime - (7L * 24 * 60 * 60 * 1000);
            return new TemporalFilter(startTime, endTime);
        }

        public static TemporalFilter last24Hours() {
            long endTime = System.currentTimeMillis();
            long startTime = endTime - (24L * 60 * 60 * 1000);
            return new TemporalFilter(startTime, endTime);
        }

        // Getters and Setters
        public long getStartTime() {
            return startTime;
        }

        public void setStartTime(long startTime) {
            this.startTime = startTime;
        }

        public long getEndTime() {
            return endTime;
        }

        public void setEndTime(long endTime) {
            this.endTime = endTime;
        }

        public FilterType getFilterType() {
            return filterType;
        }

        public void setFilterType(FilterType filterType) {
            this.filterType = filterType;
        }
    }

    /**
     * Builder pattern for constructing ContextQuery.
     */
    public static class Builder {

        private final ContextQuery query;

        public Builder() {
            this.query = new ContextQuery();
        }

        public Builder queryText(String queryText) {
            query.queryText = queryText;
            return this;
        }

        public Builder strategy(RetrievalStrategy strategy) {
            query.strategy = strategy;
            return this;
        }

        public Builder maxHops(int maxHops) {
            query.maxHops = maxHops;
            return this;
        }

        public Builder timeRange(TemporalFilter timeRange) {
            query.timeRange = timeRange;
            return this;
        }

        public Builder vectorThreshold(double vectorThreshold) {
            query.vectorThreshold = vectorThreshold;
            return this;
        }

        public Builder topK(int topK) {
            query.topK = topK;
            return this;
        }

        public ContextQuery build() {
            return query;
        }
    }
}
