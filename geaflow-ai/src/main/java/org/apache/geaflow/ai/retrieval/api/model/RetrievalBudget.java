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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.geaflow.ai.retrieval.api.model;

import org.apache.geaflow.ai.retrieval.execution.BudgetConstraint;

/** Optional request budget and the effective budget returned in a response. */
public class RetrievalBudget {

    private Integer topK;
    private Integer timeoutMs;
    private Integer maxCandidates;
    private Integer tokenBudget;
    private Integer maxEdgeScans;
    private BudgetConstraint primaryConstraint = BudgetConstraint.CANDIDATES;

    public RetrievalBudget() {
    }

    public RetrievalBudget(Integer topK, Integer timeoutMs, Integer maxCandidates,
                           Integer tokenBudget) {
        this(topK, timeoutMs, maxCandidates, tokenBudget, BudgetConstraint.CANDIDATES);
    }

    public RetrievalBudget(Integer topK, Integer timeoutMs, Integer maxCandidates,
                           Integer tokenBudget, BudgetConstraint primaryConstraint) {
        this.topK = topK;
        this.timeoutMs = timeoutMs;
        this.maxCandidates = maxCandidates;
        this.tokenBudget = tokenBudget;
        this.primaryConstraint = primaryConstraint;
    }

    public Integer getTopK() {
        return topK;
    }

    public void setTopK(Integer topK) {
        this.topK = topK;
    }

    public Integer getTimeoutMs() {
        return timeoutMs;
    }

    public void setTimeoutMs(Integer timeoutMs) {
        this.timeoutMs = timeoutMs;
    }

    public Integer getMaxCandidates() {
        return maxCandidates;
    }

    public void setMaxCandidates(Integer maxCandidates) {
        this.maxCandidates = maxCandidates;
    }

    public Integer getTokenBudget() {
        return tokenBudget;
    }

    public void setTokenBudget(Integer tokenBudget) {
        this.tokenBudget = tokenBudget;
    }

    /**
     * Returns the independent bound on graph edges examined, or {@code null} to use the server
     * default.
     *
     * <p>Graph edge scans are the dominant implicit cost of diffusion: a request can inspect many
     * edges and still return only a handful of evidence items. Keeping this bound independent of
     * {@link #getMaxCandidates()} lets callers and benchmarks trade scan work against candidate
     * work explicitly, and lets cost reports attribute the two separately.
     */
    public Integer getMaxEdgeScans() {
        return maxEdgeScans;
    }

    public void setMaxEdgeScans(Integer maxEdgeScans) {
        this.maxEdgeScans = maxEdgeScans;
    }

    /**
     * Returns a defensive copy carrying every budget dimension.
     *
     * <p>Use this instead of constructing a new budget field by field: a partial copy silently
     * drops bounds such as {@link #getMaxEdgeScans()} and makes an explicit caller limit look like
     * an unset one further down the pipeline.
     */
    public RetrievalBudget copy() {
        RetrievalBudget copy = new RetrievalBudget(topK, timeoutMs, maxCandidates, tokenBudget,
            primaryConstraint);
        copy.maxEdgeScans = maxEdgeScans;
        return copy;
    }

    public BudgetConstraint getPrimaryConstraint() {
        return primaryConstraint;
    }

    public void setPrimaryConstraint(BudgetConstraint primaryConstraint) {
        this.primaryConstraint = primaryConstraint;
    }
}
