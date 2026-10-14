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
package org.apache.geaflow.ai.retrieval.execution;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.geaflow.ai.retrieval.api.model.ExecutionMode;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalErrorCode;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalException;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalMode;

/** Immutable bounded plan with fixed channel allocations and one optional absolute deadline. */
public final class RecallPlan {
    private final RetrievalMode mode;
    private final int topK;
    private final long deadlineNanos;
    private final Map<String, Integer> channelBudgets;
    private final String graphVersion;
    private final String indexVersion;
    private final String vectorVersion;
    private final int maxHop;
    private final int neighborCap;
    private final double maxDiffusionGrowth;

    public RecallPlan(RetrievalMode mode, int topK, int candidates, long deadlineNanos) {
        this(mode, topK, candidates, deadlineNanos, null, null, null);
    }

    public RecallPlan(RetrievalMode mode, int topK, int candidates, long deadlineNanos,
                      String graphVersion, String indexVersion, String vectorVersion) {
        this(mode, topK, candidates, deadlineNanos, graphVersion, indexVersion, vectorVersion,
            BudgetConstraint.CANDIDATES, ExecutionMode.SEQUENTIAL);
    }

    public RecallPlan(RetrievalMode mode, int topK, int candidates, long deadlineNanos,
                      String graphVersion, String indexVersion, String vectorVersion,
                      BudgetConstraint primaryConstraint, ExecutionMode executionMode) {
        this.graphVersion = graphVersion;
        this.indexVersion = indexVersion;
        this.vectorVersion = vectorVersion;
        if (mode == null || primaryConstraint == null || executionMode == null || topK < 1
            || (primaryConstraint == BudgetConstraint.CANDIDATES && topK > candidates)
            || (mode.canonical() == RetrievalMode.HYBRID
                && primaryConstraint == BudgetConstraint.CANDIDATES && candidates < 3)
            || candidates < 1) {
            throw new RetrievalException(RetrievalErrorCode.INVALID_REQUEST, "invalid recall budget or mode");
        }
        this.mode = mode.canonical();
        this.topK = topK;
        this.deadlineNanos = deadlineNanos;
        int allocatedCandidates = this.mode == RetrievalMode.HYBRID
            && primaryConstraint != BudgetConstraint.CANDIDATES ? Math.max(3, candidates) : candidates;
        Map<String, Integer> budgets = new LinkedHashMap<>();
        if (this.mode == RetrievalMode.HYBRID) {
            if (executionMode == ExecutionMode.CASCADED) {
                int probeBudget = allocatedCandidates >= 5
                    ? Math.min(10, Math.max(2, allocatedCandidates / 8))
                    : Math.max(1, (allocatedCandidates - 1) / 2);
                budgets.put("BM25", probeBudget);
                budgets.put("VECTOR", probeBudget);
                budgets.put("GRAPH", allocatedCandidates - (probeBudget * 2));
            } else {
                int base = allocatedCandidates / 3;
                int remainder = allocatedCandidates % 3;
                budgets.put("BM25", base + (remainder >= 1 ? 1 : 0));
                budgets.put("VECTOR", base + (remainder >= 2 ? 1 : 0));
                budgets.put("GRAPH", base);
            }
        } else {
            budgets.put(this.mode == RetrievalMode.BM25_ONLY ? "BM25"
                : this.mode == RetrievalMode.VECTOR_ONLY ? "VECTOR" : "GRAPH", allocatedCandidates);
        }
        channelBudgets = Collections.unmodifiableMap(budgets);
        this.maxHop = this.mode == RetrievalMode.GRAPH_ONLY ? 1 : 1;
        this.neighborCap = Math.max(1, candidates);
        this.maxDiffusionGrowth = Double.POSITIVE_INFINITY;
    }

    /** Creates a plan from an already validated adaptive channel allocation. */
    public RecallPlan(RetrievalMode mode, int topK, long deadlineNanos,
                      String graphVersion, String indexVersion, String vectorVersion,
                      BudgetConstraint primaryConstraint, ExecutionMode executionMode,
                      Map<String, Integer> channelBudgets, int maxHop, int neighborCap) {
        this(mode, topK, deadlineNanos, graphVersion, indexVersion, vectorVersion, primaryConstraint,
            executionMode, channelBudgets, maxHop, neighborCap, Double.POSITIVE_INFINITY);
    }

    public RecallPlan(RetrievalMode mode, int topK, long deadlineNanos,
                      String graphVersion, String indexVersion, String vectorVersion,
                      BudgetConstraint primaryConstraint, ExecutionMode executionMode,
                      Map<String, Integer> channelBudgets, int maxHop, int neighborCap,
                      double maxDiffusionGrowth) {
        if (mode == null || primaryConstraint == null || executionMode == null || topK < 1
            || channelBudgets == null || channelBudgets.isEmpty() || maxHop < 0 || maxHop > 2
            || neighborCap < 1 || !Double.isFinite(maxDiffusionGrowth)
            && maxDiffusionGrowth != Double.POSITIVE_INFINITY) {
            throw new RetrievalException(RetrievalErrorCode.INVALID_REQUEST, "invalid adaptive recall plan");
        }
        this.mode = mode.canonical();
        this.topK = topK;
        this.deadlineNanos = deadlineNanos;
        this.graphVersion = graphVersion;
        this.indexVersion = indexVersion;
        this.vectorVersion = vectorVersion;
        Map<String, Integer> budgets = new LinkedHashMap<>();
        int total = 0;
        for (Map.Entry<String, Integer> entry : channelBudgets.entrySet()) {
            if (entry.getKey() == null || entry.getValue() == null || entry.getValue() < 1) {
                throw new RetrievalException(RetrievalErrorCode.INVALID_REQUEST, "invalid adaptive channel budget");
            }
            total += entry.getValue();
            budgets.put(entry.getKey(), entry.getValue());
        }
        if (primaryConstraint == BudgetConstraint.CANDIDATES && topK > total) {
            throw new RetrievalException(RetrievalErrorCode.INVALID_REQUEST, "invalid adaptive topK budget");
        }
        this.channelBudgets = Collections.unmodifiableMap(budgets);
        this.maxHop = maxHop;
        this.neighborCap = neighborCap;
        this.maxDiffusionGrowth = maxDiffusionGrowth;
    }

    public String getGraphVersion() {
        return graphVersion;
    }

    public String getIndexVersion() {
        return indexVersion;
    }

    public String getVectorVersion() {
        return vectorVersion;
    }

    public RetrievalMode getMode() {
        return mode;
    }

    public int getTopK() {
        return topK;
    }

    public long getDeadlineNanos() {
        return deadlineNanos;
    }

    public Map<String, Integer> getChannelBudgets() {
        return channelBudgets;
    }

    public int getMaxHop() {
        return maxHop;
    }

    public int getNeighborCap() {
        return neighborCap;
    }

    public double getMaxDiffusionGrowth() {
        return maxDiffusionGrowth;
    }
}
