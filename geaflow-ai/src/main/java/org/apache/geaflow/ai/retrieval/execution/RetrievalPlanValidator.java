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

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalErrorCode;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalException;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalMode;

/** Validates an adaptive plan against request-local availability and hard limits. */
public final class RetrievalPlanValidator {
    private static final Set<String> CHANNELS = new HashSet<>(Arrays.asList("BM25", "VECTOR", "GRAPH"));

    private RetrievalPlanValidator() {
    }

    public static RetrievalPlan validate(RetrievalPlan plan, RoutingContext context) {
        if (plan == null || context == null) {
            throw invalid("routing plan and context are required");
        }
        if (plan.getSelectedMode() == null || plan.getExecutionMode() == null) {
            throw invalid("routing plan mode is required");
        }
        if (plan.getTopK() < 1 || plan.getCandidateBudget() < 1
            || plan.getTokenBudget() < 1 || plan.getLatencyBudgetNanos() < 0L
            || plan.getNeighborCap() < 1 || plan.getMaxHop() < 0
            || plan.getMaxHop() > context.getMaxHop()) {
            throw invalid("routing plan contains invalid limits");
        }
        if (plan.getTopK() > plan.getCandidateBudget()
            || plan.getCandidateBudget() > context.getBudget().getMaxCandidates()
            || plan.getTokenBudget() > context.getBudget().getTokenBudget()
            || plan.getLatencyBudgetNanos() > context.getBudget().getTimeoutMs().longValue() * 1_000_000L
            || plan.getNeighborCap() > context.getBudget().getMaxCandidates()) {
            throw invalid("routing plan exceeds hard budget");
        }
        validateWeights(plan);
        List<String> order = plan.getChannelOrder();
        Set<String> unique = new HashSet<>();
        for (String channel : order) {
            if (!CHANNELS.contains(channel) || !unique.add(channel)) {
                throw invalid("routing plan contains an unknown or duplicate channel");
            }
            if (!context.hasIndex(channel)) {
                throw invalid("routing plan selected unavailable index: " + channel);
            }
        }
        if (!plan.getChannelBudgets().keySet().equals(unique)) {
            throw invalid("channel budgets do not match channel order");
        }
        int sum = 0;
        for (Map.Entry<String, Integer> entry : plan.getChannelBudgets().entrySet()) {
            if (entry.getValue() == null || entry.getValue() < 1) {
                throw invalid("channel budget must be positive");
            }
            sum += entry.getValue();
        }
        if (sum > plan.getCandidateBudget() || plan.getTopK() > sum) {
            throw invalid("channel budgets exceed candidate budget");
        }
        if (plan.getSelectedMode() == RetrievalMode.HYBRID
            && (!unique.contains("BM25") || !unique.contains("VECTOR"))) {
            throw invalid("HYBRID plan requires BM25 and VECTOR channels");
        }
        if (unique.contains("GRAPH") && plan.getMaxHop() < 1
            || !unique.contains("GRAPH") && plan.getMaxHop() != 0) {
            throw invalid("graph channel and maxHop do not match");
        }
        if (plan.getSelectedMode() == RetrievalMode.BM25_ONLY && !only(unique, "BM25")
            || plan.getSelectedMode() == RetrievalMode.VECTOR_ONLY && !only(unique, "VECTOR")
            || plan.getSelectedMode() == RetrievalMode.GRAPH_ONLY && !only(unique, "GRAPH")) {
            throw invalid("selected mode does not match channels");
        }
        if (!plan.getHardUpperBounds().isEmpty()) {
            validateUpperBounds(plan, context);
        }
        validateFallback(plan, context);
        return plan;
    }

    private static void validateFallback(RetrievalPlan plan, RoutingContext context) {
        RetrievalMode fallback = plan.getFallbackMode();
        if (fallback == null) {
            return;
        }
        boolean available;
        if (fallback == RetrievalMode.HYBRID) {
            available = context.hasIndex("BM25") && context.hasIndex("VECTOR");
        } else if (fallback == RetrievalMode.BM25_ONLY) {
            available = context.hasIndex("BM25");
        } else if (fallback == RetrievalMode.VECTOR_ONLY) {
            available = context.hasIndex("VECTOR");
        } else {
            available = context.hasIndex("GRAPH");
        }
        if (!available) {
            throw invalid("fallback index is unavailable");
        }
    }

    private static boolean only(Set<String> values, String channel) {
        return values.size() == 1 && values.contains(channel);
    }

    private static void validateWeights(RetrievalPlan plan) {
        if (!finiteNonNegative(plan.getBm25Weight()) || !finiteNonNegative(plan.getVectorWeight())
            || !finiteNonNegative(plan.getGraphWeight())
            || (!Double.isFinite(plan.getMaxDiffusionGrowth())
                && plan.getMaxDiffusionGrowth() != Double.POSITIVE_INFINITY)
            || plan.getBm25Weight() + plan.getVectorWeight() + plan.getGraphWeight() == 0.0D) {
            throw invalid("routing plan weights must be finite and non-negative");
        }
    }

    private static boolean finiteNonNegative(double value) {
        return Double.isFinite(value) && value >= 0.0D;
    }

    private static void validateUpperBounds(RetrievalPlan plan, RoutingContext context) {
        for (Map.Entry<String, Integer> entry : plan.getHardUpperBounds().entrySet()) {
            if (entry.getValue() == null || entry.getValue() < 1) {
                throw invalid("hard upper bound must be positive");
            }
            String key = entry.getKey();
            if (!Arrays.asList("candidates", "tokens", "latencyNanos", "maxHop").contains(key)) {
                throw invalid("unknown hard upper bound: " + key);
            }
            if ("candidates".equals(key) && entry.getValue() > context.getBudget().getMaxCandidates()
                || "tokens".equals(key) && entry.getValue() > context.getBudget().getTokenBudget()
                || "latencyNanos".equals(key)
                    && entry.getValue() > context.getBudget().getTimeoutMs().longValue() * 1_000_000L
                || "maxHop".equals(key) && entry.getValue() > context.getMaxHop()) {
                throw invalid("hard upper bound exceeds request limit: " + key);
            }
            if ("candidates".equals(key) && plan.getCandidateBudget() > entry.getValue()
                || "tokens".equals(key) && plan.getTokenBudget() > entry.getValue()
                || "latencyNanos".equals(key) && plan.getLatencyBudgetNanos() > entry.getValue()
                || "maxHop".equals(key) && plan.getMaxHop() > entry.getValue()) {
                throw invalid("routing plan exceeds hard upper bound: " + key);
            }
        }
    }

    private static RetrievalException invalid(String message) {
        return new RetrievalException(RetrievalErrorCode.INVALID_REQUEST, message);
    }
}
