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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
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

    /**
     * Validates a plan and repairs the recoverable violations instead of rejecting them.
     *
     * <p>Structural problems (unknown channels, unknown upper-bound keys, all-zero weights,
     * unresolvable budgets) remain typed rejects. Availability, hop, neighbor-cap and budget
     * overflow problems are clamped, and every clamp is recorded so the trace can replay the
     * decision.</p>
     */
    public static Result validateAndAdjust(RetrievalPlan plan, RoutingContext context) {
        if (plan == null || context == null) {
            throw invalid("routing plan and context are required");
        }
        if (plan.getSelectedMode() == null || plan.getExecutionMode() == null) {
            throw invalid("routing plan mode is required");
        }
        validateStructural(plan, context);
        List<PlanAdjustment> adjustments = new ArrayList<>();
        RetrievalPlan adjusted = clampToBudget(clampToContext(plan, context, adjustments), context, adjustments);
        adjusted = clampChannels(adjusted, context, adjustments);
        validate(adjusted, context);
        return new Result(adjusted, adjustments);
    }

    private static void validateStructural(RetrievalPlan plan, RoutingContext context) {
        if (plan.getTopK() < 1 || plan.getCandidateBudget() < 1 || plan.getTokenBudget() < 1
            || plan.getLatencyBudgetNanos() < 0L || plan.getNeighborCap() < 1 || plan.getMaxHop() < 0) {
            throw invalid("routing plan contains invalid limits");
        }
        validateWeights(plan);
        for (String channel : plan.getChannelOrder()) {
            if (!CHANNELS.contains(channel)) {
                throw invalid("routing plan contains an unknown channel: " + channel);
            }
        }
        if (!plan.getHardUpperBounds().isEmpty()) {
            for (Map.Entry<String, Integer> entry : plan.getHardUpperBounds().entrySet()) {
                if (!Arrays.asList("candidates", "tokens", "latencyNanos", "maxHop").contains(entry.getKey())) {
                    throw invalid("unknown hard upper bound: " + entry.getKey());
                }
            }
        }
        if (!plan.getChannelOrder().isEmpty()
            && !plan.getChannelBudgets().keySet().equals(new HashSet<>(plan.getChannelOrder()))) {
            throw invalid("channel budgets do not match channel order");
        }
    }

    private static RetrievalPlan clampToContext(RetrievalPlan plan, RoutingContext context,
                                                List<PlanAdjustment> adjustments) {
        RetrievalPlan.Builder builder = RetrievalPlan.builder(plan);
        boolean changed = false;
        if (plan.getMaxHop() > context.getMaxHop()) {
            builder.maxHop(context.getMaxHop());
            adjustments.add(new PlanAdjustment("maxHop", plan.getMaxHop(), context.getMaxHop(),
                PlanAdjustment.CLAMPED_MAX_HOP));
            changed = true;
        }
        if (plan.getNeighborCap() > plan.getCandidateBudget()) {
            builder.neighborCap(plan.getCandidateBudget());
            adjustments.add(new PlanAdjustment("neighborCap", plan.getNeighborCap(),
                plan.getCandidateBudget(), PlanAdjustment.CLAMPED_NEIGHBOR_CAP));
            changed = true;
        }
        return changed ? builder.build() : plan;
    }

    private static RetrievalPlan clampToBudget(RetrievalPlan plan, RoutingContext context,
                                               List<PlanAdjustment> adjustments) {
        RetrievalPlan.Builder builder = RetrievalPlan.builder(plan);
        boolean changed = false;
        int maxCandidates = context.getBudget().getMaxCandidates();
        if (plan.getCandidateBudget() > maxCandidates) {
            builder.candidateBudget(maxCandidates);
            adjustments.add(new PlanAdjustment("candidateBudget", plan.getCandidateBudget(),
                maxCandidates, PlanAdjustment.CLAMPED_HARD_UPPER_BOUND));
            changed = true;
        }
        if (plan.getTokenBudget() > context.getBudget().getTokenBudget()) {
            builder.tokenBudget(context.getBudget().getTokenBudget());
            adjustments.add(new PlanAdjustment("tokenBudget", plan.getTokenBudget(),
                context.getBudget().getTokenBudget(), PlanAdjustment.CLAMPED_HARD_UPPER_BOUND));
            changed = true;
        }
        long maxLatency = context.getBudget().getTimeoutMs().longValue() * 1_000_000L;
        if (plan.getLatencyBudgetNanos() > maxLatency) {
            builder.latencyBudgetNanos(maxLatency);
            adjustments.add(new PlanAdjustment("latencyBudgetNanos", plan.getLatencyBudgetNanos(),
                maxLatency, PlanAdjustment.CLAMPED_HARD_UPPER_BOUND));
            changed = true;
        }
        int effectiveCandidates = changed ? builder.build().getCandidateBudget() : plan.getCandidateBudget();
        if (plan.getTopK() > effectiveCandidates) {
            builder.topK(effectiveCandidates);
            adjustments.add(new PlanAdjustment("topK", plan.getTopK(), effectiveCandidates,
                PlanAdjustment.CLAMPED_TOP_K));
            changed = true;
        }
        return changed ? builder.build() : plan;
    }

    private static RetrievalPlan clampChannels(RetrievalPlan plan, RoutingContext context,
                                               List<PlanAdjustment> adjustments) {
        List<String> available = new ArrayList<>();
        for (String channel : plan.getChannelOrder()) {
            if (context.hasIndex(channel)) {
                available.add(channel);
            } else {
                adjustments.add(new PlanAdjustment("channelOrder." + channel, channel, "removed",
                    PlanAdjustment.DROPPED_UNAVAILABLE_CHANNEL));
            }
        }
        if (available.isEmpty()) {
            throw invalid("routing plan selected no available index");
        }
        RetrievalMode mode = plan.getSelectedMode();
        if (mode == RetrievalMode.HYBRID && (!available.contains("BM25") || !available.contains("VECTOR"))) {
            RetrievalMode downgraded = available.contains("BM25") ? RetrievalMode.BM25_ONLY
                : available.contains("VECTOR") ? RetrievalMode.VECTOR_ONLY : RetrievalMode.GRAPH_ONLY;
            adjustments.add(new PlanAdjustment("selectedMode", mode, downgraded,
                PlanAdjustment.DOWNGRADED_HYBRID_MODE));
            mode = downgraded;
        }
        if (mode == RetrievalMode.BM25_ONLY && !available.contains("BM25")
            || mode == RetrievalMode.VECTOR_ONLY && !available.contains("VECTOR")
            || mode == RetrievalMode.GRAPH_ONLY && !available.contains("GRAPH")) {
            throw invalid("selected mode does not match any available channel");
        }
        int candidateBudget = plan.getCandidateBudget();
        Map<String, Integer> budgets = allocate(plan.getChannelBudgets(), available, candidateBudget, adjustments);
        int maxHop = available.contains("GRAPH") ? Math.max(1, plan.getMaxHop()) : 0;
        if (maxHop == 0 && plan.getMaxHop() != 0) {
            adjustments.add(new PlanAdjustment("maxHop", plan.getMaxHop(), 0, PlanAdjustment.CLAMPED_MAX_HOP));
        }
        RetrievalPlan.Builder builder = RetrievalPlan.builder(plan).selectedMode(mode)
            .channelOrder(available).channelBudgets(budgets).maxHop(maxHop);
        if (!available.contains("GRAPH") && plan.getNeighborCap() > candidateBudget) {
            builder.neighborCap(candidateBudget);
        }
        return builder.build();
    }

    private static Map<String, Integer> allocate(Map<String, Integer> requested, List<String> available,
                                                 int candidateBudget, List<PlanAdjustment> adjustments) {
        Map<String, Integer> budgets = new LinkedHashMap<>();
        int total = 0;
        for (String channel : available) {
            Integer value = requested.get(channel);
            int budget = value == null || value < 1 ? 1 : value;
            if (value == null || value < 1) {
                adjustments.add(new PlanAdjustment("channelBudget." + channel, String.valueOf(value), 1,
                    PlanAdjustment.CLAMPED_CHANNEL_BUDGET));
            }
            budgets.put(channel, budget);
            total += budget;
        }
        while (total > candidateBudget) {
            String largest = null;
            for (Map.Entry<String, Integer> entry : budgets.entrySet()) {
                if (entry.getValue() > 1 && (largest == null
                    || entry.getValue() > budgets.get(largest))) {
                    largest = entry.getKey();
                }
            }
            if (largest == null) {
                throw invalid("candidate budget cannot fund every selected channel");
            }
            int before = budgets.get(largest);
            budgets.put(largest, before - 1);
            adjustments.add(new PlanAdjustment("channelBudget." + largest, before, before - 1,
                PlanAdjustment.CLAMPED_CHANNEL_BUDGET));
            total--;
        }
        return budgets;
    }

    /** Validated plan plus every correction applied to get there. */
    public static final class Result {
        private final RetrievalPlan plan;
        private final List<PlanAdjustment> adjustments;

        Result(RetrievalPlan plan, List<PlanAdjustment> adjustments) {
            this.plan = plan;
            this.adjustments = java.util.Collections.unmodifiableList(new ArrayList<>(adjustments));
        }

        public RetrievalPlan getPlan() {
            return plan;
        }

        public List<PlanAdjustment> getAdjustments() {
            return adjustments;
        }
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
