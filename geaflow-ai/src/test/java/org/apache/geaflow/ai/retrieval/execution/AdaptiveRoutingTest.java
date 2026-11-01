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
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import org.apache.geaflow.ai.retrieval.api.model.ExecutionMode;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalBudget;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalException;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalMode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AdaptiveRoutingTest {

    @Test
    void routingFeaturesUseStableSchemaAndRejectInvalidValues() {
        RoutingFeatures features = RoutingFeatures.builder().queryLength(5).averageIdf(2.0)
            .firstStageScoreGap(0.3).anchorCount(2).anchorConfidence(0.8)
            .candidateDegree(7).graphGrowthRate(1.0).build();

        assertEquals("routing-features-v1", features.getSchemaVersion());
        assertEquals(Arrays.asList("query_length", "average_idf", "first_stage_score_gap",
            "anchor_count", "anchor_confidence", "candidate_degree", "graph_growth_rate"),
            Arrays.asList(features.asMap().keySet().toArray(new String[0])));
        assertEquals(2.0, features.get("average_idf"));
        assertThrows(UnsupportedOperationException.class, () -> features.asMap().put("x", 1));
        assertThrows(IllegalArgumentException.class,
            () -> RoutingFeatures.builder().averageIdf(Double.NaN).build());
        assertThrows(IllegalArgumentException.class,
            () -> RoutingFeatures.builder().anchorConfidence(1.1).build());
    }

    @Test
    void rulePolicyPrefersLexicalAndSemanticChannelsAtThresholds() {
        DeterministicRulePolicy policy = new DeterministicRulePolicy(1.5, 0.2, 8,
            0.75, 1, 32, 2.0, 16);
        RoutingContext base = context(100, RoutingFeatures.builder().averageIdf(1.5)
            .anchorCount(1).anchorConfidence(0.8).build());
        RetrievalPlan lexical = policy.plan(base);
        assertEquals(Arrays.asList("BM25", "VECTOR", "GRAPH"), lexical.getChannelOrder());
        assertEquals(10, lexical.getChannelBudgets().get("BM25"));
        assertEquals(10, lexical.getChannelBudgets().get("VECTOR"));
        assertEquals(80, lexical.getChannelBudgets().get("GRAPH"));
        assertEquals(1.25, lexical.getBm25Weight());
        assertEquals("LEXICAL_CONFIDENCE", lexical.getReasons().get(0));

        RoutingContext semanticContext = context(100, RoutingFeatures.builder().queryLength(8)
            .anchorCount(1).anchorConfidence(0.8).build());
        RetrievalPlan semantic = policy.plan(semanticContext);
        assertEquals("VECTOR", semantic.getChannelOrder().get(0));
        assertEquals(1.25, semantic.getVectorWeight());
        assertEquals(2, semantic.getMaxHop());
        assertEquals(100, sum(semantic.getChannelBudgets()));
        assertEquals(semantic.getChannelBudgets(), policy.plan(semanticContext).getChannelBudgets());
    }

    @Test
    void policyKeepsBoundedGraphFallbackForWeakAnchorsAndStopsOnFastGrowth() {
        DeterministicRulePolicy policy = new DeterministicRulePolicy();
        RoutingFeatures weak = RoutingFeatures.builder().anchorCount(1).anchorConfidence(0.5).build();
        RetrievalPlan weakPlan = policy.plan(context(30, weak));
        assertTrue(weakPlan.getChannelBudgets().containsKey("GRAPH"));
        assertEquals(1, weakPlan.getMaxHop());
        assertEquals(30, sum(weakPlan.getChannelBudgets()));
        assertTrue(weakPlan.getReasons().contains("GRAPH_BOUNDED_ANCHOR_FALLBACK"));

        RoutingFeatures fast = RoutingFeatures.builder().anchorCount(2).anchorConfidence(0.9)
            .graphGrowthRate(2.1).build();
        RetrievalPlan fastPlan = policy.plan(context(30, fast), RetrievalProgress.builder()
            .anchorCount(2).anchorConfidence(0.9).graphGrowthRate(2.1).build());
        assertFalse(fastPlan.getChannelBudgets().containsKey("GRAPH"));
        assertTrue(fastPlan.getReasons().contains("GRAPH_STOPPED_GROWTH"));

        RoutingContext missing = RoutingContext.builder().query("query")
            .availableIndexes(Collections.emptySet()).budget(budget(30))
            .requestedMode(RetrievalMode.HYBRID).build();
        assertThrows(IllegalArgumentException.class, () -> policy.plan(missing));
    }

    @Test
    void afterTextReplanMovesRemainingCandidateAllocationToGraph() {
        DeterministicRulePolicy policy = new DeterministicRulePolicy();
        RoutingContext context = context(100, RoutingFeatures.empty());

        RetrievalPlan replanned = policy.plan(context, RetrievalProgress.builder().boundary("AFTER_TEXT")
            .candidateCounts(Collections.singletonMap("BM25", 34)).anchorCount(0).build());

        assertEquals(98, replanned.getChannelBudgets().get("GRAPH"));
        assertEquals(1, replanned.getChannelBudgets().get("BM25"));
        assertEquals(1, replanned.getChannelBudgets().get("VECTOR"));
        assertEquals(100, sum(replanned.getChannelBudgets()));
        assertEquals(replanned, RetrievalPlanValidator.validate(replanned, context));
    }

    @Test
    void planValidatorRejectsUnavailableChannelsAndBudgetOverflow() {
        RoutingContext context = context(30, RoutingFeatures.empty());
        RetrievalPlan valid = new DeterministicRulePolicy().plan(context);
        assertEquals(valid, RetrievalPlanValidator.validate(valid, context));

        RetrievalPlan unavailable = RetrievalPlan.builder(valid).channelBudget("GRAPH", 1)
            .channelOrder(Arrays.asList("BM25", "VECTOR", "GRAPH", "GRAPH")).build();
        assertThrows(RetrievalException.class,
            () -> RetrievalPlanValidator.validate(unavailable, context));

        RetrievalPlan overBudget = RetrievalPlan.builder(valid).candidateBudget(10).build();
        assertThrows(RetrievalException.class,
            () -> RetrievalPlanValidator.validate(overBudget, context));
    }

    @Test
    void contextAndProgressDefensivelyCopyCollections() {
        LinkedHashSet<String> indexes = new LinkedHashSet<>(Arrays.asList("BM25", "VECTOR", "GRAPH"));
        RoutingContext context = RoutingContext.builder().query("query").availableIndexes(indexes)
            .budget(budget(30)).build();
        indexes.clear();
        assertEquals(3, context.getAvailableIndexes().size());
        assertThrows(UnsupportedOperationException.class, () -> context.getAvailableIndexes().clear());

        Map<String, Integer> counts = Collections.singletonMap("BM25", 1);
        RetrievalProgress progress = RetrievalProgress.builder().candidateCounts(counts).build();
        assertEquals(1, progress.getCandidateCounts().get("BM25"));
        assertThrows(UnsupportedOperationException.class, () -> progress.getCandidateCounts().clear());
    }

    @Test
    void rulePolicyKeepsDecisionStableOnceReplanBudgetIsExhausted() {
        DeterministicRulePolicy policy = new DeterministicRulePolicy();
        RoutingContext context = RoutingContext.builder().query("deterministic query")
            .features(RoutingFeatures.empty())
            .availableIndexes(Arrays.asList("BM25", "VECTOR", "GRAPH"))
            .requestedMode(RetrievalMode.HYBRID).executionMode(ExecutionMode.SEQUENTIAL)
            .maxHop(2).maxReplanCount(1).budget(budget(100)).configVersion("cfg-v1").seed(7L).build();

        RetrievalProgress exhausted = RetrievalProgress.builder().boundary("AFTER_TEXT")
            .anchorCount(0).replanCount(1).build();
        RetrievalPlan plan = policy.plan(context, exhausted);

        assertTrue(plan.getReasons().contains("REPLAN_BUDGET_EXHAUSTED"));
        assertEquals(100, sum(plan.getChannelBudgets()));
        // The exhausted boundary ignores late progress signals so repeated replans cannot drift.
        RetrievalPlan second = policy.plan(context, RetrievalProgress.builder().boundary("AFTER_TEXT")
            .firstStageScoreGap(0.9).anchorCount(9).anchorConfidence(0.99)
            .graphGrowthRate(5.0).replanCount(1).build());
        assertEquals(plan.getChannelBudgets(), second.getChannelBudgets());
        assertEquals(plan.getMaxHop(), second.getMaxHop());
    }

    @Test
    void planAdjustmentReportsFieldTransitionsWithReasons() {
        PlanAdjustment adjustment = new PlanAdjustment("topK", 200, 100, PlanAdjustment.CLAMPED_TOP_K);

        assertEquals("topK", adjustment.getField());
        assertEquals("200", adjustment.getFrom());
        assertEquals("100", adjustment.getTo());
        assertEquals(adjustment, new PlanAdjustment("topK", "200", "100", PlanAdjustment.CLAMPED_TOP_K));
        assertEquals("topK:200->100(CLAMPED_TOP_K)", adjustment.toString());
        assertThrows(IllegalArgumentException.class,
            () -> new PlanAdjustment(" ", 1, 2, "reason"));
        assertThrows(IllegalArgumentException.class,
            () -> new PlanAdjustment("topK", 1, 2, " "));
    }

    @Test
    void validateAndAdjustClampsOverBudgetPlanAndReportsEveryChange() {
        RoutingContext context = RoutingContext.builder().query("deterministic query")
            .features(RoutingFeatures.empty())
            .availableIndexes(Arrays.asList("BM25", "VECTOR", "GRAPH"))
            .requestedMode(RetrievalMode.HYBRID).executionMode(ExecutionMode.SEQUENTIAL)
            .maxHop(2).maxReplanCount(1).budget(budget(30)).configVersion("cfg-v1").seed(7L).build();
        RetrievalPlan valid = new DeterministicRulePolicy().plan(context);
        RetrievalPlan drifted = RetrievalPlan.builder(valid).candidateBudget(300)
            .tokenBudget(10_000_000).channelBudget("BM25", 200)
            .channelBudget("VECTOR", 200).channelBudget("GRAPH", 200).build();

        RetrievalPlanValidator.Result result = RetrievalPlanValidator.validateAndAdjust(drifted, context);

        assertEquals(30, (int) result.getPlan().getCandidateBudget());
        assertEquals(30, sum(result.getPlan().getChannelBudgets()));
        assertFalse(result.getAdjustments().isEmpty());
        assertTrue(result.getAdjustments().toString().contains(PlanAdjustment.CLAMPED_CHANNEL_BUDGET)
            || result.getAdjustments().toString().contains(PlanAdjustment.CLAMPED_HARD_UPPER_BOUND));
        assertEquals(result.getPlan(), RetrievalPlanValidator.validate(result.getPlan(), context));
    }

    @Test
    void validateAndAdjustStillRejectsStructuralErrors() {
        RoutingContext context = context(30, RoutingFeatures.empty());
        RetrievalPlan valid = new DeterministicRulePolicy().plan(context);
        RetrievalPlan unavailable = RetrievalPlan.builder(valid).channelBudget("GRAPH", 1)
            .channelOrder(Arrays.asList("BM25", "VECTOR", "GRAPH", "GRAPH")).build();

        assertThrows(RetrievalException.class,
            () -> RetrievalPlanValidator.validateAndAdjust(unavailable, context));
    }

    private static RoutingContext context(int candidates, RoutingFeatures features) {
        return RoutingContext.builder().query("deterministic query").features(features)
            .availableIndexes(Arrays.asList("BM25", "VECTOR", "GRAPH"))
            .requestedMode(RetrievalMode.HYBRID).executionMode(ExecutionMode.SEQUENTIAL)
            .maxHop(2).budget(budget(candidates)).configVersion("cfg-v1").seed(7L).build();
    }

    private static RetrievalBudget budget(int candidates) {
        return new RetrievalBudget(5, 1000, candidates, 1000, BudgetConstraint.CANDIDATES);
    }

    private static int sum(Map<String, Integer> values) {
        return values.values().stream().mapToInt(Integer::intValue).sum();
    }
}
