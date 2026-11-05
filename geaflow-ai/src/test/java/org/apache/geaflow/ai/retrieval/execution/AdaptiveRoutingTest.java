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
        // A confident anchor means the graph channel has a seed to expand, so it leads the fusion
        // regardless of how rare the query terms are. Rare terms alone do not mean BM25 matched
        // anything: on the measured runs the IDF median is 3.0 while BM25 recall stays at 0.002.
        RoutingContext anchored = context(100, RoutingFeatures.builder().averageIdf(1.5)
            .anchorCount(1).anchorConfidence(0.8).build());
        RetrievalPlan graphLed = policy.plan(anchored);
        assertEquals(Arrays.asList("BM25", "VECTOR", "GRAPH"), graphLed.getChannelOrder());
        // The text probe may take at most GRAPH_PROBE_SHARE of a 100-candidate budget, so the graph
        // channel keeps 60 instead of the 80 an unconditional text reservation produced.
        assertEquals(20, graphLed.getChannelBudgets().get("BM25"));
        assertEquals(20, graphLed.getChannelBudgets().get("VECTOR"));
        assertEquals(60, graphLed.getChannelBudgets().get("GRAPH"));
        assertEquals(0.25, graphLed.getBm25Weight());
        assertEquals(2.0, graphLed.getGraphWeight());
        assertEquals("GRAPH_DOMINANT_FUSION", graphLed.getReasons().get(0));

        // Without an anchor the text channels take over; the lexical branch needs rare terms.
        RoutingContext unanchored = context(100, RoutingFeatures.builder().averageIdf(1.5)
            .anchorCount(0).build());
        RetrievalPlan lexical = policy.plan(unanchored);
        assertTrue(lexical.getReasons().contains("LEXICAL_CONFIDENCE"), lexical.getReasons().toString());
        assertEquals(1.25, lexical.getBm25Weight());
        assertEquals(1.0, lexical.getGraphWeight());

        RoutingContext semanticContext = context(100, RoutingFeatures.builder().queryLength(8)
            .anchorCount(0).build());
        RetrievalPlan semantic = policy.plan(semanticContext);
        assertEquals("VECTOR", semantic.getChannelOrder().get(0));
        assertEquals(1.25, semantic.getVectorWeight());
        // Without a confident anchor the planner keeps expansion to a single hop.
        assertEquals(1, semantic.getMaxHop());
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
        RetrievalPlan fastPlan = policy.replan(context(30, fast), RetrievalProgress.builder()
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

        RetrievalPlan replanned = policy.replan(context, RetrievalProgress.builder().boundary("AFTER_TEXT")
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
        RetrievalPlan plan = policy.replan(context, exhausted);

        assertTrue(plan.getReasons().contains("REPLAN_BUDGET_EXHAUSTED"));
        assertEquals(100, sum(plan.getChannelBudgets()));
        // The exhausted boundary ignores late progress signals so repeated replans cannot drift.
        RetrievalPlan second = policy.replan(context, RetrievalProgress.builder().boundary("AFTER_TEXT")
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

    @Test
    void graphChannelKeepsTheLargestShareOfEveryHybridBudget() {
        // The measured end-to-end runs show text channels carry almost no gold signal while graph
        // expansion supplies it. A routing plan that hands the graph channel less budget than a
        // text channel therefore cannot reach graph-only quality, so this pins the invariant.
        DeterministicRulePolicy policy = new DeterministicRulePolicy();
        for (int candidates : new int[] {10, 50, 100, 400, 2000}) {
            RetrievalPlan plan = policy.plan(context(candidates, RoutingFeatures.builder()
                .averageIdf(0.5).queryLength(3).anchorCount(1).anchorConfidence(0.9)
                .candidateDegree(4).build()));
            Map<String, Integer> budgets = plan.getChannelBudgets();
            int graph = budgets.get("GRAPH");
            for (Map.Entry<String, Integer> channel : budgets.entrySet()) {
                if ("GRAPH".equals(channel.getKey())) {
                    continue;
                }
                assertTrue(graph >= channel.getValue(),
                    "graph budget " + graph + " must not be below " + channel.getKey() + " "
                        + channel.getValue() + " at total " + candidates);
            }
            assertEquals(candidates, sum(budgets), "budgets must sum to the candidate budget");
        }
    }

    @Test
    void textProbeShareIsBoundedAndLeavesTheRemainderToGraph() {
        DeterministicRulePolicy policy = new DeterministicRulePolicy();
        RetrievalPlan plan = policy.plan(context(100, RoutingFeatures.builder()
            .averageIdf(0.5).queryLength(3).anchorCount(1).anchorConfidence(0.9)
            .candidateDegree(4).build()));
        int textTotal = 0;
        for (Map.Entry<String, Integer> channel : plan.getChannelBudgets().entrySet()) {
            if (!"GRAPH".equals(channel.getKey())) {
                textTotal += channel.getValue();
            }
        }
        assertTrue(textTotal <= (int) Math.floor(100 * DeterministicRulePolicy.GRAPH_PROBE_SHARE),
            "text probe took " + textTotal + ", above the configured share");
        assertEquals(100 - textTotal, plan.getChannelBudgets().get("GRAPH").intValue());
    }

    @Test
    void weakTextSignalRoutesToGraphDominantFusion() {
        // No lexical confidence and a short query is the case the measured runs expose: neither text
        // channel has signal, so the graph channel must lead both the fusion weights and the order.
        DeterministicRulePolicy policy = new DeterministicRulePolicy();
        RetrievalPlan plan = policy.plan(context(100, RoutingFeatures.builder()
            .averageIdf(0.5).queryLength(3).anchorCount(1).anchorConfidence(0.9)
            .candidateDegree(4).build()));

        assertEquals("GRAPH_DOMINANT_FUSION", plan.getReasons().get(0));
        assertTrue(plan.getGraphWeight() > plan.getBm25Weight(),
            "graph weight must exceed BM25 weight without lexical confidence");
        assertTrue(plan.getGraphWeight() > plan.getVectorWeight(),
            "graph weight must exceed vector weight without lexical confidence");
        assertEquals(Arrays.asList("BM25", "VECTOR", "GRAPH"), plan.getChannelOrder());
    }

    @Test
    void anchoredQueryLetsGraphLeadEvenWhenTheQueryHasRareTerms() {
        // The measured failure mode: a rare-term query that also resolves a confident anchor used to
        // be routed to BM25-first fusion, demoting the only channel that supplied evidence.
        DeterministicRulePolicy policy = new DeterministicRulePolicy();
        RetrievalPlan plan = policy.plan(context(100, RoutingFeatures.builder()
            .averageIdf(2.0).anchorCount(1).anchorConfidence(0.9).candidateDegree(4).build()));

        assertEquals("GRAPH_DOMINANT_FUSION", plan.getReasons().get(0));
        assertTrue(plan.getGraphWeight() > plan.getBm25Weight(),
            "an anchored query must not demote the graph channel");
        // The graph channel also keeps the largest candidate budget.
        assertTrue(plan.getChannelBudgets().get("GRAPH") > plan.getChannelBudgets().get("BM25"));
    }

    @Test
    void weakAnchorFallsBackToTextChannels() {
        // Without a confident anchor there is no seed to expand, so BM25 may lead again.
        DeterministicRulePolicy policy = new DeterministicRulePolicy();
        RetrievalPlan plan = policy.plan(context(100, RoutingFeatures.builder()
            .averageIdf(2.0).anchorCount(1).anchorConfidence(0.2).candidateDegree(4).build()));

        assertTrue(plan.getReasons().contains("LEXICAL_CONFIDENCE"), plan.getReasons().toString());
        assertTrue(plan.getBm25Weight() > plan.getGraphWeight());
    }

    @Test
    void configuredFusionWeightsReachThePlanAndTheTraceParameters() {
        // The weights were constants on the policy class, so a benchmark could not pin or vary them.
        DeterministicRulePolicy policy = new DeterministicRulePolicy(1.5D, 0.0005D, 12, 0.75D, 1, 32,
            2.0D, 16, 3.5D, 0.1D, 0.5D, 1.1D, 1.2D, 1.3D, 1.4D);

        RetrievalPlan plan = policy.plan(context(100, RoutingFeatures.builder()
            .anchorCount(1).anchorConfidence(0.9).candidateDegree(4).build()));

        assertEquals(3.5D, plan.getGraphWeight());
        assertEquals(0.1D, plan.getBm25Weight());
        assertEquals(0.1D, plan.getVectorWeight());
        Map<String, String> parameters = policy.getParameters();
        assertEquals("3.5", parameters.get("graphFusionGraphWeight"));
        assertEquals("0.1", parameters.get("graphFusionTextWeight"));
        assertEquals("0.5", parameters.get("graphProbeShare"));
        assertEquals("1.1", parameters.get("lexicalBm25Weight"));
        assertEquals("1.4", parameters.get("semanticVectorWeight"));
    }

    @Test
    void configuredProbeShareDrivesTheGraphBudget() {
        DeterministicRulePolicy policy = new DeterministicRulePolicy(1.5D, 0.0005D, 12, 0.75D, 1, 32,
            2.0D, 16, 2.0D, 0.25D, 0.8D, 1.25D, 0.85D, 0.85D, 1.25D);

        RetrievalPlan plan = policy.plan(context(100, RoutingFeatures.builder()
            .anchorCount(1).anchorConfidence(0.9).candidateDegree(4).build()));

        // An 80% probe share leaves the graph channel 20 candidates, and the text probe splits 80.
        assertEquals(20, plan.getChannelBudgets().get("GRAPH"));
        assertEquals(40, plan.getChannelBudgets().get("BM25"));
        assertEquals(40, plan.getChannelBudgets().get("VECTOR"));
    }

    @Test
    void rejectsInvalidFusionWeights() {
        assertThrows(IllegalArgumentException.class, () -> new DeterministicRulePolicy(1.5D, 0.0005D,
            12, 0.75D, 1, 32, 2.0D, 16, -1.0D, 0.25D, 0.4D, 1.25D, 0.85D, 0.85D, 1.25D));
        assertThrows(IllegalArgumentException.class, () -> new DeterministicRulePolicy(1.5D, 0.0005D,
            12, 0.75D, 1, 32, 2.0D, 16, 0.0D, 0.0D, 0.4D, 1.25D, 0.85D, 0.85D, 1.25D));
        assertThrows(IllegalArgumentException.class, () -> new DeterministicRulePolicy(1.5D, 0.0005D,
            12, 0.75D, 1, 32, 2.0D, 16, 2.0D, 0.25D, 1.0D, 1.25D, 0.85D, 0.85D, 1.25D));
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
