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
    void policyDisablesGraphForWeakAnchorsOrFastGrowthAndRejectsMissingIndexes() {
        DeterministicRulePolicy policy = new DeterministicRulePolicy();
        RoutingFeatures weak = RoutingFeatures.builder().anchorCount(1).anchorConfidence(0.5).build();
        RetrievalPlan weakPlan = policy.plan(context(30, weak));
        assertFalse(weakPlan.getChannelBudgets().containsKey("GRAPH"));
        assertEquals(0, weakPlan.getMaxHop());

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
