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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.LinkedHashSet;
import org.apache.geaflow.ai.retrieval.api.model.ExecutionMode;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalBudget;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalMode;
import org.junit.jupiter.api.Test;

/**
 * Pins the graph-feedback loop of the adaptive policy.
 *
 * <p>The design makes {@code graphGrowthRate} and {@code graphMarginalYield} routing inputs, and
 * lists "growth rate exceeds the diffusion limit" and "marginal yield below its threshold" as
 * stopping conditions. Those features only exist after a hop has run, so before hop-splitting the
 * policy could never observe them and both rules were unreachable. These tests assert the loop is
 * real: a measured hop changes the next plan.
 */
class AdaptiveGraphFeedbackTest {

    @Test
    void initialPlanCannotUseGrowthBecauseNothingHasExpandedYet() {
        // Guards the reason the split exists. With no measurement the growth feature is zero and the
        // policy must not read that as "diffusion is calm" for any purpose other than allowing the
        // first hop, which is what the trace's UNMEASURED reason codes record.
        DeterministicRulePolicy policy = new DeterministicRulePolicy();
        RoutingContext context = context();

        RetrievalPlan plan = policy.plan(context);

        assertEquals(2, plan.getMaxHop(), "an unmeasured frontier may still be planned for two hops");
        assertFalse(plan.getReasons().contains("GRAPH_STOPPED_GROWTH"));
    }

    @Test
    void measuredGrowthAboveTheLimitStopsExpansion() {
        DeterministicRulePolicy policy = new DeterministicRulePolicy();
        RoutingContext context = context();

        RetrievalPlan replanned = policy.replan(context, progress(5.0D, 0.9D, 1));

        assertFalse(replanned.getChannelBudgets().containsKey("GRAPH"),
            "a frontier that grew past the diffusion limit must not be expanded again");
        assertTrue(replanned.getReasons().contains("GRAPH_STOPPED_GROWTH"));
    }

    @Test
    void marginalYieldBelowTheThresholdStopsExpansion() {
        // minMarginalYield is only reachable through the progress snapshot, so a policy configured
        // with a positive threshold must stop once the first hop shows low evidence per edge.
        DeterministicRulePolicy policy = new DeterministicRulePolicy(1.5D, 0.0005D, 12, 0.75D, 1, 32,
            2.0D, 0.5D, 16);

        RetrievalPlan replanned = policy.replan(context(), progress(0.1D, 0.01D, 1));

        assertTrue(replanned.getReasons().contains("MARGINAL_YIELD_BELOW_THRESHOLD"));
        assertEquals(1, replanned.getMaxHop(),
            "a low-yield first hop must downgrade the plan to a single hop");
    }

    @Test
    void zeroGrowthAndYieldOnAMeasuredHopStillStopsExpansion() {
        DeterministicRulePolicy policy = new DeterministicRulePolicy(1.5D, 0.0005D, 12, 0.75D, 1, 32,
            2.0D, 0.5D, 16);

        RetrievalPlan replanned = policy.replan(context(), progress(0.0D, 0.0D, 1));

        assertTrue(replanned.getReasons().contains("MARGINAL_YIELD_BELOW_THRESHOLD"));
        assertEquals(1, replanned.getMaxHop());
    }

    @Test
    void healthyFirstHopKeepsTwoHopsFunded() {
        DeterministicRulePolicy policy = new DeterministicRulePolicy(1.5D, 0.0005D, 12, 0.75D, 1, 32,
            2.0D, 0.5D, 16);

        RetrievalPlan replanned = policy.replan(context(), progress(0.1D, 0.9D, 1));

        assertEquals(2, replanned.getMaxHop());
        assertTrue(replanned.getChannelBudgets().containsKey("GRAPH"));
        assertFalse(replanned.getReasons().contains("GRAPH_STOPPED_GROWTH"));
        assertFalse(replanned.getReasons().contains("MARGINAL_YIELD_BELOW_THRESHOLD"));
    }

    @Test
    void postHopBoundaryKeepsTheAfterTextBudgetSplit() {
        // The after-text split reserves a probe-sized share for the text channels. It must still
        // apply at the post-hop boundary even though that boundary is not literally AFTER_TEXT,
        // otherwise the graph stage would silently lose the allocation it had before the hop.
        DeterministicRulePolicy policy = new DeterministicRulePolicy();
        RoutingContext context = context();

        RetrievalPlan afterHop = policy.replan(context, progress(0.1D, 0.9D, 1, "AFTER_GRAPH_HOP"));

        assertEquals(1, afterHop.getChannelBudgets().get("BM25"));
        assertEquals(1, afterHop.getChannelBudgets().get("VECTOR"));
        assertEquals(48, afterHop.getChannelBudgets().get("GRAPH"));
    }

    @Test
    void progressBoundaryIsCarriedThroughInsteadOfBeingHardCoded() {
        RetrievalProgress progress = progress(0.1D, 0.9D, 1, "AFTER_GRAPH_HOP");

        assertEquals("AFTER_GRAPH_HOP", progress.getBoundary());
    }

    private static RetrievalProgress progress(double growth, double yield, int replanCount) {
        return progress(growth, yield, replanCount, "AFTER_GRAPH_HOP");
    }

    private static RetrievalProgress progress(double growth, double yield, int replanCount,
                                              String boundary) {
        return RetrievalProgress.builder().boundary(boundary)
            .completedChannels(new LinkedHashSet<>(Arrays.asList("BM25", "VECTOR")))
            .remainingCandidates(48).remainingTokens(4096)
            .firstStageScoreGap(0.001D).anchorCount(2).anchorConfidence(0.9D)
            .candidateDegree(4).graphGrowthRate(growth).graphMarginalYield(yield)
            .replanCount(replanCount).build();
    }

    private static RoutingContext context() {
        RoutingFeatures features = RoutingFeatures.builder().queryLength(8).averageIdf(3.0D)
            .anchorCount(2).anchorConfidence(0.9D).candidateDegree(4).build();
        return RoutingContext.builder().query("who influenced whom").features(features)
            .availableIndexes(Arrays.asList("BM25", "VECTOR", "GRAPH"))
            .budget(new RetrievalBudget(10, 3000, 50, 4096,
                org.apache.geaflow.ai.retrieval.execution.BudgetConstraint.CANDIDATES))
            .requestedMode(RetrievalMode.ADAPTIVE_HYBRID).executionMode(ExecutionMode.CASCADED)
            .maxHop(2).maxReplanCount(2).configVersion("v1").seed(7L).build();
    }
}
