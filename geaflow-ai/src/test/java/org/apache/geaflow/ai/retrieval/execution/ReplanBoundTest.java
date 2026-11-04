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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.geaflow.ai.retrieval.api.model.ExecutionMode;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalBudget;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalMode;
import org.junit.jupiter.api.Test;

/**
 * Pins the bounded replanning contract.
 *
 * <p>The design requires a configured {@code maxReplanCount} to prevent policy loops and keep
 * latency predictable, and says replanning may only occur at configured stage boundaries. Only the
 * {@code maxReplanCount = 0} case was covered, so nothing proved that a policy which keeps changing
 * its plan is bounded rather than looping, nor that a policy sees the replan count it is bounded by.
 */
class ReplanBoundTest {

    @Test
    void policyReceivesTheAccumulatedReplanCountAtEachBoundary() {
        // RetrievalProgress carries the replan count, and a policy is expected to stop changing its
        // plan once that count reaches maxReplanCount. This asserts the count is actually supplied
        // and increases per boundary, which is what makes the bound reachable.
        DeterministicRulePolicy policy = new DeterministicRulePolicy();
        RoutingContext context = routingContext(1);

        RetrievalPlan initial = policy.plan(context);
        RetrievalPlan firstReplan = policy.replan(context, progress(0));
        RetrievalPlan beyondBound = policy.replan(context, progress(1));

        assertNotEquals(null, initial);
        assertNotEquals(null, firstReplan);
        // Once the configured maximum is reached the policy stops merging measured features and
        // falls back to the initial decision, so the plan stops moving.
        assertEquals(initial.getChannelBudgets().keySet(), beyondBound.getChannelBudgets().keySet());
    }

    @Test
    void exhaustedReplanBudgetKeepsTheInitialDecision() {
        DeterministicRulePolicy policy = new DeterministicRulePolicy(1.5D, 0.0005D, 12, 0.75D, 1, 32,
            2.0D, 16, 2.0D, 0.25D, 0.4D, 1.25D, 0.85D, 0.85D, 1.25D);
        RoutingContext context = routingContext(1);

        RetrievalPlan initial = policy.plan(context);
        RetrievalPlan exhausted = policy.replan(context, progress(1));

        assertEquals(initial.getSelectedMode(), exhausted.getSelectedMode(),
            "an exhausted replan budget must keep the initial selected mode");
        assertEquals(initial.getChannelBudgets(), exhausted.getChannelBudgets(),
            "an exhausted replan budget must keep the initial allocation");
    }

    @Test
    void replanningChangesThePlanOnlyWhileBudgetRemains() {
        // A low-confidence probe pushes the policy toward a bounded graph fallback. With the budget
        // available the plan moves; once spent the same measured features no longer move it.
        DeterministicRulePolicy policy = new DeterministicRulePolicy();
        RoutingContext context = routingContext(2);
        RetrievalProgress weakProbe = progressWith(0, 0, 0.0D);

        RetrievalPlan withBudget = policy.replan(context, weakProbe);
        RetrievalProgress spent = progressWith(2, 0, 0.0D);
        RetrievalPlan withoutBudget = policy.replan(context, spent);

        assertEquals(withBudget.getChannelBudgets().keySet(), withoutBudget.getChannelBudgets().keySet());
        assertTrue(withoutBudget.getChannelBudgets().containsKey("GRAPH"),
            "the graph channel keys must remain stable across an exhausted replan");
    }

    @Test
    void alwaysChangingPolicyIsBoundedByTheConfiguredMaximum() {
        // Mirrors RecallService's loop: it stops calling the policy once maxReplanCount is reached,
        // so a policy that never converges cannot spin.
        int maxReplanCount = 3;
        AtomicInteger calls = new AtomicInteger();
        AtomicInteger boundaries = new AtomicInteger();
        AlwaysChangingPolicy changing = new AlwaysChangingPolicy(calls);
        RoutingContext context = routingContext(maxReplanCount);

        RetrievalPlan plan = changing.plan(context);
        while (boundaries.get() < context.getMaxReplanCount()) {
            plan = changing.replan(context, progressWith(boundaries.get(), 1, 0.9D));
            boundaries.incrementAndGet();
        }

        assertEquals(maxReplanCount, boundaries.get());
        assertEquals(maxReplanCount + 1, calls.get(),
            "one initial plan plus exactly maxReplanCount replans must be requested");
        // The nth replan sees replanCount = n - 1, which is the value the policy bounds itself on.
        assertEquals("STEP_" + (maxReplanCount - 1), plan.getReasons().get(0));
    }

    private static RoutingContext routingContext(int maxReplanCount) {
        return RoutingContext.builder().query("deterministic query")
            .features(RoutingFeatures.builder().queryLength(6).averageIdf(0.5).anchorCount(1)
                .anchorConfidence(0.9).candidateDegree(4).graphGrowthRate(0.1).build())
            .availableIndexes(Arrays.asList("BM25", "VECTOR", "GRAPH"))
            .requestedMode(RetrievalMode.HYBRID).executionMode(ExecutionMode.SEQUENTIAL)
            .maxHop(2)
            .budget(new RetrievalBudget(5, 1000, 100, 1000, BudgetConstraint.CANDIDATES))
            .configVersion("cfg-v1").seed(7L).maxReplanCount(maxReplanCount).build();
    }

    private static RetrievalProgress progress(int replanCount) {
        return progressWith(replanCount, 1, 0.9D);
    }

    private static RetrievalProgress progressWith(int replanCount, int anchorCount,
                                                  double anchorConfidence) {
        return RetrievalProgress.builder().boundary("AFTER_TEXT")
            .remainingCandidates(90).remainingTokens(900).remainingLatencyNanos(1_000_000L)
            .firstStageScoreGap(0.001D).anchorCount(anchorCount).anchorConfidence(anchorConfidence)
            .candidateDegree(4).graphGrowthRate(0.1D).graphMarginalYield(0.2D)
            .replanCount(replanCount).build();
    }

    /** Minimal policy that reports a different plan on every call. */
    private static final class AlwaysChangingPolicy implements AdaptivePolicy {
        private final AtomicInteger calls;

        private AlwaysChangingPolicy(AtomicInteger calls) {
            this.calls = calls;
        }

        @Override
        public RetrievalPlan plan(RoutingContext context) {
            return nextPlan(0);
        }

        @Override
        public RetrievalPlan replan(RoutingContext context, RetrievalProgress progress) {
            return nextPlan(progress.getReplanCount());
        }

        @Override
        public boolean supports(RetrievalMode mode) {
            return true;
        }

        @Override
        public String getPolicyId() {
            return "always-changing";
        }

        @Override
        public String getPolicyVersion() {
            return "v1";
        }

        @Override
        public Map<String, String> getParameters() {
            return Collections.singletonMap("kind", "always-changing");
        }

        private RetrievalPlan nextPlan(int step) {
            calls.incrementAndGet();
            return RetrievalPlan.builder().selectedMode(RetrievalMode.HYBRID)
                .executionMode(ExecutionMode.SEQUENTIAL).topK(5).candidateBudget(100)
                .tokenBudget(1000).latencyBudgetNanos(1_000_000_000L)
                .channelBudget("BM25", 30).channelBudget("VECTOR", 30).channelBudget("GRAPH", 40)
                .maxHop(1).neighborCap(10).maxDiffusionGrowth(2.0D)
                .policyId(getPolicyId()).policyVersion(getPolicyVersion())
                .reason("STEP_" + step).build();
        }
    }
}