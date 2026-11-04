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
 */
package org.apache.geaflow.ai.retrieval.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalBudget;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalMode;
import org.junit.jupiter.api.Test;

/**
 * Pins the design's documented extension-point names and their purity contract.
 *
 * <p>The design names {@code AdaptivePolicy} with {@code plan}/{@code replan}/{@code getPolicyId}/
 * {@code getPolicyVersion}. The working implementation ships as {@code RoutingPolicy}, so these
 * tests guarantee the documented shape stays available and behaves identically to the SPI the
 * executor actually uses.
 */
class AdaptivePolicyContractTest {

    @Test
    void designInterfaceExposesPlanAndReplan() {
        AdaptivePolicy policy = new CountingPolicy();

        RoutingContext context = context();
        RetrievalPlan initial = policy.plan(context);
        RetrievalPlan replanned = policy.replan(context,
            RetrievalProgress.builder().boundary("GRAPH_PROBE").build());

        assertNotNull(initial);
        assertNotNull(replanned);
        assertTrue(policy.getPolicyId() != null);
        assertTrue(policy.getPolicyVersion() != null);
    }

    @Test
    void defaultReplanKeepsTheInitialDecision() {
        AdaptivePolicy policy = new DefaultReplanPolicy();

        RoutingContext context = context();
        RetrievalPlan initial = policy.plan(context);
        RetrievalPlan replanned = policy.replan(context,
            RetrievalProgress.builder().boundary("SECOND_STAGE").build());

        assertSame(initial, replanned, "a policy that does not replan must return the initial plan");
    }

    @Test
    void adapterMapsInitialAndBoundaryCallsOntoTheWorkingSpi() {
        CountingPolicy policy = new CountingPolicy();
        RoutingPolicy working = policy.toRoutingPolicy();

        assertEquals("test-policy", working.getPolicyId());
        assertEquals("v-test", working.getPolicyVersion());
        assertTrue(working.supports(RetrievalMode.ADAPTIVE_HYBRID));

        working.plan(context(), RetrievalProgress.builder().boundary("INITIAL").build());
        assertEquals(1, policy.planned);
        assertEquals(0, policy.replanned);

        working.plan(context(), RetrievalProgress.builder().boundary("GRAPH_PROBE").build());
        assertEquals(1, policy.planned);
        assertEquals(1, policy.replanned);
    }

    @Test
    void productionPolicyImplementsBothTheDesignAndWorkingInterfaces() {
        DeterministicRulePolicy policy = new DeterministicRulePolicy();

        // The shipped policy is the design-level AdaptivePolicy *and* the working RoutingPolicy.
        // Implementing both directly is what stops the documented contract from being a
        // documentation-only alias that production never exercises.
        assertTrue(policy instanceof AdaptivePolicy,
            "the production policy must implement the documented AdaptivePolicy contract");
        assertTrue(policy instanceof RoutingPolicy,
            "the production policy must keep exposing the executor SPI");

        RoutingContext context = context();
        RetrievalPlan viaDesign = ((AdaptivePolicy) policy).plan(context);
        RetrievalPlan viaWorking = ((RoutingPolicy) policy).plan(context,
            RetrievalProgress.builder().boundary("INITIAL").build());
        assertEquals(viaWorking.getSelectedMode(), viaDesign.getSelectedMode());
        assertEquals(viaWorking.getTopK(), viaDesign.getTopK());
    }

    @Test
    void factoryReturnsAPolicyThatSatisfiesTheDesignContract() {
        RoutingPolicy fromFactory = RetrievalStrategyFactory.routingPolicy(
            RetrievalStrategyFactory.DETERMINISTIC_RULE_POLICY,
            new org.apache.geaflow.ai.retrieval.config.RetrievalProperties());

        assertTrue(fromFactory instanceof AdaptivePolicy,
            "the configured policy must satisfy the documented contract");
    }

    @Test
    void deterministicRulePolicySatisfiesTheDesignContract() {
        DeterministicRulePolicy supplied = new DeterministicRulePolicy();

        RoutingContext context = context();
        RetrievalPlan viaSpi = supplied.plan(context,
            RetrievalProgress.builder().boundary("INITIAL").build());
        RetrievalPlan viaDesign = supplied.plan(context);

        assertEquals(viaSpi.getSelectedMode(), viaDesign.getSelectedMode());
        assertNotNull(supplied.getPolicyId());
        assertNotNull(supplied.getPolicyVersion());
        assertTrue(supplied.getParameters().containsKey("highAverageIdf"),
            "policy parameters must be published so a decision can be replayed");
    }

    private static RoutingContext context() {
        return RoutingContext.builder()
            .query("who taught ethics")
            .features(RoutingFeatures.builder().queryLength(3).averageIdf(2.0D)
                .firstStageScoreGap(0.3D).anchorCount(2).anchorConfidence(0.9D)
                .candidateDegree(4).graphGrowthRate(0.5D).build())
            .availableIndexes(Collections.singletonList("BM25"))
            .budget(new RetrievalBudget(10, 3000, 100, 4096))
            .requestedMode(RetrievalMode.ADAPTIVE_HYBRID)
            .executionMode(org.apache.geaflow.ai.retrieval.api.model.ExecutionMode.CASCADED)
            .maxHop(2).maxReplanCount(1)
            .configVersion("v1").seed(7L).build();
    }

    /** Policy that delegates replanning to the interface default. */
    private static final class DefaultReplanPolicy implements AdaptivePolicy {
        private final CountingPolicy delegate = new CountingPolicy();

        @Override
        public RetrievalPlan plan(RoutingContext context) {
            return delegate.plan(context);
        }

        @Override
        public boolean supports(RetrievalMode mode) {
            return mode != null;
        }

        @Override
        public String getPolicyId() {
            return delegate.getPolicyId();
        }

        @Override
        public String getPolicyVersion() {
            return delegate.getPolicyVersion();
        }
    }

    /** Minimal policy that counts which entry point the executor used. */
    private static class CountingPolicy implements AdaptivePolicy {
        private RetrievalPlan cached;
        private int planned;
        private int replanned;

        @Override
        public RetrievalPlan plan(RoutingContext context) {
            planned++;
            if (cached == null) {
                cached = RetrievalPlan.builder().selectedMode(RetrievalMode.FIXED_HYBRID)
                    .executionMode(org.apache.geaflow.ai.retrieval.api.model.ExecutionMode.CASCADED)
                    .topK(context.getBudget().getTopK()).build();
            }
            return cached;
        }

        @Override
        public RetrievalPlan replan(RoutingContext context, RetrievalProgress progress) {
            replanned++;
            if (cached == null) {
                cached = RetrievalPlan.builder().selectedMode(RetrievalMode.FIXED_HYBRID)
                    .executionMode(org.apache.geaflow.ai.retrieval.api.model.ExecutionMode.CASCADED)
                    .topK(context.getBudget().getTopK()).build();
            }
            return cached;
        }

        @Override
        public boolean supports(RetrievalMode mode) {
            return mode != null;
        }

        @Override
        public String getPolicyId() {
            return "test-policy";
        }

        @Override
        public String getPolicyVersion() {
            return "v-test";
        }
    }
}