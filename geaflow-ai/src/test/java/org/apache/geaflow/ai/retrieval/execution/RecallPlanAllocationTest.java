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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.geaflow.ai.retrieval.api.model.ExecutionMode;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalBudget;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalMode;
import org.junit.jupiter.api.Test;

/**
 * Pins which allocation path owns the channel split.
 *
 * <p>Two allocation paths exist. With a routing plan, {@code DeterministicRulePolicy.addChannels}
 * owns the split and {@link RecallPlan} receives an explicit budget map. Without one, this class
 * derives the split from the cascade bounds. The paths disagree in detail, so nothing should
 * silently start consulting the wrong one. These tests fix the boundary: an explicit map wins
 * outright, and the cascade bounds only apply when no map is supplied.
 */
class RecallPlanAllocationTest {

    @Test
    void explicitChannelBudgetsAreUsedVerbatim() {
        // This is the adaptive path: the routing policy already decided, so the plan must not
        // re-derive anything from the cascade bounds.
        Map<String, Integer> budgets = new LinkedHashMap<>();
        budgets.put("BM25", 20);
        budgets.put("VECTOR", 20);
        budgets.put("GRAPH", 60);

        RecallPlan plan = new RecallPlan(RetrievalMode.HYBRID, 5, 0L, null, null, null,
            BudgetConstraint.CANDIDATES, ExecutionMode.CASCADED, budgets, 2, 16, 2.0D);

        assertEquals(budgets, plan.getChannelBudgets(),
            "an explicit allocation must not be recomputed from the cascade bounds");
    }

    @Test
    void cascadeBoundsApplyOnlyWithoutAnExplicitAllocation() {
        // This is the non-adaptive path: no routing plan, so the cascade bounds size the probe.
        RecallPlan plan = new RecallPlan(RetrievalMode.HYBRID, 5, 100, 0L, null, null, null,
            BudgetConstraint.CANDIDATES, ExecutionMode.CASCADED, 5, 10, 2, 8);

        // 100 / 8 = 12, clamped to the configured probe maximum of 10.
        assertEquals(10, plan.getChannelBudgets().get("BM25"));
        assertEquals(10, plan.getChannelBudgets().get("VECTOR"));
        assertEquals(80, plan.getChannelBudgets().get("GRAPH"));
        assertEquals(100, sum(plan.getChannelBudgets()));
    }

    @Test
    void configuredCascadeBoundsChangeTheProbeSplit() {
        RecallPlan wideProbe = new RecallPlan(RetrievalMode.HYBRID, 5, 100, 0L, null, null, null,
            BudgetConstraint.CANDIDATES, ExecutionMode.CASCADED, 5, 30, 2, 2);
        RecallPlan narrowProbe = new RecallPlan(RetrievalMode.HYBRID, 5, 100, 0L, null, null, null,
            BudgetConstraint.CANDIDATES, ExecutionMode.CASCADED, 5, 5, 1, 8);

        assertEquals(30, wideProbe.getChannelBudgets().get("BM25"));
        assertEquals(40, wideProbe.getChannelBudgets().get("GRAPH"));
        assertEquals(5, narrowProbe.getChannelBudgets().get("BM25"));
        assertEquals(90, narrowProbe.getChannelBudgets().get("GRAPH"));
        // The graph channel receives the largest share in both configurations.
        assertTrue(narrowProbe.getChannelBudgets().get("GRAPH")
            > narrowProbe.getChannelBudgets().get("BM25"));
        assertTrue(wideProbe.getChannelBudgets().get("GRAPH")
            > wideProbe.getChannelBudgets().get("BM25"));
    }

    @Test
    void belowTheCascadeMinimumTheBudgetIsHalvedInsteadOfProbed() {
        RecallPlan plan = new RecallPlan(RetrievalMode.HYBRID, 3, 4, 0L, null, null, null,
            BudgetConstraint.CANDIDATES, ExecutionMode.CASCADED, 5, 10, 2, 8);

        // Below the configured minimum budget the probe is derived by halving, not by the divisor.
        assertEquals(4, sum(plan.getChannelBudgets()));
        assertTrue(plan.getChannelBudgets().get("GRAPH") >= 1);
    }

    @Test
    void adaptiveAllocationKeepsTheGraphChannelLargest() {
        // The invariant both paths share, and the one whose violation caused the MRR collapse.
        DeterministicRulePolicy policy = new DeterministicRulePolicy();
        RoutingContext context = RoutingContext.builder().query("q")
            .features(RoutingFeatures.builder().queryLength(6).averageIdf(0.5).anchorCount(1)
                .anchorConfidence(0.9).candidateDegree(4).graphGrowthRate(0.1).build())
            .availableIndexes(Arrays.asList("BM25", "VECTOR", "GRAPH"))
            .requestedMode(RetrievalMode.HYBRID).executionMode(ExecutionMode.CASCADED)
            .maxHop(2)
            .budget(new RetrievalBudget(5, 1000, 100, 1000, BudgetConstraint.CANDIDATES))
            .configVersion("cfg-v1").seed(7L).build();

        Map<String, Integer> budgets = policy.plan(context).getChannelBudgets();
        assertEquals(100, sum(budgets));
        for (Map.Entry<String, Integer> channel : budgets.entrySet()) {
            if (!"GRAPH".equals(channel.getKey())) {
                assertTrue(budgets.get("GRAPH") >= channel.getValue(),
                    "the graph channel must not be below " + channel.getKey());
            }
        }
    }

    private static int sum(Map<String, Integer> values) {
        int total = 0;
        for (Integer value : values.values()) {
            total += value;
        }
        return total;
    }
}