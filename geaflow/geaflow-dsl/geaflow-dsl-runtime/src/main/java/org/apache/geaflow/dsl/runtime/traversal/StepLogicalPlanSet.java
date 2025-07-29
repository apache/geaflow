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

package org.apache.geaflow.dsl.runtime.traversal;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.geaflow.dsl.common.types.GraphSchema;
import org.apache.geaflow.dsl.runtime.traversal.operator.MatchVertexOperator;
import org.apache.geaflow.dsl.runtime.traversal.operator.MatchVirtualEdgeOperator;
import org.apache.geaflow.dsl.runtime.traversal.operator.StepEndOperator;
import org.apache.geaflow.dsl.runtime.traversal.operator.StepExchangeOperator;
import org.apache.geaflow.dsl.runtime.traversal.operator.StepGlobalAggregateOperator;
import org.apache.geaflow.dsl.runtime.traversal.operator.StepGlobalSortOperator;
import org.apache.geaflow.dsl.runtime.traversal.operator.StepSubQueryStartOperator;

public class StepLogicalPlanSet {

    private final GraphSchema graphSchema;

    private StepLogicalPlan mainLogicalPlan;

    private final Map<String, StepLogicalPlan> subLogicalPlans = new HashMap<>();

    public StepLogicalPlanSet(GraphSchema graphSchema) {
        this.graphSchema = Objects.requireNonNull(graphSchema);
    }

    public StepLogicalPlanSet(StepLogicalPlan mainLogicalPlan) {
        this(mainLogicalPlan.getGraphSchema());
        this.mainLogicalPlan = mainLogicalPlan;
    }

    public void addSubLogicalPlan(StepLogicalPlan subLogicalPlan) {
        StepLogicalPlan headPlan = subLogicalPlan.getHeadPlan();
        assert headPlan.getOperator() instanceof StepSubQueryStartOperator;
        StepSubQueryStartOperator startOperator = (StepSubQueryStartOperator) headPlan.getOperator();

        subLogicalPlans.put(startOperator.getQueryName(), subLogicalPlan);
    }

    public GraphSchema getGraphSchema() {
        return graphSchema;
    }

    public StepLogicalPlan getMainPlan() {
        return mainLogicalPlan;
    }

    public void setMainPlan(StepLogicalPlan mainPlan) {
        this.mainLogicalPlan = Objects.requireNonNull(mainPlan);
    }

    public Map<String, StepLogicalPlan> getSubPlans() {
        return subLogicalPlans;
    }

    public String getPlanSetDesc() {
        StringBuilder graphviz = new StringBuilder();
        graphviz.append("digraph G {\n");
        String mainPlanDesc = mainLogicalPlan.getPlanDesc(true);
        graphviz.append(mainPlanDesc).append("\n");

        for (StepLogicalPlan subPlan : subLogicalPlans.values()) {
            graphviz.append(subPlan.getPlanDesc(true)).append("\n");
        }
        String str = StringUtils.stripEnd(graphviz.toString(), "\n");
        return str + "\n}";
    }

    public StepLogicalPlanSet markChainable() {
        this.mainLogicalPlan = mainLogicalPlan.end();
        markChainable(mainLogicalPlan);
        for (StepLogicalPlan subPlan : subLogicalPlans.values()) {
            markChainable(subPlan);
        }
        return this;
    }

    private static StepLogicalPlan markChainable(StepLogicalPlan endPlan) {
        Map<Long, Integer> visitedPlanNumMV = new HashMap<>();
        markChainable(endPlan, visitedPlanNumMV);
        return endPlan;
    }

    private static void markChainable(StepLogicalPlan plan, Map<Long, Integer> visitedPlanNumMV) {
        if (visitedPlanNumMV.containsKey(plan.getId())) {
            return;
        }
        plan.getInputs().forEach(input -> markChainable(input, visitedPlanNumMV));
        List<Integer> inputsNumMV = plan.getInputs()
            .stream().map(input -> visitedPlanNumMV.get(input.getId()))
            .collect(Collectors.toList());

        // init allow chain
        plan.setAllowChain(true);
        if (inputsNumMV.size() > 0) {
            int maxNumMatchVertex = inputsNumMV.stream().max(Integer::compareTo).get();
            if (plan.getOperator() instanceof MatchVertexOperator) {
                if (maxNumMatchVertex == 1) { //For VC
                    plan.getInputs().forEach(input -> input.setAllowChain(false));
                    visitedPlanNumMV.put(plan.getId(), 1);
                } else {
                    visitedPlanNumMV.put(plan.getId(), maxNumMatchVertex + 1);
                }
            } else if (plan.getOperator() instanceof StepEndOperator) {
                if (inputsNumMV.size() > 1) {
                    // If input size of end operator is > 1 ,then it cannot chain with the inputs.
                    plan.getInputs().forEach(input -> input.setAllowChain(false));
                }
            } else if (plan.getOperator() instanceof StepExchangeOperator) {
                plan.setAllowChain(false);
                visitedPlanNumMV.put(plan.getId(), 0);
            } else if (plan.getOperator() instanceof StepGlobalSortOperator) {
                // after global sort, we should send the vertex back, so it cannot chain with the follow op.
                // This is an implicit vertex load, so the init mv number should be 1.
                plan.setAllowChain(false);
                visitedPlanNumMV.put(plan.getId(), 1);
            } else if (plan.getOperator() instanceof StepGlobalAggregateOperator) {
                // after global aggregate, we should send the vertex back, so it cannot chain with
                // the follow op. This is an implicit vertex load, so the init mv number should
                // be 1.
                plan.setAllowChain(false);
                visitedPlanNumMV.put(plan.getId(), 1);
            } else if (plan.getOperator() instanceof MatchVirtualEdgeOperator) {
                plan.setAllowChain(false);
                visitedPlanNumMV.put(plan.getId(), 1);
            } else if (plan.getOperator() instanceof StepSubQueryStartOperator) {
                plan.getInputs().forEach(input -> input.setAllowChain(false));
                visitedPlanNumMV.put(plan.getId(), maxNumMatchVertex);
            } else {
                visitedPlanNumMV.put(plan.getId(), maxNumMatchVertex);
            }
        } else {
            visitedPlanNumMV.put(plan.getId(), 0);
        }
    }

}
