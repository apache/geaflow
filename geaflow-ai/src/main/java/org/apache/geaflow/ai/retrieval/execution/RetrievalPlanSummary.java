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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Compact immutable plan state stored in a replan trace event. */
public final class RetrievalPlanSummary {
    private final String selectedMode;
    private final List<String> channelOrder;
    private final Map<String, Integer> channelBudgets;
    private final int maxHop;
    private final int neighborCap;
    private final Map<String, Double> weights;

    public RetrievalPlanSummary(RetrievalPlan plan) {
        selectedMode = plan.getSelectedMode().name();
        channelOrder = Collections.unmodifiableList(new ArrayList<>(plan.getChannelOrder()));
        channelBudgets = Collections.unmodifiableMap(new LinkedHashMap<>(plan.getChannelBudgets()));
        maxHop = plan.getMaxHop();
        neighborCap = plan.getNeighborCap();
        Map<String, Double> values = new LinkedHashMap<>();
        values.put("BM25", plan.getBm25Weight());
        values.put("VECTOR", plan.getVectorWeight());
        values.put("GRAPH", plan.getGraphWeight());
        weights = Collections.unmodifiableMap(values);
    }

    public String getSelectedMode() {
        return selectedMode;
    }

    public List<String> getChannelOrder() {
        return channelOrder;
    }

    public Map<String, Integer> getChannelBudgets() {
        return channelBudgets;
    }

    public int getMaxHop() {
        return maxHop;
    }

    public int getNeighborCap() {
        return neighborCap;
    }

    public Map<String, Double> getWeights() {
        return weights;
    }
}
