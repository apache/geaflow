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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import org.apache.geaflow.ai.retrieval.api.model.ExecutionMode;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalMode;

/** Immutable routing result that can be validated before execution. */
public final class RetrievalPlan {
    private final RetrievalMode selectedMode;
    private final ExecutionMode executionMode;
    private final int topK;
    private final Map<String, Integer> channelBudgets;
    private final List<String> channelOrder;
    private final int candidateBudget;
    private final int tokenBudget;
    private final long latencyBudgetNanos;
    private final int maxHop;
    private final int neighborCap;
    private final double maxDiffusionGrowth;
    private final double bm25Weight;
    private final double vectorWeight;
    private final double graphWeight;
    private final RetrievalMode fallbackMode;
    private final Map<String, Integer> hardUpperBounds;
    private final List<String> reasons;
    private final String policyId;
    private final String policyVersion;

    private RetrievalPlan(Builder builder) {
        selectedMode = builder.selectedMode == null ? RetrievalMode.HYBRID : builder.selectedMode.canonical();
        executionMode = builder.executionMode == null ? ExecutionMode.SEQUENTIAL : builder.executionMode;
        topK = builder.topK;
        channelBudgets = Collections.unmodifiableMap(new LinkedHashMap<>(builder.channelBudgets));
        channelOrder = Collections.unmodifiableList(new ArrayList<>(builder.channelOrder));
        candidateBudget = builder.candidateBudget;
        tokenBudget = builder.tokenBudget;
        latencyBudgetNanos = builder.latencyBudgetNanos;
        maxHop = builder.maxHop;
        neighborCap = builder.neighborCap;
        maxDiffusionGrowth = builder.maxDiffusionGrowth;
        bm25Weight = builder.bm25Weight;
        vectorWeight = builder.vectorWeight;
        graphWeight = builder.graphWeight;
        fallbackMode = builder.fallbackMode == null ? null : builder.fallbackMode.canonical();
        hardUpperBounds = Collections.unmodifiableMap(new LinkedHashMap<>(builder.hardUpperBounds));
        reasons = Collections.unmodifiableList(new ArrayList<>(builder.reasons));
        policyId = builder.policyId;
        policyVersion = builder.policyVersion;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(RetrievalPlan source) {
        Builder builder = builder().selectedMode(source.selectedMode).executionMode(source.executionMode)
            .topK(source.topK).candidateBudget(source.candidateBudget).tokenBudget(source.tokenBudget)
            .latencyBudgetNanos(source.latencyBudgetNanos).maxHop(source.maxHop)
            .neighborCap(source.neighborCap).maxDiffusionGrowth(source.maxDiffusionGrowth)
            .bm25Weight(source.bm25Weight)
            .vectorWeight(source.vectorWeight).graphWeight(source.graphWeight)
            .fallbackMode(source.fallbackMode).policyId(source.policyId)
            .policyVersion(source.policyVersion).channelOrder(source.channelOrder)
            .channelBudgets(source.channelBudgets).hardUpperBounds(source.hardUpperBounds)
            .reasons(source.reasons);
        return builder;
    }

    public RetrievalMode getSelectedMode() {
        return selectedMode;
    }

    public RetrievalMode getMode() {
        return selectedMode;
    }

    public ExecutionMode getExecutionMode() {
        return executionMode;
    }

    public int getTopK() {
        return topK;
    }

    public Map<String, Integer> getChannelBudgets() {
        return channelBudgets;
    }

    public List<String> getChannelOrder() {
        return channelOrder;
    }

    public int getCandidateBudget() {
        return candidateBudget;
    }

    public int getTokenBudget() {
        return tokenBudget;
    }

    public long getLatencyBudgetNanos() {
        return latencyBudgetNanos;
    }

    public int getMaxHop() {
        return maxHop;
    }

    public int getNeighborCap() {
        return neighborCap;
    }

    public double getMaxDiffusionGrowth() {
        return maxDiffusionGrowth;
    }

    public double getBm25Weight() {
        return bm25Weight;
    }

    public double getVectorWeight() {
        return vectorWeight;
    }

    public double getGraphWeight() {
        return graphWeight;
    }

    public RetrievalMode getFallbackMode() {
        return fallbackMode;
    }

    public Map<String, Integer> getHardUpperBounds() {
        return hardUpperBounds;
    }

    public List<String> getReasons() {
        return reasons;
    }

    public String getPolicyId() {
        return policyId;
    }

    public String getPolicyVersion() {
        return policyVersion;
    }

    /** Builder for a policy output. */
    public static final class Builder {
        private RetrievalMode selectedMode = RetrievalMode.HYBRID;
        private ExecutionMode executionMode = ExecutionMode.SEQUENTIAL;
        private int topK = 1;
        private final Map<String, Integer> channelBudgets = new LinkedHashMap<>();
        private final List<String> channelOrder = new ArrayList<>();
        private int candidateBudget = 1;
        private int tokenBudget = Integer.MAX_VALUE;
        private long latencyBudgetNanos;
        private int maxHop = 1;
        private int neighborCap = 1;
        private double maxDiffusionGrowth = Double.POSITIVE_INFINITY;
        private double bm25Weight = 1.0D;
        private double vectorWeight = 1.0D;
        private double graphWeight = 1.0D;
        private RetrievalMode fallbackMode;
        private final Map<String, Integer> hardUpperBounds = new LinkedHashMap<>();
        private final List<String> reasons = new ArrayList<>();
        private String policyId;
        private String policyVersion;

        public Builder selectedMode(RetrievalMode value) {
            selectedMode = value;
            return this;
        }

        public Builder mode(RetrievalMode value) {
            return selectedMode(value);
        }

        public Builder executionMode(ExecutionMode value) {
            executionMode = value;
            return this;
        }

        public Builder topK(int value) {
            topK = value;
            return this;
        }

        public Builder channelBudgets(Map<String, Integer> values) {
            channelBudgets.clear();
            if (values != null) {
                channelBudgets.putAll(values);
            }
            return this;
        }

        public Builder channelBudget(String channel, int value) {
            channelBudgets.put(channel, value);
            return this;
        }

        public Builder channelOrder(Iterable<String> values) {
            channelOrder.clear();
            if (values != null) {
                for (String value : values) {
                    if (value != null) {
                        channelOrder.add(value);
                    }
                }
            }
            return this;
        }

        public Builder candidateBudget(int value) {
            candidateBudget = value;
            return this;
        }

        public Builder tokenBudget(int value) {
            tokenBudget = value;
            return this;
        }

        public Builder latencyBudgetNanos(long value) {
            latencyBudgetNanos = value;
            return this;
        }

        public Builder maxHop(int value) {
            maxHop = value;
            return this;
        }

        public Builder neighborCap(int value) {
            neighborCap = value;
            return this;
        }

        public Builder maxDiffusionGrowth(double value) {
            maxDiffusionGrowth = value;
            return this;
        }

        public Builder bm25Weight(double value) {
            bm25Weight = value;
            return this;
        }

        public Builder vectorWeight(double value) {
            vectorWeight = value;
            return this;
        }

        public Builder graphWeight(double value) {
            graphWeight = value;
            return this;
        }

        public Builder fallbackMode(RetrievalMode value) {
            fallbackMode = value;
            return this;
        }

        public Builder hardUpperBounds(Map<String, Integer> values) {
            hardUpperBounds.clear();
            if (values != null) {
                hardUpperBounds.putAll(values);
            }
            return this;
        }

        public Builder hardUpperBound(String name, int value) {
            hardUpperBounds.put(name, value);
            return this;
        }

        public Builder reasons(Iterable<String> values) {
            reasons.clear();
            if (values != null) {
                for (String value : values) {
                    if (value != null) {
                        reasons.add(value);
                    }
                }
            }
            return this;
        }

        public Builder reason(String value) {
            if (value != null) {
                reasons.add(value);
            }
            return this;
        }

        public Builder policyId(String value) {
            policyId = value;
            return this;
        }

        public Builder policyVersion(String value) {
            policyVersion = value;
            return this;
        }

        public RetrievalPlan build() {
            if (channelOrder.isEmpty()) {
                channelOrder.addAll(new LinkedHashSet<>(channelBudgets.keySet()));
            }
            return new RetrievalPlan(this);
        }
    }
}
