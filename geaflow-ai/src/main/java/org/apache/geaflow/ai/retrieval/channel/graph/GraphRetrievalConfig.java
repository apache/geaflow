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

package org.apache.geaflow.ai.retrieval.channel.graph;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** Immutable deterministic budgets and scoring controls for bounded graph expansion. */
public final class GraphRetrievalConfig {
    private final int maxHop;
    private final int candidateBudget;
    private final int edgeScanBudget;
    private final int neighborCap;
    private final int tokenBudget;
    private final double minAssociationScore;
    private final double maxDiffusionGrowth;
    private final double minMarginalYield;
    private final double hopDecay;
    private final double degreePenalty;
    private final double anchorScoreWeight;
    private final double edgeStrengthWeight;
    private final double sourceRelevanceWeight;
    private final long samplingSeed;
    private final double sampleRate;
    private final Map<String, Double> relationPriorities;

    private GraphRetrievalConfig(Builder builder) {
        this.maxHop = builder.maxHop;
        this.candidateBudget = builder.candidateBudget;
        this.edgeScanBudget = builder.edgeScanBudget;
        this.neighborCap = builder.neighborCap;
        this.tokenBudget = builder.tokenBudget;
        this.minAssociationScore = builder.minAssociationScore;
        this.maxDiffusionGrowth = builder.maxDiffusionGrowth;
        this.minMarginalYield = builder.minMarginalYield;
        this.hopDecay = builder.hopDecay;
        this.degreePenalty = builder.degreePenalty;
        this.anchorScoreWeight = builder.anchorScoreWeight;
        this.edgeStrengthWeight = builder.edgeStrengthWeight;
        this.sourceRelevanceWeight = builder.sourceRelevanceWeight;
        this.samplingSeed = builder.samplingSeed;
        this.sampleRate = builder.sampleRate;
        this.relationPriorities = Collections.unmodifiableMap(new LinkedHashMap<>(builder.relationPriorities));
    }

    public static Builder builder() {
        return new Builder();
    }

    public static GraphRetrievalConfig defaults() {
        return builder().build();
    }

    public static GraphRetrievalConfig oneHop(int candidateBudget, int edgeScanBudget) {
        return builder().maxHop(1).candidateBudget(candidateBudget)
            .edgeScanBudget(edgeScanBudget).build();
    }

    public int getMaxHop() {
        return maxHop;
    }

    public int getCandidateBudget() {
        return candidateBudget;
    }

    public int getMaxCandidates() {
        return candidateBudget;
    }

    public int getEdgeScanBudget() {
        return edgeScanBudget;
    }

    public int getMaxEdgesExamined() {
        return edgeScanBudget;
    }

    public int getNeighborCap() {
        return neighborCap;
    }

    public int getMaxNeighborsPerVertex() {
        return neighborCap;
    }

    public int getTokenBudget() {
        return tokenBudget;
    }

    public double getMinAssociationScore() {
        return minAssociationScore;
    }

    public double getMaxDiffusionGrowth() {
        return maxDiffusionGrowth;
    }

    public double getDiffusionGrowthLimit() {
        return maxDiffusionGrowth;
    }

    public double getMinMarginalYield() {
        return minMarginalYield;
    }

    public double getMarginalYieldThreshold() {
        return minMarginalYield;
    }

    public double getHopDecay() {
        return hopDecay;
    }

    public double getDegreePenalty() {
        return degreePenalty;
    }

    public double getAnchorScoreWeight() {
        return anchorScoreWeight;
    }

    public double getEdgeStrengthWeight() {
        return edgeStrengthWeight;
    }

    public double getSourceRelevanceWeight() {
        return sourceRelevanceWeight;
    }

    public long getSamplingSeed() {
        return samplingSeed;
    }

    public long getConfigSeed() {
        return samplingSeed;
    }

    public double getSampleRate() {
        return sampleRate;
    }

    public Map<String, Double> getRelationPriorities() {
        return relationPriorities;
    }

    public double relationPriority(String label) {
        return relationPriorities.getOrDefault(label, 1.0);
    }

    /** Builder with finite, non-negative controls suitable for request-local use. */
    public static final class Builder {
        private int maxHop = 2;
        private int candidateBudget = 100;
        private int edgeScanBudget = 100;
        private int neighborCap = 100;
        private int tokenBudget = Integer.MAX_VALUE;
        private double minAssociationScore;
        private double maxDiffusionGrowth = Double.POSITIVE_INFINITY;
        private double minMarginalYield;
        private double hopDecay = 0.8;
        private double degreePenalty = 0.1;
        private double anchorScoreWeight = 1.0;
        private double edgeStrengthWeight = 1.0;
        private double sourceRelevanceWeight = 1.0;
        private long samplingSeed;
        private double sampleRate = 1.0;
        private final Map<String, Double> relationPriorities = new LinkedHashMap<>();

        public Builder maxHop(int value) {
            maxHop = value;
            return this;
        }

        public Builder candidateBudget(int value) {
            candidateBudget = value;
            return this;
        }

        public Builder maxCandidates(int value) {
            return candidateBudget(value);
        }

        public Builder edgeScanBudget(int value) {
            edgeScanBudget = value;
            return this;
        }

        public Builder maxEdgesExamined(int value) {
            return edgeScanBudget(value);
        }

        public Builder neighborCap(int value) {
            neighborCap = value;
            return this;
        }

        public Builder maxNeighborsPerVertex(int value) {
            return neighborCap(value);
        }

        public Builder tokenBudget(int value) {
            tokenBudget = value;
            return this;
        }

        public Builder minAssociationScore(double value) {
            minAssociationScore = value;
            return this;
        }

        public Builder maxDiffusionGrowth(double value) {
            maxDiffusionGrowth = value;
            return this;
        }

        public Builder diffusionGrowthLimit(double value) {
            return maxDiffusionGrowth(value);
        }

        public Builder minMarginalYield(double value) {
            minMarginalYield = value;
            return this;
        }

        public Builder marginalYieldThreshold(double value) {
            return minMarginalYield(value);
        }

        public Builder hopDecay(double value) {
            hopDecay = value;
            return this;
        }

        public Builder degreePenalty(double value) {
            degreePenalty = value;
            return this;
        }

        public Builder anchorScoreWeight(double value) {
            anchorScoreWeight = value;
            return this;
        }

        public Builder edgeStrengthWeight(double value) {
            edgeStrengthWeight = value;
            return this;
        }

        public Builder sourceRelevanceWeight(double value) {
            sourceRelevanceWeight = value;
            return this;
        }

        public Builder samplingSeed(long value) {
            samplingSeed = value;
            return this;
        }

        public Builder configSeed(long value) {
            return samplingSeed(value);
        }

        public Builder sampleRate(double value) {
            sampleRate = value;
            return this;
        }

        public Builder relationPriority(String label, double value) {
            if (label == null || label.trim().isEmpty()) {
                throw new IllegalArgumentException("relation label is required");
            }
            relationPriorities.put(label, value);
            return this;
        }

        public Builder relationPriorities(Map<String, Double> values) {
            relationPriorities.clear();
            if (values != null) {
                for (Map.Entry<String, Double> entry : values.entrySet()) {
                    relationPriority(entry.getKey(), entry.getValue());
                }
            }
            return this;
        }

        public GraphRetrievalConfig build() {
            if (maxHop < 1 || maxHop > 2 || candidateBudget < 1 || edgeScanBudget < 1
                || neighborCap < 1 || tokenBudget < 1) {
                throw new IllegalArgumentException("graph budgets must be positive and maxHop must be 1 or 2");
            }
            requireUnitInterval(hopDecay, "hopDecay");
            requireNonNegative(minAssociationScore, "minAssociationScore");
            requireNonNegative(maxDiffusionGrowth, "maxDiffusionGrowth", true);
            requireNonNegative(minMarginalYield, "minMarginalYield");
            requireNonNegative(degreePenalty, "degreePenalty");
            requireNonNegative(anchorScoreWeight, "anchorScoreWeight");
            requireNonNegative(edgeStrengthWeight, "edgeStrengthWeight");
            requireNonNegative(sourceRelevanceWeight, "sourceRelevanceWeight");
            requireUnitInterval(sampleRate, "sampleRate");
            for (Map.Entry<String, Double> entry : relationPriorities.entrySet()) {
                requireNonNegative(entry.getValue(), "relation priority");
            }
            return new GraphRetrievalConfig(this);
        }

        private static void requireNonNegative(double value, String name) {
            requireNonNegative(value, name, false);
        }

        private static void requireNonNegative(double value, String name, boolean allowInfinity) {
            if (Double.isNaN(value) || (!allowInfinity && !Double.isFinite(value)) || value < 0.0) {
                throw new IllegalArgumentException(name + " must be finite and non-negative");
            }
        }

        private static void requireUnitInterval(double value, String name) {
            if (!Double.isFinite(value) || value < 0.0 || value > 1.0) {
                throw new IllegalArgumentException(name + " must be in [0,1]");
            }
        }
    }
}
