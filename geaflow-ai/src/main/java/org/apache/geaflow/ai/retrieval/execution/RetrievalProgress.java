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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/** Immutable progress snapshot observed only at a retrieval stage boundary. */
public final class RetrievalProgress {
    private final String boundary;
    private final Set<String> completedChannels;
    private final Map<String, Integer> candidateCounts;
    private final int remainingCandidates;
    private final int remainingTokens;
    private final long remainingLatencyNanos;
    private final double firstStageScoreGap;
    private final int anchorCount;
    private final double anchorConfidence;
    private final int candidateDegree;
    private final double graphGrowthRate;
    private final double graphMarginalYield;
    private final int replanCount;

    private RetrievalProgress(Builder builder) {
        boundary = builder.boundary == null ? "unknown" : builder.boundary;
        completedChannels = Collections.unmodifiableSet(new LinkedHashSet<>(builder.completedChannels));
        candidateCounts = Collections.unmodifiableMap(new LinkedHashMap<>(builder.candidateCounts));
        remainingCandidates = nonNegative(builder.remainingCandidates, "remainingCandidates");
        remainingTokens = nonNegative(builder.remainingTokens, "remainingTokens");
        remainingLatencyNanos = nonNegativeLong(builder.remainingLatencyNanos, "remainingLatencyNanos");
        firstStageScoreGap = finiteNonNegative(builder.firstStageScoreGap, "firstStageScoreGap");
        anchorCount = nonNegative(builder.anchorCount, "anchorCount");
        anchorConfidence = finiteRange(builder.anchorConfidence, "anchorConfidence");
        candidateDegree = nonNegative(builder.candidateDegree, "candidateDegree");
        graphGrowthRate = finiteNonNegative(builder.graphGrowthRate, "graphGrowthRate");
        graphMarginalYield = finiteNonNegative(builder.graphMarginalYield, "graphMarginalYield");
        replanCount = nonNegative(builder.replanCount, "replanCount");
    }

    public static Builder builder() {
        return new Builder();
    }

    private static int nonNegative(int value, String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must be non-negative");
        }
        return value;
    }

    private static long nonNegativeLong(long value, String name) {
        if (value < 0L) {
            throw new IllegalArgumentException(name + " must be non-negative");
        }
        return value;
    }

    private static double finiteNonNegative(double value, String name) {
        if (!Double.isFinite(value) || value < 0.0D) {
            throw new IllegalArgumentException(name + " must be finite and non-negative");
        }
        return value;
    }

    private static double finiteRange(double value, String name) {
        if (!Double.isFinite(value) || value < 0.0D || value > 1.0D) {
            throw new IllegalArgumentException(name + " must be between zero and one");
        }
        return value;
    }

    public String getBoundary() {
        return boundary;
    }

    public Set<String> getCompletedChannels() {
        return completedChannels;
    }

    public Map<String, Integer> getCandidateCounts() {
        return candidateCounts;
    }

    public int getRemainingCandidates() {
        return remainingCandidates;
    }

    public int getRemainingTokens() {
        return remainingTokens;
    }

    public long getRemainingLatencyNanos() {
        return remainingLatencyNanos;
    }

    public double getFirstStageScoreGap() {
        return firstStageScoreGap;
    }

    public int getAnchorCount() {
        return anchorCount;
    }

    public double getAnchorConfidence() {
        return anchorConfidence;
    }

    public int getCandidateDegree() {
        return candidateDegree;
    }

    public double getGraphGrowthRate() {
        return graphGrowthRate;
    }

    public double getGraphMarginalYield() {
        return graphMarginalYield;
    }

    public int getReplanCount() {
        return replanCount;
    }

    /** Builder for a boundary snapshot. */
    public static final class Builder {
        private String boundary;
        private final Set<String> completedChannels = new LinkedHashSet<>();
        private final Map<String, Integer> candidateCounts = new LinkedHashMap<>();
        private int remainingCandidates;
        private int remainingTokens;
        private long remainingLatencyNanos;
        private double firstStageScoreGap;
        private int anchorCount;
        private double anchorConfidence;
        private int candidateDegree;
        private double graphGrowthRate;
        private double graphMarginalYield;
        private int replanCount;

        public Builder boundary(String value) {
            boundary = value;
            return this;
        }

        public Builder completedChannels(Iterable<String> values) {
            completedChannels.clear();
            if (values != null) {
                for (String value : values) {
                    if (value != null) {
                        completedChannels.add(value);
                    }
                }
            }
            return this;
        }

        public Builder candidateCounts(Map<String, Integer> values) {
            candidateCounts.clear();
            if (values != null) {
                candidateCounts.putAll(values);
            }
            return this;
        }

        public Builder remainingCandidates(int value) {
            remainingCandidates = value;
            return this;
        }

        public Builder remainingTokens(int value) {
            remainingTokens = value;
            return this;
        }

        public Builder remainingLatencyNanos(long value) {
            remainingLatencyNanos = value;
            return this;
        }

        public Builder firstStageScoreGap(double value) {
            firstStageScoreGap = value;
            return this;
        }

        public Builder anchorCount(int value) {
            anchorCount = value;
            return this;
        }

        public Builder anchorConfidence(double value) {
            anchorConfidence = value;
            return this;
        }

        public Builder candidateDegree(int value) {
            candidateDegree = value;
            return this;
        }

        public Builder graphGrowthRate(double value) {
            graphGrowthRate = value;
            return this;
        }

        public Builder graphMarginalYield(double value) {
            graphMarginalYield = value;
            return this;
        }

        public Builder replanCount(int value) {
            replanCount = value;
            return this;
        }

        public RetrievalProgress build() {
            return new RetrievalProgress(this);
        }
    }
}
