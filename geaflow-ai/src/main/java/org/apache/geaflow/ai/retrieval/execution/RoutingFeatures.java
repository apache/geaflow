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
import java.util.Map;
import java.util.Objects;

/** Fixed, versioned features consumed by an adaptive retrieval policy. */
public final class RoutingFeatures {
    public static final String SCHEMA_VERSION = "routing-features-v1";

    private final String schemaVersion;
    private final int queryLength;
    private final double averageIdf;
    private final double firstStageScoreGap;
    private final int anchorCount;
    private final double anchorConfidence;
    private final int candidateDegree;
    private final double graphGrowthRate;

    private RoutingFeatures(Builder builder) {
        schemaVersion = builder.schemaVersion;
        queryLength = builder.queryLength;
        averageIdf = builder.averageIdf;
        firstStageScoreGap = builder.firstStageScoreGap;
        anchorCount = builder.anchorCount;
        anchorConfidence = builder.anchorConfidence;
        candidateDegree = builder.candidateDegree;
        graphGrowthRate = builder.graphGrowthRate;
        validate();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static RoutingFeatures empty() {
        return builder().build();
    }

    private void validate() {
        if (!SCHEMA_VERSION.equals(schemaVersion) || queryLength < 0 || anchorCount < 0
            || candidateDegree < 0 || !finiteNonNegative(averageIdf)
            || !finiteNonNegative(firstStageScoreGap) || !finiteNonNegative(anchorConfidence)
            || anchorConfidence > 1.0D || !finiteNonNegative(graphGrowthRate)) {
            throw new IllegalArgumentException("invalid routing features");
        }
    }

    private static boolean finiteNonNegative(double value) {
        return Double.isFinite(value) && value >= 0.0D;
    }

    public String getSchemaVersion() {
        return schemaVersion;
    }

    public int getQueryLength() {
        return queryLength;
    }

    public double getAverageIdf() {
        return averageIdf;
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

    public Map<String, Number> asMap() {
        Map<String, Number> values = new LinkedHashMap<>();
        values.put("query_length", queryLength);
        values.put("average_idf", averageIdf);
        values.put("first_stage_score_gap", firstStageScoreGap);
        values.put("anchor_count", anchorCount);
        values.put("anchor_confidence", anchorConfidence);
        values.put("candidate_degree", candidateDegree);
        values.put("graph_growth_rate", graphGrowthRate);
        return Collections.unmodifiableMap(values);
    }

    public Map<String, Number> toMap() {
        return asMap();
    }

    public double get(String name) {
        Number value = asMap().get(name);
        if (value == null) {
            throw new IllegalArgumentException("unknown routing feature: " + name);
        }
        return value.doubleValue();
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof RoutingFeatures)) {
            return false;
        }
        RoutingFeatures that = (RoutingFeatures) other;
        return queryLength == that.queryLength && Double.compare(averageIdf, that.averageIdf) == 0
            && Double.compare(firstStageScoreGap, that.firstStageScoreGap) == 0
            && anchorCount == that.anchorCount
            && Double.compare(anchorConfidence, that.anchorConfidence) == 0
            && candidateDegree == that.candidateDegree
            && Double.compare(graphGrowthRate, that.graphGrowthRate) == 0
            && Objects.equals(schemaVersion, that.schemaVersion);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schemaVersion, queryLength, averageIdf, firstStageScoreGap,
            anchorCount, anchorConfidence, candidateDegree, graphGrowthRate);
    }

    /** Builder for the fixed feature schema. */
    public static final class Builder {
        private String schemaVersion = SCHEMA_VERSION;
        private int queryLength;
        private double averageIdf;
        private double firstStageScoreGap;
        private int anchorCount;
        private double anchorConfidence;
        private int candidateDegree;
        private double graphGrowthRate;

        public Builder schemaVersion(String value) {
            schemaVersion = value;
            return this;
        }

        public Builder queryLength(int value) {
            queryLength = value;
            return this;
        }

        public Builder averageIdf(double value) {
            averageIdf = value;
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

        public RoutingFeatures build() {
            return new RoutingFeatures(this);
        }
    }
}
