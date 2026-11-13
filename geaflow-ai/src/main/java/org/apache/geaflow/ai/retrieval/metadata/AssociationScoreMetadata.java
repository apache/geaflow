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
package org.apache.geaflow.ai.retrieval.metadata;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Versioned components of the graph association score, recorded in build metadata.
 *
 * <p>The design requires the association-score components and their versions to be emitted in build
 * metadata:
 *
 * <pre>
 * associationScore(path) = anchorScore * product(edgeStrength) * hopDecay^(hop-1)
 *                          * degreePenalty * sourceRelevance
 * degreePenalty(vertex) = 1 / log(2 + degree(vertex))
 * </pre>
 *
 * <p>Without these values a published graph cannot be scored the same way twice, and a ranking
 * change cannot be attributed to a configuration change rather than to different input data.
 */
public final class AssociationScoreMetadata {
    private final double hopDecay;
    private final double degreePenalty;
    private final double anchorScoreWeight;
    private final double edgeStrengthWeight;
    private final double sourceRelevanceWeight;
    private final Map<String, Double> relationPriorities;
    private final String edgeStrengthVersion;
    private final String scoreVersion;
    private final String timeDecayVersion;
    private final double timeDecayWeight;
    private final double timeDecayHalfLifeMillis;
    private final double unrelatedSourceRelevance;
    private final double anchorHitBaseScore;

    /**
     * Creates the recorded association-score components.
     *
     * @param hopDecay per-hop multiplicative decay applied to path score
     * @param degreePenalty exponent applied to {@code 1 / log(2 + degree)}
     * @param anchorScoreWeight weight of the anchor score term
     * @param edgeStrengthWeight weight of the edge-strength term
     * @param sourceRelevanceWeight weight of the source-relevance term
     * @param relationPriorities relation-type prior keyed by edge label
     * @param edgeStrengthVersion version of the edge-strength model
     * @param scoreVersion version of the whole association-score formula
     */
    public AssociationScoreMetadata(double hopDecay, double degreePenalty,
                                    double anchorScoreWeight, double edgeStrengthWeight,
                                    double sourceRelevanceWeight,
                                    Map<String, Double> relationPriorities,
                                    String edgeStrengthVersion, String scoreVersion) {
        this(hopDecay, degreePenalty, anchorScoreWeight, edgeStrengthWeight, sourceRelevanceWeight,
            relationPriorities, edgeStrengthVersion, scoreVersion, null, 0.0D, 0.0D, 0.5D, 1.0D);
    }

    /**
     * Creates the recorded association-score components including the optional time decay.
     *
     * <p>The time-decay components were missing from the first revision even though the design
     * lists an optional time-decay factor among the edge-strength inputs. They are recorded here so
     * a replay can tell which decay configuration produced a ranking.
     *
     * @param hopDecay per-hop multiplicative decay applied to path score
     * @param degreePenalty exponent applied to {@code 1 / log(2 + degree)}
     * @param anchorScoreWeight weight of the anchor score term
     * @param edgeStrengthWeight weight of the edge-strength term
     * @param sourceRelevanceWeight weight of the source-relevance term
     * @param relationPriorities relation-type prior keyed by edge label
     * @param edgeStrengthVersion version of the edge-strength model
     * @param scoreVersion version of the whole association-score formula
     * @param timeDecayVersion version of the time-decay model, nullable for legacy artifacts
     * @param timeDecayWeight how strongly decay is applied, in {@code [0,1]}
     * @param timeDecayHalfLifeMillis half-life of the decay in milliseconds
     */
    public AssociationScoreMetadata(double hopDecay, double degreePenalty,
                                    double anchorScoreWeight, double edgeStrengthWeight,
                                    double sourceRelevanceWeight,
                                    Map<String, Double> relationPriorities,
                                    String edgeStrengthVersion, String scoreVersion,
                                    String timeDecayVersion, double timeDecayWeight,
                                    double timeDecayHalfLifeMillis) {
        this(hopDecay, degreePenalty, anchorScoreWeight, edgeStrengthWeight, sourceRelevanceWeight,
            relationPriorities, edgeStrengthVersion, scoreVersion, timeDecayVersion, timeDecayWeight,
            timeDecayHalfLifeMillis, 0.5D, 1.0D);
    }

    /**
     * Creates the recorded association-score components including the source-relevance multipliers.
     *
     * <p>{@code sourceRelevance} is a multiplier of the documented formula, but its two coefficients
     * were constants in the retriever, so a published score could not be reproduced from metadata
     * alone. They are recorded here for the same reason as the other components.
     *
     * @param hopDecay per-hop multiplicative decay applied to path score
     * @param degreePenalty exponent applied to {@code 1 / log(2 + degree)}
     * @param anchorScoreWeight weight of the anchor score term
     * @param edgeStrengthWeight weight of the edge-strength term
     * @param sourceRelevanceWeight weight of the source-relevance term
     * @param relationPriorities relation-type prior keyed by edge label
     * @param edgeStrengthVersion version of the edge-strength model
     * @param scoreVersion version of the whole association-score formula
     * @param timeDecayVersion version of the time-decay model
     * @param timeDecayWeight how strongly decay is applied, in {@code [0,1]}
     * @param timeDecayHalfLifeMillis half-life of the decay in milliseconds
     * @param unrelatedSourceRelevance relevance when anchor and edge share no source chunk
     * @param anchorHitBaseScore base relevance when anchor and edge share at least one chunk
     */
    public AssociationScoreMetadata(double hopDecay, double degreePenalty,
                                    double anchorScoreWeight, double edgeStrengthWeight,
                                    double sourceRelevanceWeight,
                                    Map<String, Double> relationPriorities,
                                    String edgeStrengthVersion, String scoreVersion,
                                    String timeDecayVersion, double timeDecayWeight,
                                    double timeDecayHalfLifeMillis, double unrelatedSourceRelevance,
                                    double anchorHitBaseScore) {
        this.hopDecay = requireUnitInterval(hopDecay, "hopDecay");
        this.degreePenalty = requireNonNegative(degreePenalty, "degreePenalty");
        this.anchorScoreWeight = requireNonNegative(anchorScoreWeight, "anchorScoreWeight");
        this.edgeStrengthWeight = requireNonNegative(edgeStrengthWeight, "edgeStrengthWeight");
        this.sourceRelevanceWeight = requireNonNegative(sourceRelevanceWeight, "sourceRelevanceWeight");
        Map<String, Double> priorities = new LinkedHashMap<>();
        if (relationPriorities != null) {
            relationPriorities.forEach((label, value) -> {
                Objects.requireNonNull(label, "relation label");
                priorities.put(label, requireNonNegative(value, "relation priority"));
            });
        }
        this.relationPriorities = Collections.unmodifiableMap(priorities);
        this.edgeStrengthVersion = Objects.requireNonNull(edgeStrengthVersion, "edgeStrengthVersion");
        this.scoreVersion = Objects.requireNonNull(scoreVersion, "scoreVersion");
        this.timeDecayVersion = timeDecayVersion;
        this.timeDecayWeight = requireUnitInterval(timeDecayWeight, "timeDecayWeight");
        this.timeDecayHalfLifeMillis = requireNonNegativeFinite(timeDecayHalfLifeMillis,
            "timeDecayHalfLifeMillis");
        this.unrelatedSourceRelevance = requireUnitInterval(unrelatedSourceRelevance,
            "unrelatedSourceRelevance");
        this.anchorHitBaseScore = requireNonNegativeFinite(anchorHitBaseScore, "anchorHitBaseScore");
    }

    /** Returns the recorded components for the default graph retrieval configuration. */
    public static AssociationScoreMetadata defaults() {
        return new AssociationScoreMetadata(0.8D, 1.0D, 1.0D, 1.0D, 1.0D,
            Collections.emptyMap(), "relation-prior-v2", "association-score-v2",
            "time-decay-v1", 0.0D, 86_400_000.0D, 0.5D, 1.0D);
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

    public Map<String, Double> getRelationPriorities() {
        return relationPriorities;
    }

    public String getEdgeStrengthVersion() {
        return edgeStrengthVersion;
    }

    public String getScoreVersion() {
        return scoreVersion;
    }

    /**
     * Returns the recorded time-decay model version, null for artifacts published without one.
     */
    public String getTimeDecayVersion() {
        return timeDecayVersion;
    }

    /**
     * Returns how strongly the recorded time decay is applied, in {@code [0,1]}.
     */
    public double getTimeDecayWeight() {
        return timeDecayWeight;
    }

    /**
     * Returns the recorded time-decay half-life in milliseconds.
     */
    public double getTimeDecayHalfLifeMillis() {
        return timeDecayHalfLifeMillis;
    }

    /**
     * Returns the recorded relevance applied when an anchor and edge share no source chunk.
     */
    public double getUnrelatedSourceRelevance() {
        return unrelatedSourceRelevance;
    }

    /**
     * Returns the recorded base relevance when an anchor and edge share at least one chunk.
     */
    public double getAnchorHitBaseScore() {
        return anchorHitBaseScore;
    }

    private static double requireNonNegative(double value, String name) {
        if (!Double.isFinite(value) || value < 0.0D) {
            throw MetadataValidation.invalid(name + " must be finite and non-negative");
        }
        return value;
    }

    private static double requireNonNegativeFinite(double value, String name) {
        if (!Double.isFinite(value) || value < 0.0D) {
            throw MetadataValidation.invalid(name + " must be finite and non-negative");
        }
        return value;
    }

    private static double requireUnitInterval(double value, String name) {
        if (!Double.isFinite(value) || value < 0.0D || value > 1.0D) {
            throw MetadataValidation.invalid(name + " must be in [0,1]");
        }
        return value;
    }
}