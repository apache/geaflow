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

/**
 * Pure functions for the bounded local association score.
 *
 * <p>The design fixes the shape of the score and the associative-domain penalty:
 *
 * <pre>
 * associationScore(path) = anchorScore * product(edgeStrength) * hopDecay^(hop - 1)
 *                          * degreePenalty * sourceRelevance
 * degreePenalty(vertex)  = 1 / log(2 + degree(vertex))
 * </pre>
 *
 * <p>These are kept as static pure helpers so the formula can be asserted numerically without
 * standing up anchors, artifacts, or a neighbor provider. A previous revision inlined the formula
 * into {@link GraphRetriever}, which meant the documented constants were only reachable through a
 * full expansion and were never checked against the documented expression.
 */
public final class AssociationScore {

    /**
     * Version of the formula implemented here, recorded as build metadata {@code scoreVersion}.
     */
    public static final String SCORE_VERSION = "association-score-v2";

    private AssociationScore() {
    }

    /**
     * Computes the documented per-vertex associative-domain penalty.
     *
     * @param degree degree of the vertex being entered
     * @return {@code 1 / log(2 + degree)}, which is {@code 1.0} at degree zero
     */
    public static double degreePenalty(int degree) {
        if (degree < 0) {
            throw new IllegalArgumentException("degree must be non-negative");
        }
        return 1.0D / Math.log(2.0D + degree);
    }

    /**
     * Applies the configured exponent to the documented degree penalty.
     *
     * <p>An exponent of {@code 1.0} reproduces the documented formula exactly; {@code 0.0} disables
     * the penalty entirely, which is how the configuration turns the term off.
     *
     * @param degree degree of the vertex being entered
     * @param exponent configured exponent, non-negative
     * @return {@code (1 / log(2 + degree))^exponent}
     */
    public static double degreePenalty(int degree, double exponent) {
        if (!Double.isFinite(exponent) || exponent < 0.0D) {
            throw new IllegalArgumentException("degreePenalty exponent must be finite and non-negative");
        }
        return Math.pow(degreePenalty(degree), exponent);
    }

    /**
     * Applies the configured hop decay to a score entering {@code hop}.
     *
     * <p>The first edge is hop one and carries no decay; the exponent is therefore {@code hop - 1}.
     *
     * @param score score accumulated so far
     * @param hop one-based hop being entered, at least one
     * @param hopDecay configured per-hop multiplicative decay in {@code [0,1]}
     * @return {@code score * hopDecay^(hop - 1)}
     */
    public static double applyHopDecay(double score, int hop, double hopDecay) {
        if (hop < 1) {
            throw new IllegalArgumentException("hop must be at least one");
        }
        if (!Double.isFinite(hopDecay) || hopDecay < 0.0D || hopDecay > 1.0D) {
            throw new IllegalArgumentException("hopDecay must be in [0,1]");
        }
        return score * Math.pow(hopDecay, hop - 1);
    }

    /**
     * Combines the terms of the documented formula for one edge traversal.
     *
     * @param pathScore accumulated score of the path reaching the current vertex
     * @param hop one-based hop being entered
     * @param edgeStrength relation prior combined with any reinforcement and time decay
     * @param targetDegree degree of the vertex being entered
     * @param degreeExponent configured exponent applied to the documented degree penalty
     * @param hopDecay configured per-hop multiplicative decay
     * @param sourceRelevance source-overlap relevance term already weighted
     * @return the association score of the path after entering this edge
     */
    public static double score(double pathScore, int hop, double edgeStrength, int targetDegree,
                               double degreeExponent, double hopDecay, double sourceRelevance) {
        return applyHopDecay(pathScore, hop, hopDecay)
            * edgeStrength
            * degreePenalty(targetDegree, degreeExponent)
            * sourceRelevance;
    }
}