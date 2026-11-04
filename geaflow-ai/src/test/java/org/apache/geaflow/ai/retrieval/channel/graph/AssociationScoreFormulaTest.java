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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Pins the documented association-score formula numerically.
 *
 * <p>The design fixes:
 *
 * <pre>
 * associationScore(path) = anchorScore * product(edgeStrength) * hopDecay^(hop - 1)
 *                          * degreePenalty * sourceRelevance
 * degreePenalty(vertex)  = 1 / log(2 + degree(vertex))
 * </pre>
 *
 * <p>These tests assert the expression itself rather than incidental behaviour of an expansion, so
 * a changed constant, a changed exponent, or a changed decay base fails here first.
 */
class AssociationScoreFormulaTest {

    private static final double TOLERANCE = 1.0e-12;

    @Test
    void degreePenaltyMatchesTheDocumentedExpression() {
        // degreePenalty(v) = 1 / log(2 + degree(v))
        assertEquals(1.0D / Math.log(2.0D), AssociationScore.degreePenalty(0), TOLERANCE);
        assertEquals(1.0D / Math.log(3.0D), AssociationScore.degreePenalty(1), TOLERANCE);
        assertEquals(1.0D / Math.log(10.0D), AssociationScore.degreePenalty(8), TOLERANCE);
        assertEquals(1.0D / Math.log(102.0D), AssociationScore.degreePenalty(100), TOLERANCE);
    }

    @Test
    void degreePenaltyDecreasesMonotonicallyWithDegree() {
        double previous = Double.MAX_VALUE;
        for (int degree = 0; degree <= 500; degree++) {
            double penalty = AssociationScore.degreePenalty(degree);
            assertTrue(penalty <= previous,
                "degree penalty must not increase with degree, failed at degree " + degree);
            assertTrue(penalty > 0.0D, "degree penalty must stay positive at degree " + degree);
            previous = penalty;
        }
    }

    @Test
    void degreePenaltyStaysPositiveAndFallsBelowOneFromDegreeOne() {
        // 1 / log(2) alone exceeds one, so only degree zero sits above the unit bound.
        assertEquals(1.0D / Math.log(2.0D), AssociationScore.degreePenalty(0), TOLERANCE);
        assertTrue(AssociationScore.degreePenalty(0) > 1.0D);
        for (int degree : new int[] {1, 2, 7, 64, 1_000, 100_000}) {
            double penalty = AssociationScore.degreePenalty(degree);
            assertTrue(penalty > 0.0D, "penalty must stay positive at degree " + degree);
            assertTrue(penalty < 1.0D, "penalty must fall below 1.0 at degree " + degree);
        }
    }

    @Test
    void unitExponentReproducesTheDocumentedFormulaExactly() {
        for (int degree : new int[] {0, 1, 3, 15, 240}) {
            assertEquals(AssociationScore.degreePenalty(degree),
                AssociationScore.degreePenalty(degree, 1.0D), TOLERANCE);
        }
    }

    @Test
    void configuredExponentRaisesTheDocumentedPenalty() {
        // The documented formula is exponent 1.0; zero disables the term, other values rescale it.
        assertEquals(1.0D, AssociationScore.degreePenalty(42, 0.0D), TOLERANCE);
        assertEquals(Math.pow(1.0D / Math.log(5.0D), 2.0D),
            AssociationScore.degreePenalty(3, 2.0D), TOLERANCE);
        assertEquals(Math.pow(1.0D / Math.log(5.0D), 0.5D),
            AssociationScore.degreePenalty(3, 0.5D), TOLERANCE);
    }

    @Test
    void hopDecayExponentStartsAtZeroForTheFirstHop() {
        // hopDecay^(hop - 1): hop one carries no decay, hop two carries exactly one factor.
        assertEquals(10.0D, AssociationScore.applyHopDecay(10.0D, 1, 0.8D), TOLERANCE);
        assertEquals(8.0D, AssociationScore.applyHopDecay(10.0D, 2, 0.8D), TOLERANCE);
        assertEquals(6.4D, AssociationScore.applyHopDecay(10.0D, 3, 0.8D), TOLERANCE);
        assertEquals(10.0D * Math.pow(0.8D, 4.0D),
            AssociationScore.applyHopDecay(10.0D, 5, 0.8D), TOLERANCE);
    }

    @Test
    void fullFormulaMatchesAHandComputedSingleHopScore() {
        // anchor 2.0 -> edge strength 1.5 -> vertex of degree 8 -> full source overlap 1.2
        double expected = 2.0D
            * Math.pow(0.8D, 0.0D)
            * 1.5D
            * (1.0D / Math.log(10.0D))
            * 1.2D;
        double actual = AssociationScore.score(2.0D, 1, 1.5D, 8, 1.0D, 0.8D, 1.2D);
        assertEquals(expected, actual, TOLERANCE);
    }

    @Test
    void fullFormulaMatchesAHandComputedTwoHopScore() {
        // hop1: root 1.0 -> edge 1.4 into degree 0; the documented degree penalty applies here too.
        double hopOne = AssociationScore.score(1.0D, 1, 1.4D, 0, 1.0D, 0.8D, 1.0D);
        assertEquals(1.4D / Math.log(2.0D), hopOne, TOLERANCE);

        // hop2 applies exactly one hopDecay factor and the second vertex degree penalty
        double hopTwo = AssociationScore.score(hopOne, 2, 0.9D, 31, 1.0D, 0.8D, 1.0D);
        double expected = (1.4D / Math.log(2.0D)) * 0.8D * 0.9D * (1.0D / Math.log(33.0D));
        assertEquals(expected, hopTwo, TOLERANCE);
    }

    @Test
    void scoreIsMultiplicativeInEveryDocumentedTerm() {
        double baseline = AssociationScore.score(1.0D, 2, 1.0D, 4, 1.0D, 0.5D, 1.0D);
        assertEquals(baseline * 2.0D,
            AssociationScore.score(2.0D, 2, 1.0D, 4, 1.0D, 0.5D, 1.0D), TOLERANCE);
        assertEquals(baseline * 3.0D,
            AssociationScore.score(1.0D, 2, 3.0D, 4, 1.0D, 0.5D, 1.0D), TOLERANCE);
        assertEquals(baseline * 0.25D,
            AssociationScore.score(1.0D, 2, 1.0D, 4, 1.0D, 0.5D, 0.25D), TOLERANCE);
    }

    @Test
    void rejectsInvalidInputs() {
        assertThrows(IllegalArgumentException.class, () -> AssociationScore.degreePenalty(-1));
        assertThrows(IllegalArgumentException.class, () -> AssociationScore.degreePenalty(3, -0.5D));
        assertThrows(IllegalArgumentException.class,
            () -> AssociationScore.degreePenalty(3, Double.NaN));
        assertThrows(IllegalArgumentException.class, () -> AssociationScore.applyHopDecay(1.0D, 0, 0.8D));
        assertThrows(IllegalArgumentException.class, () -> AssociationScore.applyHopDecay(1.0D, 1, 1.5D));
        assertThrows(IllegalArgumentException.class, () -> AssociationScore.applyHopDecay(1.0D, 1, -0.1D));
    }
}