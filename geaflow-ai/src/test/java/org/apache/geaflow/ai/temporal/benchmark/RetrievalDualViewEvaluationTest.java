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

package org.apache.geaflow.ai.temporal.benchmark;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class RetrievalDualViewEvaluationTest {

    @Test
    public void testThreeToOneRrfUsesRanksInsteadOfLogitMagnitudes() {
        // Title ranks A..F and body ranks B..F,A give B > A > C with k=60 and 3:1 weights.
        Assertions.assertEquals(Arrays.asList(1, 0, 2, 3, 4, 5),
            RetrievalDualViewEvaluation.rankCandidates(candidates("A", "B", "C", "D", "E", "F"),
                new double[] {1000000, 5, 4, 3, 2, 1},
                new double[] {-1000000, 1000, 999, 998, 997, 996}, new boolean[6]));
    }

    @Test
    public void testRulesStayFirstInOriginalPoolOrder() {
        Assertions.assertEquals(Arrays.asList(3, 4, 0, 2, 1),
            RetrievalDualViewEvaluation.rankCandidates(candidates("z", "b", "a", "rule-z", "rule-a"),
                new double[] {-1, -2, -2, -99, -100},
                new double[] {-100, -3, -3, -2, -1},
                new boolean[] {false, false, false, true, true}));
    }

    @Test
    public void testPureTitleWeightPreservesBaselineIncludingTiesAndNegativeScores() {
        final JsonArray candidates = candidates("z", "b", "a", "plus", "minus", "rule-z", "rule-a");
        final double[] title = {-1, -2, -2, 0.0, -0.0, -100, -99};
        final boolean[] rules = {false, false, false, false, false, true, true};
        final List<Integer> expected = Arrays.asList(5, 6, 4, 3, 0, 2, 1);
        Assertions.assertEquals(expected, RetrievalBaselineEvaluation.rankCandidates(candidates, title, rules));
        Assertions.assertEquals(expected, RetrievalDualViewEvaluation.rankCandidates(candidates, title,
            new double[] {100, 99, 98, 97, 96, 95, 94}, rules, 1.0));
    }

    @Test
    public void testCeRanksUseEvidenceIdForEqualLogitsIncludingSignedZero() {
        Assertions.assertArrayEquals(new int[] {1, 3, 2, 4}, RetrievalDualViewEvaluation.ceRanks(
            candidates("z", "b", "a", "last"), new double[] {-1, -2, -2, -3}));
        Assertions.assertArrayEquals(new int[] {2, 1}, RetrievalDualViewEvaluation.ceRanks(
            candidates("z", "a"), new double[] {0.0, -0.0}));
    }

    @Test
    public void testEqualRrfScoresUseTitleRankBeforeOriginalPoolOrder() {
        final String[] ids = new String[126];
        final double[] title = new double[126];
        final double[] body = new double[126];
        for (int index = 0; index < ids.length; index++) {
            ids[index] = "e" + index;
            title[index] = -index;
            body[index] = -index;
        }
        title[125] = -122;
        title[122] = -125;
        body[125] = 1;
        body[122] = 2;
        // (title 123, body 2) and (title 126, body 1) tie: .75/183=.25/61 and .75/186=.25/62.
        final List<Integer> order = RetrievalDualViewEvaluation.rankCandidates(candidates(ids),
            title, body, new boolean[126]);
        Assertions.assertTrue(order.indexOf(125) < order.indexOf(122));
    }

    @Test
    public void testInvalidScoresAndPoolSizesAreRejected() {
        final JsonArray candidates = candidates("a");
        for (double value : new double[] {Double.NaN, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY}) {
            Assertions.assertThrows(IllegalArgumentException.class,
                () -> RetrievalDualViewEvaluation.ceRanks(candidates, new double[] {value}));
            Assertions.assertThrows(IllegalArgumentException.class,
                () -> RetrievalDualViewEvaluation.rankCandidates(candidates, new double[] {value},
                    new double[] {0}, new boolean[1]));
            Assertions.assertThrows(IllegalArgumentException.class,
                () -> RetrievalDualViewEvaluation.rankCandidates(candidates, new double[] {0},
                    new double[] {value}, new boolean[1]));
        }
        Assertions.assertThrows(IllegalArgumentException.class,
            () -> RetrievalDualViewEvaluation.ceRanks(candidates, new double[0]));
        Assertions.assertThrows(IllegalArgumentException.class,
            () -> RetrievalDualViewEvaluation.rankCandidates(candidates, new double[0],
                new double[1], new boolean[1]));
        Assertions.assertThrows(IllegalArgumentException.class,
            () -> RetrievalDualViewEvaluation.rankCandidates(candidates, new double[1],
                new double[0], new boolean[1]));
        Assertions.assertThrows(IllegalArgumentException.class,
            () -> RetrievalDualViewEvaluation.rankCandidates(candidates, new double[1],
                new double[1], new boolean[0]));
        final JsonArray oversized = candidates(new String[201]);
        Assertions.assertThrows(IllegalArgumentException.class,
            () -> RetrievalDualViewEvaluation.rankCandidates(oversized, new double[201],
                new double[201], new boolean[201]));
    }

    @Test
    public void testRankingPreservesAllTwoHundredCandidates() {
        final String[] ids = new String[200];
        final double[] logits = new double[200];
        final List<Integer> expected = new ArrayList<>();
        for (int index = 0; index < ids.length; index++) {
            ids[index] = "e" + index;
            logits[index] = -index;
            expected.add(index);
        }
        Assertions.assertEquals(expected, RetrievalDualViewEvaluation.rankCandidates(candidates(ids),
            logits, logits, new boolean[200]));
        Assertions.assertEquals(Collections.emptyList(), RetrievalDualViewEvaluation.rankCandidates(
            candidates(), new double[0], new double[0], new boolean[0]));
    }

    private static JsonArray candidates(String... ids) {
        final JsonArray result = new JsonArray();
        for (int index = 0; index < ids.length; index++) {
            final JsonObject candidate = new JsonObject();
            candidate.addProperty("evidenceId", ids[index] == null ? "e" + index : ids[index]);
            candidate.addProperty("text", "Article\nOriginal body " + index);
            result.add(candidate);
        }
        return result;
    }
}
