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
package org.apache.geaflow.ai.retrieval.fusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.geaflow.ai.retrieval.model.evidence.ChannelScore;
import org.apache.geaflow.ai.retrieval.model.evidence.Evidence;
import org.apache.geaflow.ai.retrieval.model.evidence.EvidenceKind;
import org.junit.jupiter.api.Test;

class WeightedRrfFusionTest {
    @Test
    void computesWeightedRrfAndUsesStableTieBreak() {
        WeightedRrfConfig config = new WeightedRrfConfig(1.0, 1.0, 60);
        WeightedRrfFusion fusion = new WeightedRrfFusion(config);
        Evidence both = evidence("c", 1, 2);
        Evidence bm25 = evidence("b", 2, 0);
        Evidence vector = evidence("a", 0, 1);

        assertEquals(1.0 / 61.0 + 1.0 / 62.0, fusion.score(both), 1.0e-12);
        assertEquals(Arrays.asList("c", "a", "b"), ids(fusion.sort(
            Arrays.asList(bm25, vector, both))));
    }

    @Test
    void zeroWeightDoesNotAffectOrderingAndConfigurationMustBeValid() {
        WeightedRrfFusion fusion = new WeightedRrfFusion(new WeightedRrfConfig(1.0, 0.0, 60));
        Evidence vectorOnly = evidence("a", 0, 1);
        Evidence bm25 = evidence("z", 3, 1);
        assertEquals(0.0, fusion.score(vectorOnly));
        assertEquals("z", fusion.sort(Arrays.asList(vectorOnly, bm25)).get(0).getEvidenceId());
        assertThrows(IllegalArgumentException.class,
            () -> new WeightedRrfConfig(0.0, 0.0, 60));
        assertThrows(IllegalArgumentException.class,
            () -> new WeightedRrfConfig(-1.0, 1.0, 60));
    }

    @Test
    void unequalWeightsActualTiesEmptyResultsAndLargeConstant() {
        WeightedRrfFusion fusion = new WeightedRrfFusion(new WeightedRrfConfig(2.0, 0.5, 10));
        Evidence both = evidence("both", 1, 2);
        assertEquals(2.0 / 11 + 0.5 / 12, fusion.score(both), 1.0e-12);
        java.util.List<Evidence> tied = fusion.sort(Arrays.asList(evidence("z", 1, 0), evidence("a", 1, 0)));
        assertEquals(Arrays.asList("a", "z"), ids(tied));
        assertEquals(1, tied.get(0).getRank());
        assertEquals(2, tied.get(1).getRank());
        assertEquals(Collections.emptyList(), fusion.sort(Collections.emptyList()));
        assertEquals(both.getStageScores(), fusion.sort(Collections.singletonList(both)).get(0).getStageScores());
        assertEquals(1.0 / ((double) Integer.MAX_VALUE + 1),
            new WeightedRrfFusion(new WeightedRrfConfig(1, 0, Integer.MAX_VALUE)).score(evidence("a", 1, 0)), 1.0e-20);
    }

    private static Evidence evidence(String id, int bm25Rank, int vectorRank) {
        Map<String, ChannelScore> scores = new LinkedHashMap<>();
        if (bm25Rank > 0) {
            scores.put("BM25", new ChannelScore("BM25", 10.0, null, bm25Rank));
        }
        if (vectorRank > 0) {
            scores.put("VECTOR", new ChannelScore("VECTOR", 0.9, null, vectorRank));
        }
        double fused = (bm25Rank == 0 ? 0 : 1.0 / (60 + bm25Rank))
            + (vectorRank == 0 ? 0 : 1.0 / (60 + vectorRank));
        return new Evidence(id, EvidenceKind.CHUNK, id, Collections.emptyList(),
            Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), scores,
            fused, 1);
    }

    private static java.util.List<String> ids(java.util.List<Evidence> values) {
        java.util.List<String> result = new java.util.ArrayList<>();
        for (Evidence value : values) {
            result.add(value.getEvidenceId());
        }
        return result;
    }
}
