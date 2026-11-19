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

package org.apache.geaflow.ai.temporal.candidate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CandidateMergerTest {

    @Test
    public void testMandatoryPriorityDeduplicationAndOriginalChannelTrace() {
        Map<String, List<Bm25Index.Hit>> channels = new LinkedHashMap<>();
        channels.put("MINHASH", Arrays.asList(hit("b", 0.8, 1), hit("c", 0.6, 2)));
        channels.put("BM25", Arrays.asList(hit("a", 9.0, 1), hit("b", 7.0, 2)));
        List<Bm25Index.Hit> result = CandidateMerger.merge(channels,
            Collections.singleton("z"), 1);
        Assertions.assertEquals(Arrays.asList("z", "b"), ids(result));
        Assertions.assertEquals(1, result.get(0).getRank());
        Assertions.assertEquals(3, result.get(0).getChannelRanks().get("RRF"));
        Bm25Index.Hit fused = result.get(1);
        Assertions.assertEquals(7.0, fused.getChannelScores().get("BM25"));
        Assertions.assertEquals(0.8, fused.getChannelScores().get("MINHASH"));
        Assertions.assertEquals(2, fused.getChannelRanks().get("BM25"));
        Assertions.assertEquals(1, fused.getChannelRanks().get("MINHASH"));
        Assertions.assertEquals(1.0 / 62 + 1.0 / 61, fused.getScore());
        Assertions.assertEquals(1, fused.getChannelRanks().get("RRF"));
        Assertions.assertEquals(2, fused.getRank());
        Assertions.assertThrows(UnsupportedOperationException.class, result::clear);
        Assertions.assertThrows(UnsupportedOperationException.class,
            () -> fused.getChannelScores().clear());
        Assertions.assertThrows(UnsupportedOperationException.class,
            () -> fused.getChannelRanks().clear());
    }

    @Test
    public void testMandatoryCandidatesAreCompleteAndInputOrderDoesNotAffectRanking() {
        List<String> mandatory = new ArrayList<>();
        for (int i = 30; i > 0; i--) {
            mandatory.add(String.format("rule-%02d", i));
        }
        Map<String, List<Bm25Index.Hit>> channels = new LinkedHashMap<>();
        channels.put("BM25", Arrays.asList(hit("soft-z", 1, 2), hit("soft-a", 2, 1)));
        channels.put("MINHASH", Collections.singletonList(hit("soft-z", 1, 1)));
        List<Bm25Index.Hit> first = CandidateMerger.merge(channels, mandatory, 1);
        Assertions.assertEquals(31, first.size());
        Assertions.assertEquals("rule-01", first.get(0).getId());
        Assertions.assertEquals("rule-30", first.get(29).getId());
        Assertions.assertEquals("soft-z", first.get(30).getId());
        Collections.reverse(mandatory);
        Map<String, List<Bm25Index.Hit>> reversed = new LinkedHashMap<>();
        reversed.put("MINHASH", channels.get("MINHASH"));
        reversed.put("BM25", Arrays.asList(hit("soft-a", 2, 1), hit("soft-z", 1, 2)));
        List<Bm25Index.Hit> second = CandidateMerger.merge(reversed, mandatory, 1);
        Assertions.assertEquals(ids(first), ids(second));
        for (int i = 0; i < first.size(); i++) {
            Assertions.assertEquals(first.get(i).getScore(), second.get(i).getScore());
            Assertions.assertEquals(first.get(i).getChannelRanks(), second.get(i).getChannelRanks());
        }
    }

    @Test
    public void testRejectsInvalidRankScoreAndDuplicateChannelEntries() {
        for (Bm25Index.Hit invalid : Arrays.asList(hit("a", Double.NaN, 1),
            hit("a", Double.POSITIVE_INFINITY, 1), hit("a", -1, 1), hit("a", 1, 0),
            hit(" ", 1, 1))) {
            Assertions.assertThrows(IllegalArgumentException.class, () -> CandidateMerger.merge(
                Collections.singletonMap("BM25", Collections.singletonList(invalid)),
                Collections.emptyList(), 1));
        }
        Assertions.assertThrows(IllegalArgumentException.class, () -> CandidateMerger.merge(
            Collections.singletonMap("BM25", Arrays.asList(hit("a", 2, 1), hit("a", 1, 2))),
            Collections.emptyList(), 1));
        Assertions.assertThrows(IllegalArgumentException.class, () -> CandidateMerger.merge(
            Collections.singletonMap("BM25", Arrays.asList(hit("a", 2, 1), hit("b", 1, 1))),
            Collections.emptyList(), 1));
        Assertions.assertThrows(IllegalArgumentException.class, () -> CandidateMerger.merge(
            Collections.singletonMap("RRF", Collections.emptyList()), Collections.emptyList(), 1));
        Assertions.assertThrows(IllegalArgumentException.class, () -> CandidateMerger.merge(
            Collections.emptyMap(), Collections.singleton(" "), 1));
        Assertions.assertThrows(IllegalArgumentException.class, () -> CandidateMerger.merge(
            Collections.emptyMap(), Collections.emptyList(), 0));
        Assertions.assertTrue(CandidateMerger.merge(Collections.singletonMap("BM25",
            Collections.singletonList(hit("a", 1, Integer.MAX_VALUE))),
            Collections.emptyList(), 1).get(0).getScore() > 0);
    }

    private static Bm25Index.Hit hit(String id, double score, int rank) {
        return new Bm25Index.Hit(id, score, rank, Collections.emptyMap(), Collections.emptyMap());
    }

    private static List<String> ids(List<Bm25Index.Hit> hits) {
        return hits.stream().map(Bm25Index.Hit::getId).collect(Collectors.toList());
    }
}
