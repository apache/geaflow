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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class MinHashIndexTest {

    @Test
    public void testStableRankingAcrossInputOrderAndCallerMutation() {
        final Map<String, Map<String, String>> documents = new LinkedHashMap<>();
        for (String id : Arrays.asList("z", "b", "a")) {
            documents.put(id, Collections.singletonMap("evidence", "alpha beta gamma delta"));
        }
        final MinHashIndex first = new MinHashIndex(documents);
        final Map<String, Map<String, String>> reordered = new LinkedHashMap<>();
        for (String id : Arrays.asList("a", "b", "z")) {
            reordered.put(id, documents.get(id));
        }
        final MinHashIndex second = new MinHashIndex(reordered);
        documents.clear();
        final List<Bm25Index.Hit> hits = first.search("alpha beta gamma delta", 2);
        Assertions.assertEquals(Arrays.asList("a", "b"), ids(hits));
        Assertions.assertEquals(ids(hits), ids(second.search("alpha beta gamma delta", 2)));
        Assertions.assertEquals(1.0, hits.get(0).getScore());
        Assertions.assertEquals(1, hits.get(0).getRank());
        Assertions.assertEquals(2, hits.get(1).getRank());
        Assertions.assertEquals(Collections.singletonMap("MINHASH", 1.0),
            hits.get(0).getChannelScores());
        Assertions.assertEquals(Collections.singletonMap("MINHASH", 2),
            hits.get(1).getChannelRanks());
        Assertions.assertThrows(UnsupportedOperationException.class, hits::clear);
    }

    @Test
    public void testUnicodePunctuationAndRepeatedShingleSets() {
        final Map<String, Map<String, String>> documents = new LinkedHashMap<>();
        documents.put("MixedCase-ID", Collections.singletonMap("evidence", "Café—CLIMATE…2024"));
        documents.put("repeated", Collections.singletonMap("evidence", "echo echo echo echo"));
        final MinHashIndex index = new MinHashIndex(documents);
        Assertions.assertEquals(Collections.singletonList("MixedCase-ID"),
            ids(index.search("CAFE\u0301\u00a0climate\u20032024", 20)));
        Assertions.assertEquals("Café—CLIMATE…2024", documents.get("MixedCase-ID").get("evidence"));
        Assertions.assertEquals(1.0, index.search("echo echo echo", 20).get(0).getScore());
        Assertions.assertEquals(3,
            MinHashIndex.shingles("alpha beta gamma alpha beta gamma").size());
        Assertions.assertEquals(MinHashIndex.shingles("alpha beta gamma"),
            MinHashIndex.shingles("ALPHA / BETA, GAMMA"));
    }

    @Test
    public void testFieldsNeverCreateCrossFieldShinglesAndShortTextsNeverMatch() {
        final Map<String, String> split = new LinkedHashMap<>();
        split.put("subject", "alpha beta");
        split.put("predicate", "gamma delta");
        split.put("evidence", "");
        final Map<String, String> duplicated = new LinkedHashMap<>();
        duplicated.put("object", "alpha beta gamma");
        duplicated.put("evidence", "alpha beta gamma");
        final Map<String, Map<String, String>> documents = new LinkedHashMap<>();
        documents.put("split", split);
        documents.put("duplicated", duplicated);
        final MinHashIndex index = new MinHashIndex(documents);
        Assertions.assertEquals(Collections.singletonList("duplicated"),
            ids(index.search("alpha beta gamma", 20)));
        Assertions.assertEquals(1.0, index.search("alpha beta gamma", 20).get(0).getScore());
        Assertions.assertTrue(index.search("alpha beta", 20).isEmpty());
        Assertions.assertTrue(index.search(" \u2003...", 20).isEmpty());
        Assertions.assertTrue(new MinHashIndex(Collections.emptyMap())
            .search("alpha beta gamma", 20).isEmpty());
        Assertions.assertEquals(0.0, MinHashIndex.jaccard(Collections.emptySet(),
            Collections.emptySet()));
        Assertions.assertEquals(0.0, MinHashIndex.jaccard(Collections.singleton("a"),
            Collections.emptySet()));
    }

    @Test
    public void testExactJaccardBoundaryAndLshFalsePositiveVerification() {
        final String query = "alpha beta gamma delta";
        final String boundary = collidingExtension(query, false);
        final String below = collidingExtension(query, true);
        Assertions.assertEquals(0.5, MinHashIndex.jaccard(MinHashIndex.shingles(query),
            MinHashIndex.shingles(boundary)));
        Assertions.assertEquals(0.4, MinHashIndex.jaccard(MinHashIndex.shingles(query),
            MinHashIndex.shingles(below)));
        final Map<String, Map<String, String>> documents = new LinkedHashMap<>();
        documents.put("boundary", Collections.singletonMap("evidence", boundary));
        documents.put("below", Collections.singletonMap("evidence", below));
        documents.put("exact", Collections.singletonMap("evidence", query));
        final List<Bm25Index.Hit> hits = new MinHashIndex(documents).search(query, 20);
        Assertions.assertEquals(Arrays.asList("exact", "boundary"), ids(hits));
        Assertions.assertEquals(0.5, hits.get(1).getScore());
        Assertions.assertEquals(2, hits.get(1).getRank());
        final Set<String> left = new HashSet<>(Arrays.asList("a", "b", "c"));
        final Set<String> right = new HashSet<>(Arrays.asList("b", "c", "d"));
        Assertions.assertEquals(0.5, MinHashIndex.jaccard(left, right));
        Assertions.assertEquals(MinHashIndex.jaccard(left, right),
            MinHashIndex.jaccard(right, left));
    }

    @Test
    public void testInvalidInputs() {
        Assertions.assertThrows(NullPointerException.class, () -> new MinHashIndex(null));
        Assertions.assertThrows(NullPointerException.class, () -> new MinHashIndex(
            Collections.singletonMap(null, Collections.singletonMap("evidence", "a b c"))));
        Assertions.assertThrows(IllegalArgumentException.class, () -> new MinHashIndex(
            Collections.singletonMap(" ", Collections.singletonMap("evidence", "a b c"))));
        Assertions.assertThrows(NullPointerException.class, () -> new MinHashIndex(
            Collections.singletonMap("id", null)));
        Assertions.assertThrows(IllegalArgumentException.class, () -> new MinHashIndex(
            Collections.singletonMap("id", Collections.singletonMap("unknown", "a b c"))));
        Assertions.assertThrows(NullPointerException.class, () -> new MinHashIndex(
            Collections.singletonMap("id", Collections.singletonMap("evidence", null))));
        final MinHashIndex index = new MinHashIndex(Collections.emptyMap());
        Assertions.assertThrows(NullPointerException.class, () -> index.search(null, 20));
        Assertions.assertThrows(IllegalArgumentException.class, () -> index.search("a b c", 0));
        Assertions.assertThrows(IllegalArgumentException.class, () -> index.search("a b c", -1));
    }

    private static String collidingExtension(String query, boolean belowThreshold) {
        final long[] querySignature = MinHashIndex.signature(MinHashIndex.shingles(query));
        for (int index = 0; index < 1000; index++) {
            String candidate = query + " extra" + index + " tail" + index
                + (belowThreshold ? " final" + index : "");
            long[] signature = MinHashIndex.signature(MinHashIndex.shingles(candidate));
            if (sharesBand(querySignature, signature)) {
                return candidate;
            }
        }
        throw new AssertionError("Fixed-seed fixture must include an LSH collision");
    }

    private static boolean sharesBand(long[] left, long[] right) {
        for (int band = 0; band < MinHashIndex.BANDS; band++) {
            boolean same = true;
            for (int row = 0; row < MinHashIndex.ROWS_PER_BAND; row++) {
                int offset = band * MinHashIndex.ROWS_PER_BAND + row;
                same &= left[offset] == right[offset];
            }
            if (same) {
                return true;
            }
        }
        return false;
    }

    private static List<String> ids(List<Bm25Index.Hit> hits) {
        return hits.stream().map(Bm25Index.Hit::getId).collect(Collectors.toList());
    }
}
