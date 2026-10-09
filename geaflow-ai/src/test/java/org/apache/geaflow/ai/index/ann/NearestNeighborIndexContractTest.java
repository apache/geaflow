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

package org.apache.geaflow.ai.index.ann;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.geaflow.ai.index.vector.EmbeddingVector;
import org.apache.geaflow.ai.index.vector.IVector;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Contract every {@link NearestNeighborIndex} must hold, written against a fresh index supplied
 * by {@link #newIndex()} so any future adapter runs the identical suite — that is the issue #854
 * acceptance criterion "the linear adapter passes the same contract that an ANN adapter will use".
 *
 * <p>The tests in {@link #resultShape()} hold for any adapter, exact or approximate. The tests in
 * {@link #rankingIsExact()} assert the exact ordering — score from {@link IVector#match},
 * descending, ties on insertion order — and are required only of adapters that claim exactness.
 * An ANN implementation should extend this class, keep the shape tests, and override
 * {@link #rankingIsExact()} (or exclude it) while its recall is approximate; the distinction is
 * deliberate and documented in the interface.
 */
public abstract class NearestNeighborIndexContractTest {

    /** Fresh, empty index of the implementation under test. */
    protected abstract NearestNeighborIndex<String> newIndex();

    private static EmbeddingVector vec(double... components) {
        return new EmbeddingVector(components);
    }

    /**
     * Shape rules that must hold for every adapter: topK bound, result-page score monotonicity,
     * empty-index behavior, argument validation, append-only size accounting.
     */
    @Test
    public void resultShape() {
        NearestNeighborIndex<String> index = newIndex();
        Assertions.assertEquals(0, index.size());
        Assertions.assertTrue(index.search(vec(1, 0), 5).isEmpty(),
            "an empty index returns no neighbors");

        index.add("a", vec(1, 0));
        index.add("b", vec(0, 1));
        index.add("a", vec(1, 1));
        Assertions.assertEquals(3, index.size(),
            "an id added twice is two entries, per the append-only contract");

        List<Neighbor<String>> page = index.search(vec(1, 0), 2);
        Assertions.assertEquals(2, page.size(), "at most topK neighbors are returned");
        Assertions.assertTrue(page.get(0).getScore() >= page.get(1).getScore(),
            "the returned page is ordered by non-increasing score");

        Assertions.assertEquals(3, index.search(vec(1, 0), 10).size(),
            "topK beyond the index size returns everything");
        Assertions.assertThrows(IllegalArgumentException.class,
            () -> index.search(vec(1, 0), 0), "topK must be positive");
        Assertions.assertThrows(IllegalArgumentException.class,
            () -> index.search(vec(1, 0), -1), "topK must be positive");
    }

    /**
     * The exactness rules: scores equal {@code entryVector.match(query)} verbatim, the global
     * order is score descending with ties on insertion order, and repeated searches over
     * unchanged data are bit-for-bit identical.
     */
    @Test
    public void rankingIsExact() {
        NearestNeighborIndex<String> index = newIndex();
        // Cosine similarity against (1, 0): exactly 1, exactly 0, and 1/sqrt(2) in that order —
        // three distinct scores with hand-checkable values.
        index.add("east", vec(1, 0));
        index.add("north", vec(0, 1));
        index.add("diagonal", vec(1, 1));
        // Zero vector: cosine is undefined and EmbeddingVector.match returns 0.0 for it.
        index.add("origin", vec(0, 0));

        List<Neighbor<String>> ranked = index.search(vec(1, 0), 4);
        Assertions.assertEquals(4, ranked.size());
        Assertions.assertAll(
            () -> Assertions.assertEquals("east", ranked.get(0).getId()),
            () -> Assertions.assertEquals(1.0, ranked.get(0).getScore(), 1e-12),
            () -> Assertions.assertEquals("diagonal", ranked.get(1).getId()),
            () -> Assertions.assertEquals(1.0 / Math.sqrt(2), ranked.get(1).getScore(), 1e-12),
            () -> Assertions.assertEquals("north", ranked.get(2).getId()),
            () -> Assertions.assertEquals(0.0, ranked.get(2).getScore(), 1e-12),
            () -> Assertions.assertEquals("origin", ranked.get(3).getId()),
            () -> Assertions.assertEquals(0.0, ranked.get(3).getScore(), 1e-12));

        List<Neighbor<String>> again = index.search(vec(1, 0), 4);
        Assertions.assertEquals(ranked, again,
            "search is deterministic: identical query and data give identical results");
    }

    /**
     * The tie-break rule the issue asks to be documented and pinned: equal scores come out in
     * insertion order. "third" and "twin" are collinear with the query (identical cosine of 1),
     * and the three orthogonal axes all score exactly 0 — each group must rank in insertion
     * order, and no member of the zero group may jump ahead of the collinear group.
     */
    @Test
    public void tiesBreakOnInsertionOrder() {
        NearestNeighborIndex<String> index = newIndex();
        index.add("second", vec(0, 1, 0, 0));
        index.add("fourth", vec(0, 0, 0, 1));
        index.add("first", vec(1, 0, 0, 0));
        index.add("third", vec(0, 0, 1, 0));
        index.add("twin", vec(0, 0, 2, 0));

        List<Neighbor<String>> ranked = index.search(vec(0, 0, 1, 0), 5);
        Assertions.assertEquals(
            List.of("third", "twin", "second", "fourth", "first"),
            ids(ranked),
            "collinear entries first (insertion order among themselves), "
                + "then the zero-scored ones, again in insertion order");
        Assertions.assertEquals(ranked.get(0).getScore(), ranked.get(1).getScore(), 0.0,
            "collinear vectors produce exactly equal scores");
    }

    /**
     * The page window slides by score: raising topK never reorders what a smaller topK returned —
     * the first k entries of the topK=n page equal the topK=k page.
     */
    @Test
    public void largerTopKExtendsWithoutReordering() {
        NearestNeighborIndex<String> index = newIndex();
        for (int i = 0; i < 8; i++) {
            index.add("v" + i, vec(i, 8 - i));
        }
        List<Neighbor<String>> top3 = index.search(vec(1, 1), 3);
        List<Neighbor<String>> top8 = index.search(vec(1, 1), 8);
        Assertions.assertEquals(ids(top3), ids(top8).subList(0, 3),
            "a larger page must extend, not reorder");
    }

    /** No id may appear more often in a page than it was added — append-only accounting. */
    @Test
    public void pagesDoNotDuplicateEntries() {
        NearestNeighborIndex<String> index = newIndex();
        index.add("dup", vec(1, 0));
        index.add("dup", vec(1, 0));
        index.add("other", vec(1, 0));
        List<Neighbor<String>> page = index.search(vec(1, 0), 3);
        Set<String> uniqueIds = new HashSet<>(ids(page));
        Assertions.assertEquals(2, uniqueIds.size(), "ids collapse to distinct values");
        Assertions.assertEquals(3, page.size(), "both dup entries and other are returned");
    }

    private static List<String> ids(List<Neighbor<String>> page) {
        List<String> ids = new java.util.ArrayList<>(page.size());
        for (Neighbor<String> neighbor : page) {
            ids.add(neighbor.getId());
        }
        return ids;
    }
}
