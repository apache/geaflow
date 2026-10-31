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

import java.util.ArrayList;
import java.util.List;
import org.apache.geaflow.ai.index.vector.IVector;

/**
 * Exact linear scan over the added entries — the default {@link NearestNeighborIndex} adapter for
 * Phase 1 and the reference implementation an ANN adapter is measured against.
 *
 * <p>Every search scores every entry through {@code entryVector.match(query)} and sorts by the
 * interface contract: score descending, ties broken by insertion order. The sort is stable over
 * the insertion sequence, which is what makes equal-scored neighbors come out in the order they
 * were added. Results are a fresh list; mutating it does not affect the index.
 *
 * <p>This adapter makes no performance claim: it is the correctness baseline. Swap-in of an ANN
 * implementation is a change behind the same interface, not an optimization of this class.
 *
 * @param <T> id type of the indexed entries
 */
public class LinearNearestNeighborIndex<T> implements NearestNeighborIndex<T> {

    private final List<Entry<T>> entries = new ArrayList<>();

    @Override
    public void add(T id, IVector vector) {
        if (id == null || vector == null) {
            throw new IllegalArgumentException("id and vector must not be null");
        }
        entries.add(new Entry<>(id, vector));
    }

    @Override
    public List<Neighbor<T>> search(IVector query, int topK) {
        if (query == null) {
            throw new IllegalArgumentException("query must not be null");
        }
        if (topK <= 0) {
            throw new IllegalArgumentException("topK must be positive, got " + topK);
        }
        double[] scores = new double[entries.size()];
        Integer[] order = new Integer[entries.size()];
        for (int i = 0; i < entries.size(); i++) {
            scores[i] = entries.get(i).vector.match(query);
            order[i] = i;
        }
        // Tie-break key: the insertion ordinal travels alongside the score so equal-scored
        // neighbors keep the order they were added in, independent of sort stability.
        java.util.Arrays.sort(order, (a, b) -> {
            int byScore = Double.compare(scores[b], scores[a]);
            return byScore != 0 ? byScore : Integer.compare(a, b);
        });
        List<Neighbor<T>> result = new ArrayList<>(Math.min(topK, order.length));
        for (int i = 0; i < Math.min(topK, order.length); i++) {
            Entry<T> entry = entries.get(order[i]);
            result.add(new Neighbor<>(entry.id, scores[order[i]]));
        }
        return result;
    }

    @Override
    public int size() {
        return entries.size();
    }

    private static final class Entry<T> {
        private final T id;
        private final IVector vector;

        private Entry(T id, IVector vector) {
            this.id = id;
            this.vector = vector;
        }
    }
}
