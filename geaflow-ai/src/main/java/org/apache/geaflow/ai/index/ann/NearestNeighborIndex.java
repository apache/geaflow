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

import java.util.List;
import org.apache.geaflow.ai.index.vector.IVector;

/**
 * Nearest-neighbor search over {@link IVector} entries, keyed by an opaque id.
 *
 * <p>Phase 1 ships this contract with the exact {@link LinearNearestNeighborIndex} only — vector
 * search today is a linear TopN over the candidate set, and this interface exists so an ANN
 * implementation (HNSW and friends) can slot in behind the same calls later without touching the
 * retrieval path. No performance claim is made or implied by implementing this interface.
 *
 * <p><b>Contract.</b> Implementations are not required to be thread-safe. {@link #add} appends an
 * entry; adding the same id twice keeps both entries (append-only semantics, like most ANN
 * structures). {@link #search}:
 *
 * <ul>
 *   <li>returns at most {@code topK} neighbors, ordered by descending {@link Neighbor#getScore};</li>
 *   <li>breaks score ties by <b>insertion order</b> — of equally-scored entries, the one added
 *       first ranks first — so two runs over the same data produce the same ranking;</li>
 *   <li>must throw {@link IllegalArgumentException} for {@code topK <= 0};</li>
 *   <li>returns an empty list when the index holds no entries.</li>
 * </ul>
 *
 * <p>Score direction is fixed as {@code entryVector.match(query)} — never the other way around —
 * because some {@link IVector} implementations are not symmetric in their arguments.
 *
 * <p>An exact adapter must score every entry with {@link IVector#match} and honor the full
 * ordering above. An approximate adapter may miss neighbors and coarsen scores, but must still
 * respect the result shape: at most {@code topK} entries, no id appearing twice unless it was
 * added twice, and a non-increasing score order within the returned page.
 *
 * @param <T> id type of the indexed entries
 */
public interface NearestNeighborIndex<T> {

    /**
     * Appends one entry to the index.
     *
     * @param id     caller-defined id returned in search results
     * @param vector representation the entry is searched by
     */
    void add(T id, IVector vector);

    /**
     * Returns up to {@code topK} nearest entries for the query, ordered by the contract above.
     *
     * @param query vector to search against
     * @param topK  maximum number of results, must be positive
     * @return ranked neighbors, highest score first
     */
    List<Neighbor<T>> search(IVector query, int topK);

    /** Number of entries currently held (an id added twice counts twice). */
    int size();
}
