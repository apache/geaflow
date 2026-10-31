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

import org.apache.geaflow.ai.index.vector.EmbeddingVector;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * The linear adapter runs the full {@link NearestNeighborIndexContractTest} suite — including the
 * exactness tests — plus its own argument validation. This is the adapter a future ANN
 * implementation is measured against: same tests, same orderings, no approximations.
 */
public class LinearNearestNeighborIndexTest extends NearestNeighborIndexContractTest {

    @Override
    protected NearestNeighborIndex<String> newIndex() {
        return new LinearNearestNeighborIndex<>();
    }

    @Test
    public void rejectsNullArguments() {
        LinearNearestNeighborIndex<String> index = new LinearNearestNeighborIndex<>();
        Assertions.assertThrows(IllegalArgumentException.class,
            () -> index.add(null, new EmbeddingVector(new double[]{1, 0})));
        Assertions.assertThrows(IllegalArgumentException.class,
            () -> index.add("a", null));
        Assertions.assertThrows(IllegalArgumentException.class,
            () -> index.search(null, 1));
    }

    /** The returned page is a fresh list: caller-side mutation cannot corrupt the index. */
    @Test
    public void returnedPageIsIndependentOfTheIndex() {
        LinearNearestNeighborIndex<String> index = new LinearNearestNeighborIndex<>();
        index.add("a", new EmbeddingVector(new double[]{1, 0}));
        index.add("b", new EmbeddingVector(new double[]{0, 1}));
        index.search(new EmbeddingVector(new double[]{1, 0}), 2).clear();
        Assertions.assertEquals(2, index.search(new EmbeddingVector(new double[]{1, 0}), 2).size());
    }
}
