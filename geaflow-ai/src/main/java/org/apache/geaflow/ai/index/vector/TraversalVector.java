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

package org.apache.geaflow.ai.index.vector;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class TraversalVector implements IVector {

    private final String[] vec;

    public TraversalVector(String... vec) {
        if (vec.length % 3 != 0) {
            throw new RuntimeException("Traversal vector should be src-edge-dst triple");
        }
        this.vec = vec;
    }

    @Override
    public double match(IVector other) {
        if (!(other instanceof TraversalVector)) {
            return 0.0;
        }

        TraversalVector otherVec = (TraversalVector) other;

        // Exact match: identical triple sequence
        if (Arrays.equals(this.vec, otherVec.vec)) {
            return 1.0;
        }

        // Convert triples to set for efficient comparison
        Set<String> thisTriples = getTriplesSet();
        Set<String> otherTriples = otherVec.getTriplesSet();

        // Check for subgraph containment (this is contained in other)
        if (otherTriples.containsAll(thisTriples)) {
            return 0.8;
        }

        // Compute partial overlap using Jaccard similarity
        Set<String> intersection = new HashSet<>(thisTriples);
        intersection.retainAll(otherTriples);

        if (intersection.isEmpty()) {
            return 0.0;
        }

        Set<String> union = new HashSet<>(thisTriples);
        union.addAll(otherTriples);

        return (double) intersection.size() / union.size();
    }

    /**
     * Converts the array of triples into a Set of string representations.
     * Each triple is represented as "src|edge|dst".
     */
    private Set<String> getTriplesSet() {
        Set<String> triples = new HashSet<>();
        for (int i = 0; i < vec.length; i += 3) {
            String triple = vec[i] + "|" + vec[i + 1] + "|" + vec[i + 2];
            triples.add(triple);
        }
        return triples;
    }

    @Override
    public VectorType getType() {
        return VectorType.TraversalVector;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TraversalVector that = (TraversalVector) o;
        return Arrays.equals(vec, that.vec);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(vec));
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("TraversalVector{vec=");
        for (int i = 0; i < vec.length; i++) {
            if (i > 0) {
                sb.append(i % 3 == 0 ? "; " : "-");
            }
            sb.append(vec[i]);
        }
        return sb.append('}').toString();
    }
}
