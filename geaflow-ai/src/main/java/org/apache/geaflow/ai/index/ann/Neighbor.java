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

import java.util.Objects;

/**
 * One search result of a {@link NearestNeighborIndex}: the caller-supplied id plus its similarity
 * score against the query, as produced by {@link org.apache.geaflow.ai.index.vector.IVector#match}.
 */
public final class Neighbor<T> {

    private final T id;
    private final double score;

    public Neighbor(T id, double score) {
        this.id = Objects.requireNonNull(id);
        this.score = score;
    }

    public T getId() {
        return id;
    }

    public double getScore() {
        return score;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Neighbor<?> other = (Neighbor<?>) o;
        return Double.compare(score, other.score) == 0 && id.equals(other.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, score);
    }

    @Override
    public String toString() {
        return "Neighbor{" + "id=" + id + ", score=" + score + '}';
    }
}
