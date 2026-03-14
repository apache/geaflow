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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import org.junit.jupiter.api.Test;

public class MagnitudeVectorTest {

    @Test
    public void testConstructorAndGetter() {
        MagnitudeVector vector = new MagnitudeVector(0.85);
        assertEquals(vector.getMagnitude(), 0.85, 0.0001);
    }

    @Test
    public void testMatchExactSameValue() {
        MagnitudeVector v1 = new MagnitudeVector(5.0);
        MagnitudeVector v2 = new MagnitudeVector(5.0);

        assertEquals(v1.match(v2), 1.0, 0.0001);
    }

    @Test
    public void testMatchBothZero() {
        MagnitudeVector v1 = new MagnitudeVector(0.0);
        MagnitudeVector v2 = new MagnitudeVector(0.0);

        assertEquals(v1.match(v2), 1.0, 0.0001);
    }

    @Test
    public void testMatchOneZero() {
        MagnitudeVector v1 = new MagnitudeVector(0.0);
        MagnitudeVector v2 = new MagnitudeVector(5.0);

        assertEquals(v1.match(v2), 0.0, 0.0001);
        assertEquals(v2.match(v1), 0.0, 0.0001);
    }

    @Test
    public void testMatchDifferentValues() {
        MagnitudeVector v1 = new MagnitudeVector(10.0);
        MagnitudeVector v2 = new MagnitudeVector(5.0);

        // Expected: 1 - |10-5|/max(10,5) = 1 - 5/10 = 0.5
        assertEquals(v1.match(v2), 0.5, 0.0001);
    }

    @Test
    public void testMatchPageRankValues() {
        // Simulating PageRank centrality matching
        MagnitudeVector highRank = new MagnitudeVector(0.85);
        MagnitudeVector mediumRank = new MagnitudeVector(0.45);

        double similarity = highRank.match(mediumRank);
        // Expected: 1 - |0.85-0.45|/0.85 = 1 - 0.4/0.85 ≈ 0.529
        assertEquals(similarity, 0.529, 0.01);
    }

    @Test
    public void testMatchWithNegativeValues() {
        MagnitudeVector v1 = new MagnitudeVector(-5.0);
        MagnitudeVector v2 = new MagnitudeVector(-3.0);

        // Expected: 1 - |-5-(-3)|/max(|-5|,|-3|) = 1 - 2/5 = 0.6
        assertEquals(v1.match(v2), 0.6, 0.0001);
    }

    @Test
    public void testMatchWithIncompatibleType() {
        MagnitudeVector v1 = new MagnitudeVector(5.0);
        IVector incompatibleVector = new IVector() {
            @Override
            public double match(IVector other) {
                return 0;
            }

            @Override
            public VectorType getType() {
                return null;
            }
        };

        assertEquals(v1.match(incompatibleVector), 0.0, 0.0001);
    }

    @Test
    public void testEqualsAndHashCode() {
        MagnitudeVector v1 = new MagnitudeVector(5.0);
        MagnitudeVector v2 = new MagnitudeVector(5.0);
        MagnitudeVector v3 = new MagnitudeVector(10.0);

        assertEquals(v1, v2);
        assertEquals(v1.hashCode(), v2.hashCode());
        assertNotEquals(v1, v3);
    }

    @Test
    public void testToString() {
        MagnitudeVector vector = new MagnitudeVector(0.75);
        String str = vector.toString();

        assertEquals(str, "MagnitudeVector{magnitude=0.75}");
    }

    @Test
    public void testGetType() {
        MagnitudeVector vector = new MagnitudeVector(1.0);
        assertEquals(vector.getType(), VectorType.MagnitudeVector);
    }
}
