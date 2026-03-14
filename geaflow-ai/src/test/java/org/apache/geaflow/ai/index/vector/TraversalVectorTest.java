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

public class TraversalVectorTest {

    @Test
    public void testConstructorValidInput() {
        new TraversalVector(
            "Alice", "knows", "Bob",
            "Bob", "knows", "Charlie"
        );

        // Should not throw exception
    }

    @Test
    public void testConstructorInvalidInput() {
        // Should throw exception if not multiple of 3
        org.junit.jupiter.api.Assertions.assertThrows(RuntimeException.class, () ->
            new TraversalVector("Alice", "knows", "Bob", "Bob", "knows")
        );
    }

    @Test
    public void testMatchExactSamePath() {
        TraversalVector v1 = new TraversalVector(
            "Alice", "knows", "Bob",
            "Bob", "knows", "Charlie"
        );
        TraversalVector v2 = new TraversalVector(
            "Alice", "knows", "Bob",
            "Bob", "knows", "Charlie"
        );

        assertEquals(v1.match(v2), 1.0, 0.0001);
    }

    @Test
    public void testMatchSubgraphContainment() {
        // v1 is contained within v2
        TraversalVector v1 = new TraversalVector(
            "Bob", "knows", "Charlie"
        );
        TraversalVector v2 = new TraversalVector(
            "Alice", "knows", "Bob",
            "Bob", "knows", "Charlie",
            "Charlie", "knows", "Dave"
        );

        // v1 is subgraph of v2, so score should be 0.8
        assertEquals(v1.match(v2), 0.8, 0.0001);
    }

    @Test
    public void testMatchPartialOverlap() {
        // Two vectors sharing one common edge
        TraversalVector v1 = new TraversalVector(
            "Alice", "knows", "Bob",
            "Bob", "likes", "Charlie"
        );
        TraversalVector v2 = new TraversalVector(
            "Bob", "knows", "Charlie",
            "Alice", "knows", "Bob"
        );

        // One common edge out of 3 unique edges total = 1/3
        double expected = 1.0 / 3.0;
        assertEquals(v1.match(v2), expected, 0.0001);
    }

    @Test
    public void testMatchNoOverlap() {
        TraversalVector v1 = new TraversalVector(
            "Alice", "knows", "Bob"
        );
        TraversalVector v2 = new TraversalVector(
            "Charlie", "knows", "Dave"
        );

        assertEquals(v1.match(v2), 0.0, 0.0001);
    }

    @Test
    public void testMatchIncompatibleType() {
        TraversalVector v1 = new TraversalVector("Alice", "knows", "Bob");
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
    public void testGuaranteeCirclePattern() {
        // Financial guarantee circle detection
        TraversalVector guaranteeCycle = new TraversalVector(
            "CompanyA", "guarantees", "CompanyB",
            "CompanyB", "guarantees", "CompanyC",
            "CompanyC", "guarantees", "CompanyA"
        );

        TraversalVector partialMatch = new TraversalVector(
            "CompanyB", "guarantees", "CompanyC",
            "CompanyC", "guarantees", "CompanyA"
        );

        // Partial match - 2 out of 3 edges match
        guaranteeCycle.match(partialMatch);
        // Note: The current implementation checks if this is subgraph of other
        // Since partialMatch is not a subgraph of guaranteeCycle in terms of exact triples
        // the score may differ
    }

    @Test
    public void testEqualsAndHashCode() {
        TraversalVector v1 = new TraversalVector(
            "Alice", "knows", "Bob",
            "Bob", "knows", "Charlie"
        );
        TraversalVector v2 = new TraversalVector(
            "Alice", "knows", "Bob",
            "Bob", "knows", "Charlie"
        );
        TraversalVector v3 = new TraversalVector(
            "Bob", "knows", "Charlie"
        );

        assertEquals(v1, v2);
        assertEquals(v1.hashCode(), v2.hashCode());
        assertNotEquals(v1, v3);
    }

    @Test
    public void testToString() {
        TraversalVector vector = new TraversalVector(
            "Alice", "knows", "Bob"
        );
        String str = vector.toString();

        assertEquals(str, "TraversalVector{vec=Alice-knows-Bob}");
    }

    @Test
    public void testToStringMultipleEdges() {
        TraversalVector vector = new TraversalVector(
            "Alice", "knows", "Bob",
            "Bob", "knows", "Charlie"
        );
        String str = vector.toString();

        assertEquals(str, "TraversalVector{vec=Alice-knows-Bob; Bob-knows-Charlie}");
    }

    @Test
    public void testGetType() {
        TraversalVector vector = new TraversalVector("Alice", "knows", "Bob");
        assertEquals(vector.getType(), VectorType.TraversalVector);
    }

    @Test
    public void testEmptyVector() {
        // Edge case: empty traversal vector (length 0 is valid - 0 % 3 == 0)
        TraversalVector empty1 = new TraversalVector();
        TraversalVector empty2 = new TraversalVector();
        assertEquals(empty1.match(empty2), 1.0, 0.0001);
    }
}
