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

package org.apache.geaflow.dsl.schema.function;

import org.apache.geaflow.dsl.common.data.impl.ObjectRow;
import org.apache.geaflow.dsl.common.data.impl.types.ObjectEdge;
import org.apache.geaflow.dsl.common.data.impl.types.ObjectVertex;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Unit tests for the ISO-GQL SAME predicate function.
 */
public class SameTest {

    @Test
    public void testSameWithIdenticalVertices() {
        // Create two vertices with the same ID
        ObjectVertex v1 = new ObjectVertex(1, null, ObjectRow.create("Alice", 25));
        ObjectVertex v2 = new ObjectVertex(1, null, ObjectRow.create("Bob", 30));

        Boolean result = GeaFlowBuiltinFunctions.same(v1, v2);
        Assert.assertTrue(result, "Vertices with same ID should return true");
    }

    @Test
    public void testSameWithDifferentVertices() {
        // Create two vertices with different IDs
        ObjectVertex v1 = new ObjectVertex(1, null, ObjectRow.create("Alice", 25));
        ObjectVertex v2 = new ObjectVertex(2, null, ObjectRow.create("Bob", 30));

        Boolean result = GeaFlowBuiltinFunctions.same(v1, v2);
        Assert.assertFalse(result, "Vertices with different IDs should return false");
    }

    @Test
    public void testSameWithIdenticalEdges() {
        // Create two edges with the same source and target IDs
        ObjectEdge e1 = new ObjectEdge(1, 2, ObjectRow.create("knows"));
        ObjectEdge e2 = new ObjectEdge(1, 2, ObjectRow.create("likes"));

        Boolean result = GeaFlowBuiltinFunctions.same(e1, e2);
        Assert.assertTrue(result, "Edges with same source and target IDs should return true");
    }

    @Test
    public void testSameWithDifferentEdgesSameSource() {
        // Create two edges with the same source but different target IDs
        ObjectEdge e1 = new ObjectEdge(1, 2, ObjectRow.create("knows"));
        ObjectEdge e2 = new ObjectEdge(1, 3, ObjectRow.create("knows"));

        Boolean result = GeaFlowBuiltinFunctions.same(e1, e2);
        Assert.assertFalse(result, "Edges with different target IDs should return false");
    }

    @Test
    public void testSameWithDifferentEdgesSameTarget() {
        // Create two edges with different source but same target IDs
        ObjectEdge e1 = new ObjectEdge(1, 2, ObjectRow.create("knows"));
        ObjectEdge e2 = new ObjectEdge(3, 2, ObjectRow.create("knows"));

        Boolean result = GeaFlowBuiltinFunctions.same(e1, e2);
        Assert.assertFalse(result, "Edges with different source IDs should return false");
    }

    @Test
    public void testSameWithDifferentEdges() {
        // Create two edges with completely different IDs
        ObjectEdge e1 = new ObjectEdge(1, 2, ObjectRow.create("knows"));
        ObjectEdge e2 = new ObjectEdge(3, 4, ObjectRow.create("knows"));

        Boolean result = GeaFlowBuiltinFunctions.same(e1, e2);
        Assert.assertFalse(result, "Edges with different IDs should return false");
    }

    @Test
    public void testSameWithMixedTypes() {
        // Test vertex and edge - should return false
        ObjectVertex v = new ObjectVertex(1, null, ObjectRow.create("Alice", 25));
        ObjectEdge e = new ObjectEdge(1, 2, ObjectRow.create("knows"));

        Boolean result = GeaFlowBuiltinFunctions.same(v, e);
        Assert.assertFalse(result, "Vertex and edge should return false");
    }

    @Test
    public void testSameWithNullFirst() {
        // Test with first argument null
        ObjectVertex v = new ObjectVertex(1, null, ObjectRow.create("Alice", 25));

        Boolean result = GeaFlowBuiltinFunctions.same(null, v);
        Assert.assertNull(result, "Null first argument should return null");
    }

    @Test
    public void testSameWithNullSecond() {
        // Test with second argument null
        ObjectVertex v = new ObjectVertex(1, null, ObjectRow.create("Alice", 25));

        Boolean result = GeaFlowBuiltinFunctions.same(v, null);
        Assert.assertNull(result, "Null second argument should return null");
    }

    @Test
    public void testSameWithBothNull() {
        // Test with both arguments null - use explicit cast to Object to resolve ambiguity
        Boolean result = GeaFlowBuiltinFunctions.same((Object) null, (Object) null);
        Assert.assertNull(result, "Both null arguments should return null");
    }

    @Test
    public void testSameWithStringIds() {
        // Test with string IDs instead of integer IDs
        ObjectVertex v1 = new ObjectVertex("user123", null, ObjectRow.create("Alice", 25));
        ObjectVertex v2 = new ObjectVertex("user123", null, ObjectRow.create("Bob", 30));

        Boolean result = GeaFlowBuiltinFunctions.same(v1, v2);
        Assert.assertTrue(result, "Vertices with same string ID should return true");
    }

    @Test
    public void testSameWithDifferentStringIds() {
        // Test with different string IDs
        ObjectVertex v1 = new ObjectVertex("user123", null, ObjectRow.create("Alice", 25));
        ObjectVertex v2 = new ObjectVertex("user456", null, ObjectRow.create("Bob", 30));

        Boolean result = GeaFlowBuiltinFunctions.same(v1, v2);
        Assert.assertFalse(result, "Vertices with different string IDs should return false");
    }

    @Test
    public void testSameWithInvalidTypes() {
        // Test with objects that are not RowVertex or RowEdge
        String s1 = "test";
        String s2 = "test";

        Boolean result = GeaFlowBuiltinFunctions.same(s1, s2);
        Assert.assertFalse(result, "Non-graph elements should return false");
    }

    // Tests for type-specific overloads (RowVertex, RowEdge)

    @Test
    public void testSameVertexOverloadWithIdenticalIds() {
        // Test the type-specific RowVertex overload
        ObjectVertex v1 = new ObjectVertex(100, null, ObjectRow.create("Alice", 25));
        ObjectVertex v2 = new ObjectVertex(100, null, ObjectRow.create("Bob", 30));

        // Explicitly call with RowVertex types
        Boolean result = GeaFlowBuiltinFunctions.same((org.apache.geaflow.dsl.common.data.RowVertex) v1,
            (org.apache.geaflow.dsl.common.data.RowVertex) v2);
        Assert.assertTrue(result, "Type-specific vertex overload should work");
    }

    @Test
    public void testSameEdgeOverloadWithIdenticalIds() {
        // Test the type-specific RowEdge overload
        ObjectEdge e1 = new ObjectEdge(10, 20, ObjectRow.create("knows"));
        ObjectEdge e2 = new ObjectEdge(10, 20, ObjectRow.create("likes"));

        // Explicitly call with RowEdge types
        Boolean result = GeaFlowBuiltinFunctions.same((org.apache.geaflow.dsl.common.data.RowEdge) e1,
            (org.apache.geaflow.dsl.common.data.RowEdge) e2);
        Assert.assertTrue(result, "Type-specific edge overload should work");
    }

    // Tests for multi-argument same() varargs method

    @Test
    public void testSameWithThreeIdenticalVertices() {
        // Test varargs with 3 identical vertices
        ObjectVertex v1 = new ObjectVertex(1, null, ObjectRow.create("Alice", 25));
        ObjectVertex v2 = new ObjectVertex(1, null, ObjectRow.create("Bob", 30));
        ObjectVertex v3 = new ObjectVertex(1, null, ObjectRow.create("Charlie", 35));

        Boolean result = GeaFlowBuiltinFunctions.same(v1, v2, v3);
        Assert.assertTrue(result, "Three vertices with same ID should return true");
    }

    @Test
    public void testSameWithThreeVerticesOneDifferent() {
        // Test varargs with one different vertex
        ObjectVertex v1 = new ObjectVertex(1, null, ObjectRow.create("Alice", 25));
        ObjectVertex v2 = new ObjectVertex(1, null, ObjectRow.create("Bob", 30));
        ObjectVertex v3 = new ObjectVertex(2, null, ObjectRow.create("Charlie", 35));

        Boolean result = GeaFlowBuiltinFunctions.same(v1, v2, v3);
        Assert.assertFalse(result, "Three vertices with one different ID should return false");
    }

    @Test
    public void testSameWithFourIdenticalEdges() {
        // Test varargs with 4 identical edges
        ObjectEdge e1 = new ObjectEdge(1, 2, ObjectRow.create("knows"));
        ObjectEdge e2 = new ObjectEdge(1, 2, ObjectRow.create("likes"));
        ObjectEdge e3 = new ObjectEdge(1, 2, ObjectRow.create("follows"));
        ObjectEdge e4 = new ObjectEdge(1, 2, ObjectRow.create("trusts"));

        Boolean result = GeaFlowBuiltinFunctions.same(e1, e2, e3, e4);
        Assert.assertTrue(result, "Four edges with same source and target IDs should return true");
    }

    @Test
    public void testSameWithMultipleNullInMiddle() {
        // Test varargs with null in the middle
        ObjectVertex v1 = new ObjectVertex(1, null, ObjectRow.create("Alice", 25));
        ObjectVertex v3 = new ObjectVertex(1, null, ObjectRow.create("Charlie", 35));

        Boolean result = GeaFlowBuiltinFunctions.same(v1, null, v3);
        Assert.assertNull(result, "Varargs with null element should return null");
    }

    @Test
    public void testSameWithEmptyVarargs() {
        // Test varargs with no arguments (should return null)
        Boolean result = GeaFlowBuiltinFunctions.same(new Object[0]);
        Assert.assertNull(result, "Empty varargs should return null");
    }

    @Test
    public void testSameWithSingleVararg() {
        // Test varargs with single argument (should return null - need at least 2)
        ObjectVertex v1 = new ObjectVertex(1, null, ObjectRow.create("Alice", 25));

        Boolean result = GeaFlowBuiltinFunctions.same(new Object[]{v1});
        Assert.assertNull(result, "Single vararg should return null");
    }

    @Test
    public void testSameWithMixedTypesInVarargs() {
        // Test varargs with mixed vertex and edge types
        ObjectVertex v = new ObjectVertex(1, null, ObjectRow.create("Alice", 25));
        ObjectEdge e = new ObjectEdge(1, 2, ObjectRow.create("knows"));

        Boolean result = GeaFlowBuiltinFunctions.same(v, e);
        Assert.assertFalse(result, "Mixed vertex and edge in varargs should return false");
    }
}
