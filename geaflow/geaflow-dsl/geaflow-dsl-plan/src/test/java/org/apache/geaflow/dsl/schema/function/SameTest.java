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
        // Test with both arguments null
        Boolean result = GeaFlowBuiltinFunctions.same(null, null);
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
}
