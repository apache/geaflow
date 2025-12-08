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

package org.apache.geaflow.dsl.udf.table.other;

import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.RowEdge;
import org.apache.geaflow.dsl.common.data.RowVertex;
import org.apache.geaflow.dsl.common.data.impl.ObjectRow;
import org.apache.geaflow.dsl.common.data.impl.types.LongVertex;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for PropertyExists ISO-GQL predicate function.
 *
 * <p>Tests validate:
 * <ul>
 *   <li>Three-valued logic (NULL handling)</li>
 *   <li>Type validation and error handling</li>
 *   <li>Property name validation</li>
 *   <li>ISO-GQL compliance</li>
 * </ul>
 */
public class PropertyExistsTest {

    @Test
    public void testNullElement() {
        PropertyExists func = new PropertyExists();

        // ISO-GQL Rule: NULL element → Unknown (null)
        // Test with null Object
        Boolean result = func.eval((Object) null, "anyProperty");
        Assert.assertNull("NULL element should return NULL (Unknown in three-valued logic)", result);

        // Test with null RowVertex
        result = func.eval((RowVertex) null, "anyProperty");
        Assert.assertNull("NULL vertex should return NULL", result);

        // Test with null RowEdge
        result = func.eval((RowEdge) null, "anyProperty");
        Assert.assertNull("NULL edge should return NULL", result);

        // Test with null Row
        result = func.eval((Row) null, "anyProperty");
        Assert.assertNull("NULL row should return NULL", result);
    }

    @Test
    public void testNonNullVertex() {
        PropertyExists func = new PropertyExists();

        // Create a simple vertex (non-null)
        RowVertex vertex = new LongVertex(1L);

        // In GeaFlow's implementation, property existence is validated at compile-time
        // At runtime, non-null elements with valid property names return true
        Boolean result = func.eval(vertex, "name");
        Assert.assertNotNull("Non-null vertex should not return NULL", result);
        Assert.assertTrue("Non-null vertex with valid property name should return TRUE", result);
    }

    @Test
    public void testNonNullRow() {
        PropertyExists func = new PropertyExists();

        // Create a simple row (non-null)
        Row row = ObjectRow.create(new Object[]{"value1", "value2"});

        // Property existence validated at compile-time
        Boolean result = func.eval(row, "field1");
        Assert.assertNotNull("Non-null row should not return NULL", result);
        Assert.assertTrue("Non-null row with valid property name should return TRUE", result);
    }

    @Test
    public void testThreeValuedLogic() {
        PropertyExists func = new PropertyExists();

        // Test NULL case (Unknown in three-valued logic)
        Boolean resultNull = func.eval((Object) null, "property");
        Assert.assertNull("Three-valued logic: NULL element → Unknown (null)", resultNull);

        // Test TRUE case (property exists - simplified as non-null element)
        RowVertex vertex = new LongVertex(1L);
        Boolean resultTrue = func.eval(vertex, "property");
        Assert.assertTrue("Three-valued logic: Non-null element → TRUE", resultTrue);
    }

    @Test
    public void testDescription() {
        PropertyExists func = new PropertyExists();

        // Verify the function has proper description annotation
        Assert.assertNotNull("PropertyExists class should exist", func);

        // The @Description annotation should be present (checked by reflection if needed)
        Assert.assertTrue("PropertyExists should be a UDF",
            func.getClass().getSuperclass().getSimpleName().equals("UDF"));
    }

    // ==================== Error Handling Tests ====================

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidElementType() {
        PropertyExists func = new PropertyExists();

        // Test with invalid element type (String instead of graph element)
        func.eval("not a graph element", "propertyName");
        // Should throw IllegalArgumentException
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidElementTypeInteger() {
        PropertyExists func = new PropertyExists();

        // Test with invalid element type (Integer)
        func.eval(123, "propertyName");
        // Should throw IllegalArgumentException
    }

    @Test
    public void testInvalidElementTypeMessage() {
        PropertyExists func = new PropertyExists();

        try {
            func.eval("invalid", "propertyName");
            Assert.fail("Should have thrown IllegalArgumentException for invalid element type");
        } catch (IllegalArgumentException e) {
            // Verify error message contains useful information
            Assert.assertTrue("Error message should mention graph element requirement",
                e.getMessage().contains("graph element"));
            Assert.assertTrue("Error message should include actual type",
                e.getMessage().contains("String"));
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullPropertyName() {
        PropertyExists func = new PropertyExists();

        // Test with null property name
        RowVertex vertex = new LongVertex(1L);
        func.eval(vertex, null);
        // Should throw IllegalArgumentException
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEmptyPropertyName() {
        PropertyExists func = new PropertyExists();

        // Test with empty property name
        RowVertex vertex = new LongVertex(1L);
        func.eval(vertex, "");
        // Should throw IllegalArgumentException
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWhitespacePropertyName() {
        PropertyExists func = new PropertyExists();

        // Test with whitespace-only property name
        RowVertex vertex = new LongVertex(1L);
        func.eval(vertex, "   ");
        // Should throw IllegalArgumentException
    }

    @Test
    public void testInvalidPropertyNameMessage() {
        PropertyExists func = new PropertyExists();
        RowVertex vertex = new LongVertex(1L);

        try {
            func.eval(vertex, null);
            Assert.fail("Should have thrown IllegalArgumentException for null property name");
        } catch (IllegalArgumentException e) {
            // Verify error message is clear
            Assert.assertTrue("Error message should mention property name requirement",
                e.getMessage().contains("property name"));
        }
    }

    @Test
    public void testTypeSpecificOverloads() {
        PropertyExists func = new PropertyExists();

        // Test that type-specific overloads work correctly
        RowVertex vertex = new LongVertex(1L);
        Row row = ObjectRow.create(new Object[]{"value"});

        // These should all work without ClassCastException
        Boolean vertexResult = func.eval(vertex, "name");
        Boolean rowResult = func.eval(row, "field");

        Assert.assertNotNull("Vertex overload should work", vertexResult);
        Assert.assertNotNull("Row overload should work", rowResult);
        Assert.assertTrue("Type-specific overloads should return TRUE", vertexResult && rowResult);
    }
}
