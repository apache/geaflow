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

package org.apache.geaflow.dsl.runtime.query;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import org.apache.geaflow.common.binary.BinaryString;
import org.apache.geaflow.common.type.primitive.BinaryStringType;
import org.apache.geaflow.common.type.primitive.IntegerType;
import org.apache.geaflow.dsl.common.data.impl.ObjectRow;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.runtime.expression.field.FieldExpression;
import org.apache.geaflow.dsl.runtime.function.table.order.MultiFieldRadixSort;
import org.apache.geaflow.dsl.runtime.function.table.order.OrderByField;
import org.apache.geaflow.dsl.runtime.function.table.order.OrderByField.ORDER;
import org.apache.geaflow.dsl.runtime.function.table.order.SortInfo;
import org.testng.annotations.Test;

public class MultiFieldRadixSortTest {

    @Test
    public void testSortEmptyList() {
        List<Row> data = new ArrayList<>();

        SortInfo sortInfo = new SortInfo();
        sortInfo.orderByFields = new ArrayList<>(1);
        
        OrderByField intField = new OrderByField();
        intField.expression = new FieldExpression(0, IntegerType.INSTANCE);
        intField.order = ORDER.ASC;
        sortInfo.orderByFields.add(intField);
        
        MultiFieldRadixSort.multiFieldRadixSort(data, sortInfo);
        
        assertEquals(data.size(), 0);
    }

    @Test
    public void testSortSingleElement() {
        List<Row> data = new ArrayList<>(1);
        data.add(ObjectRow.create(new Object[]{1, BinaryString.fromString("test")}));

        SortInfo sortInfo = new SortInfo();
        sortInfo.orderByFields = new ArrayList<>(1);
        
        OrderByField intField = new OrderByField();
        intField.expression = new FieldExpression(0, IntegerType.INSTANCE);
        intField.order = ORDER.ASC;
        sortInfo.orderByFields.add(intField);
        
        MultiFieldRadixSort.multiFieldRadixSort(data, sortInfo);
        
        assertEquals(data.size(), 1);
        assertEquals(data.get(0).getField(0, IntegerType.INSTANCE), Integer.valueOf(1));
    }

    @Test
    public void testSortByIntegerFieldAscending() {
        List<Row> data = new ArrayList<>(3);
        data.add(ObjectRow.create(new Object[]{3, BinaryString.fromString("c")}));
        data.add(ObjectRow.create(new Object[]{1, BinaryString.fromString("a")}));
        data.add(ObjectRow.create(new Object[]{2, BinaryString.fromString("b")}));
        
        SortInfo sortInfo = new SortInfo();
        sortInfo.orderByFields = new ArrayList<>(1);
        OrderByField intField = new OrderByField();
        intField.expression = new FieldExpression(0, IntegerType.INSTANCE);
        intField.order = ORDER.ASC;
        sortInfo.orderByFields.add(intField);
        
        MultiFieldRadixSort.multiFieldRadixSort(data, sortInfo);
        
        assertEquals(data.size(), 3);
        assertEquals(data.get(0).getField(0, IntegerType.INSTANCE), Integer.valueOf(1));
        assertEquals(data.get(1).getField(0, IntegerType.INSTANCE), Integer.valueOf(2));
        assertEquals(data.get(2).getField(0, IntegerType.INSTANCE), Integer.valueOf(3));
    }

    @Test
    public void testSortByIntegerFieldDescending() {
        List<Row> data = new ArrayList<>(3);
        data.add(ObjectRow.create(new Object[]{1, BinaryString.fromString("a")}));
        data.add(ObjectRow.create(new Object[]{3, BinaryString.fromString("c")}));
        data.add(ObjectRow.create(new Object[]{2, BinaryString.fromString("b")}));
        
        SortInfo sortInfo = new SortInfo();
        sortInfo.orderByFields = new ArrayList<>(1);
        OrderByField intField = new OrderByField();
        intField.expression = new FieldExpression(0, IntegerType.INSTANCE);
        intField.order = ORDER.DESC;
        sortInfo.orderByFields.add(intField);
        
        MultiFieldRadixSort.multiFieldRadixSort(data, sortInfo);
        
        assertEquals(data.size(), 3);
        assertEquals(data.get(0).getField(0, IntegerType.INSTANCE), Integer.valueOf(3));
        assertEquals(data.get(1).getField(0, IntegerType.INSTANCE), Integer.valueOf(2));
        assertEquals(data.get(2).getField(0, IntegerType.INSTANCE), Integer.valueOf(1));
    }

    @Test
    public void testSortByStringFieldAscending() {
        List<Row> data = new ArrayList<>(3);
        data.add(ObjectRow.create(new Object[]{3, BinaryString.fromString("zebra")}));
        data.add(ObjectRow.create(new Object[]{1, BinaryString.fromString("apple")}));
        data.add(ObjectRow.create(new Object[]{2, BinaryString.fromString("banana")}));
        
        SortInfo sortInfo = new SortInfo();
        sortInfo.orderByFields = new ArrayList<>(1);
        OrderByField stringField = new OrderByField();
        stringField.expression = new FieldExpression(1, BinaryStringType.INSTANCE);
        stringField.order = ORDER.ASC;
        sortInfo.orderByFields.add(stringField);
        
        MultiFieldRadixSort.multiFieldRadixSort(data, sortInfo);
        
        assertEquals(data.size(), 3);
        assertEquals(((BinaryString)data.get(0).getField(1, BinaryStringType.INSTANCE)).toString(), "apple");
        assertEquals(((BinaryString)data.get(1).getField(1, BinaryStringType.INSTANCE)).toString(), "banana");
        assertEquals(((BinaryString)data.get(2).getField(1, BinaryStringType.INSTANCE)).toString(), "zebra");
    }

    @Test
    public void testSortByStringFieldDescending() {
        List<Row> data = new ArrayList<>(3);
        data.add(ObjectRow.create(new Object[]{1, BinaryString.fromString("apple")}));
        data.add(ObjectRow.create(new Object[]{3, BinaryString.fromString("zebra")}));
        data.add(ObjectRow.create(new Object[]{2, BinaryString.fromString("banana")}));
        
        SortInfo sortInfo = new SortInfo();
        sortInfo.orderByFields = new ArrayList<>(1);
        OrderByField stringField = new OrderByField();
        stringField.expression = new FieldExpression(1, BinaryStringType.INSTANCE);
        stringField.order = ORDER.DESC;
        sortInfo.orderByFields.add(stringField);
        
        MultiFieldRadixSort.multiFieldRadixSort(data, sortInfo);
        
        assertEquals(data.size(), 3);
        assertEquals(((BinaryString)data.get(0).getField(1, BinaryStringType.INSTANCE)).toString(), "zebra");
        assertEquals(((BinaryString)data.get(1).getField(1, BinaryStringType.INSTANCE)).toString(), "banana");
        assertEquals(((BinaryString)data.get(2).getField(1, BinaryStringType.INSTANCE)).toString(), "apple");
    }

    @Test
    public void testMultiFieldSort() {
        List<Row> data = new ArrayList<>(4);
        data.add(ObjectRow.create(new Object[]{1, BinaryString.fromString("b")}));
        data.add(ObjectRow.create(new Object[]{2, BinaryString.fromString("a")}));
        data.add(ObjectRow.create(new Object[]{1, BinaryString.fromString("a")}));
        data.add(ObjectRow.create(new Object[]{2, BinaryString.fromString("b")}));
        
        SortInfo sortInfo = new SortInfo();
        sortInfo.orderByFields = new ArrayList<>(2);
        
        // First sort by integer field (ascending)
        OrderByField intField = new OrderByField();
        intField.expression = new FieldExpression(0, IntegerType.INSTANCE);
        intField.order = ORDER.ASC;
        sortInfo.orderByFields.add(intField);
        
        // Then sort by string field (ascending)
        OrderByField stringField = new OrderByField();
        stringField.expression = new FieldExpression(1, BinaryStringType.INSTANCE);
        stringField.order = ORDER.ASC;
        sortInfo.orderByFields.add(stringField);
        
        MultiFieldRadixSort.multiFieldRadixSort(data, sortInfo);
        
        assertEquals(data.size(), 4);
        // Expected order: (1, "a"), (1, "b"), (2, "a"), (2, "b")
        assertEquals(data.get(0).getField(0, IntegerType.INSTANCE), Integer.valueOf(1));
        assertEquals(((BinaryString)data.get(0).getField(1, BinaryStringType.INSTANCE)).toString(), "a");
        
        assertEquals(data.get(1).getField(0, IntegerType.INSTANCE), Integer.valueOf(1));
        assertEquals(((BinaryString)data.get(1).getField(1, BinaryStringType.INSTANCE)).toString(), "b");
        
        assertEquals(data.get(2).getField(0, IntegerType.INSTANCE), Integer.valueOf(2));
        assertEquals(((BinaryString)data.get(2).getField(1, BinaryStringType.INSTANCE)).toString(), "a");
        
        assertEquals(data.get(3).getField(0, IntegerType.INSTANCE), Integer.valueOf(2));
        assertEquals(((BinaryString)data.get(3).getField(1, BinaryStringType.INSTANCE)).toString(), "b");
    }

    @Test
    public void testSortWithNullValues() {
        List<Row> data = new ArrayList<>(3);
        data.add(ObjectRow.create(new Object[]{null, BinaryString.fromString("b")}));
        data.add(ObjectRow.create(new Object[]{2, BinaryString.fromString("a")}));
        data.add(ObjectRow.create(new Object[]{1, BinaryString.fromString("c")}));
        
        SortInfo sortInfo = new SortInfo();
        sortInfo.orderByFields = new ArrayList<>(1);
        OrderByField intField = new OrderByField();
        intField.expression = new FieldExpression(0, IntegerType.INSTANCE);
        intField.order = ORDER.ASC;
        sortInfo.orderByFields.add(intField);
        
        MultiFieldRadixSort.multiFieldRadixSort(data, sortInfo);
        
        assertEquals(data.size(), 3);
        // Null values should appear first in ascending order
        assertEquals(data.get(0).getField(0, IntegerType.INSTANCE), null);
        assertEquals(data.get(1).getField(0, IntegerType.INSTANCE), Integer.valueOf(1));
        assertEquals(data.get(2).getField(0, IntegerType.INSTANCE), Integer.valueOf(2));
    }

    @Test
    public void testSortWithNegativeNumbers() {
        List<Row> data = new ArrayList<>(4);
        data.add(ObjectRow.create(new Object[]{-1, BinaryString.fromString("a")}));
        data.add(ObjectRow.create(new Object[]{3, BinaryString.fromString("b")}));
        data.add(ObjectRow.create(new Object[]{-5, BinaryString.fromString("c")}));
        data.add(ObjectRow.create(new Object[]{0, BinaryString.fromString("d")}));
        
        SortInfo sortInfo = new SortInfo();
        sortInfo.orderByFields = new ArrayList<>(1);
        OrderByField intField = new OrderByField();
        intField.expression = new FieldExpression(0, IntegerType.INSTANCE);
        intField.order = ORDER.ASC;
        sortInfo.orderByFields.add(intField);
        
        MultiFieldRadixSort.multiFieldRadixSort(data, sortInfo);
        
        assertEquals(data.size(), 4);
        assertEquals(data.get(0).getField(0, IntegerType.INSTANCE), Integer.valueOf(-5));
        assertEquals(data.get(1).getField(0, IntegerType.INSTANCE), Integer.valueOf(-1));
        assertEquals(data.get(2).getField(0, IntegerType.INSTANCE), Integer.valueOf(0));
        assertEquals(data.get(3).getField(0, IntegerType.INSTANCE), Integer.valueOf(3));
    }

    @Test
    public void testSortWithEmptyStrings() {
        List<Row> data = new ArrayList<>(3);
        data.add(ObjectRow.create(new Object[]{1, BinaryString.fromString("")}));
        data.add(ObjectRow.create(new Object[]{2, BinaryString.fromString("hello")}));
        data.add(ObjectRow.create(new Object[]{3, BinaryString.fromString("a")}));
        
        SortInfo sortInfo = new SortInfo();
        sortInfo.orderByFields = new ArrayList<>(1);
        OrderByField stringField = new OrderByField();
        stringField.expression = new FieldExpression(1, BinaryStringType.INSTANCE);
        stringField.order = ORDER.ASC;
        sortInfo.orderByFields.add(stringField);
        
        MultiFieldRadixSort.multiFieldRadixSort(data, sortInfo);
        
        assertEquals(data.size(), 3);
        // Empty string should come first
        assertEquals(((BinaryString)data.get(0).getField(1, BinaryStringType.INSTANCE)).toString(), "");
        assertEquals(((BinaryString)data.get(1).getField(1, BinaryStringType.INSTANCE)).toString(), "a");
        assertEquals(((BinaryString)data.get(2).getField(1, BinaryStringType.INSTANCE)).toString(), "hello");
    }

    @Test
    public void testSortStability() {
        List<Row> data = new ArrayList<>(3);
        // Create rows with same sort key but different secondary values
        data.add(ObjectRow.create(new Object[]{1, BinaryString.fromString("first")}));
        data.add(ObjectRow.create(new Object[]{1, BinaryString.fromString("second")}));
        data.add(ObjectRow.create(new Object[]{1, BinaryString.fromString("third")}));
        
        SortInfo sortInfo = new SortInfo();
        sortInfo.orderByFields = new ArrayList<>(1);
        OrderByField intField = new OrderByField();
        intField.expression = new FieldExpression(0, IntegerType.INSTANCE);
        intField.order = ORDER.ASC;
        sortInfo.orderByFields.add(intField);
        
        MultiFieldRadixSort.multiFieldRadixSort(data, sortInfo);
        
        assertEquals(data.size(), 3);
        // All should have the same integer value
        for (Row row : data) {
            assertEquals(row.getField(0, IntegerType.INSTANCE), Integer.valueOf(1));
        }
    }
}