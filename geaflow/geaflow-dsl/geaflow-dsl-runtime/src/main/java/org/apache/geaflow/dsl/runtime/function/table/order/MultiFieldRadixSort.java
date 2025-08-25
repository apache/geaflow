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

package org.apache.geaflow.dsl.runtime.function.table.order;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IntSummaryStatistics;
import java.util.List;
import java.util.Objects;
import org.apache.geaflow.common.binary.BinaryString;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.common.type.Types;
import org.apache.geaflow.dsl.common.data.Row;

public class MultiFieldRadixSort {
    
    /**
     * Multi-field radix sort.
     */
    public static void multiFieldRadixSort(List<Row> data,
                                    SortInfo sortInfo) {
        if (data == null || data.size() <= 1) return;
        
        // Sort by field with the lowest priority.
        List<OrderByField> fields = sortInfo.orderByFields;

        for (int i = fields.size() - 1; i >= 0; i--) {
            OrderByField field = fields.get(i);
            IType<?> orderType = field.expression.getOutputType();
            if (Types.getType(orderType.getTypeClass()) == Types.INTEGER) {
                radixSortByIntField(data, field);
            } else if (Types.getType(orderType.getTypeClass()) == Types.BINARY_STRING) {
                radixSortByStringField(data, field);
            }
        }
    }
    
    /**
     * Radix sort by integer field.
     */
    private static void radixSortByIntField(List<Row> data, 
                                               OrderByField field) {
        if (data.isEmpty()) return;
        
        // Determine the number of digits.
        IntSummaryStatistics stats = data.stream()
            .map(item -> field.expression.evaluate(item))
            .filter(Objects::nonNull)
            .filter(obj -> obj instanceof Number)  // Make sure it is a numeric type.
            .mapToInt(obj -> ((Number) obj).intValue())
            .summaryStatistics();
        int max = 0, min = 0;
        if (stats.getCount() > 0) {
            max = stats.getMax();
            min = stats.getMin();
        }
        
        // Handling negative numbers: Add the offset to all numbers to make them positive.
        int offset = min < 0 ? -min : 0;
        max += offset;
        
        // Bitwise sorting.
        for (int exp = 1; max / exp > 0; exp *= 10) {
            countingSortByDigit(data, field, exp, offset);
        }
    }
    
    /**
     * Radix sorting by string field.
     */
    private static void radixSortByStringField(List<Row> data, 
                                                  OrderByField field) {
        if (data.isEmpty()) return;
        
        int maxLength = data.stream()
            .map(item -> ((BinaryString)field.expression.evaluate(item)).getLength())
            .filter(Objects::nonNull)
            .mapToInt(Integer::intValue)
            .max()
            .orElse(0);

        // Sort from the last digit of the string.
        for (int pos = maxLength - 1; pos >= 0; pos--) {
            countingSortByChar(data, field, pos);
        }
    }
    
    /**
     * Sort by the specified number of digits (integer).
     */
    private static void countingSortByDigit(List<Row> data, 
                                               OrderByField field,
                                               int exp, int offset) {
        int n = data.size();
        List<Row> output = new ArrayList<>(Collections.nCopies(n, null));
        int[] count = new int[10];
        
        // Pre-calculate all values to avoid repeated evaluation.
        int[] values = new int[n];
        for (int i = 0; i < n; i++) {
            values[i] = (Integer) field.expression.evaluate(data.get(i));
        }
        
        // Count the number of times each number appears.
        for (int i = 0; i < n; i++) {
            int digit = (values[i] + offset) / exp % 10;
            count[digit]++;
        }
        
        // Calculate cumulative count.
        if (field.order.value>0){
            for (int i = 1; i < 10; i++) {
                count[i] += count[i - 1];
            }
        }else{
            for (int i = 8; i >= 0; i--) {
                count[i] += count[i + 1];
            }
        }
        // Build the output array from back to front (to ensure stability).
        for (int i = n - 1; i >= 0; i--) {
            int digit = (values[i] + offset) / exp % 10;
            output.set(count[digit] - 1, data.get(i));
            count[digit]--;
        }
        
        // Copy back to the original array.
        for (int i = 0; i < n; i++) {
            data.set(i, output.get(i));
        }
    }
    
    /**
     * Sort by the specified number of digits (string).
     */
    private static void countingSortByChar(List<Row> data, 
                                              OrderByField field,
                                              int pos) {
        int n = data.size();
        List<Row> output = new ArrayList<>(Collections.nCopies(n, null));
        
        // Precompute all strings and character codes to avoid repeated evaluate and toString.
        String[] strings = new String[n];
        int[] charCodes = new int[n];

        int minChar = Integer.MAX_VALUE;
        int maxChar = Integer.MIN_VALUE;

        for (int i = 0; i < n; i++) {
            strings[i] = ((BinaryString)field.expression.evaluate(data.get(i))).toString();
            if (pos < strings[i].length()) {
                int charCode = strings[i].codePointAt(pos);
                minChar = Math.min(minChar, charCode);
                maxChar = Math.max(maxChar, charCode);
            }
        }
        int range = maxChar - minChar + 2;
        int[] count = new int[range];

        for (int i = 0; i < n; i++) {
            charCodes[i] = pos < strings[i].length() ? strings[i].charAt(pos) : 0;
            charCodes[i] = charCodes[i] == 0 ? 0 : charCodes[i] - minChar + 1;
            count[charCodes[i]]++;
        }
        
        if (field.order.value>0){
            for (int i = 1; i < range; i++) {
                count[i] += count[i - 1];
            }
        }else{
            for (int i = range-2; i >= 0; i--) {
                count[i] += count[i + 1];
            }
        }
        
        for (int i = n - 1; i >= 0; i--) {
            output.set(count[charCodes[i]] - 1, data.get(i));
            count[charCodes[i]]--;
        }
        
        for (int i = 0; i < n; i++) {
            data.set(i, output.get(i));
        }
    }
}
