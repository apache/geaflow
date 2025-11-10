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

import java.util.List;
import org.apache.geaflow.common.binary.BinaryString;
import org.apache.geaflow.dsl.common.data.Row;

public class MultiFieldRadixSort {

    private static final ThreadLocal<Integer> dataSize = new ThreadLocal<>();
    private static final ThreadLocal<int[]> intValues = new ThreadLocal<>();
    private static final ThreadLocal<int[]> sortedIntValues = new ThreadLocal<>();
    private static final ThreadLocal<int[]> charCodes = new ThreadLocal<>();
    private static final ThreadLocal<byte[]> digits = new ThreadLocal<>();
    private static final ThreadLocal<String[]> stringValues = new ThreadLocal<>();
    private static final ThreadLocal<String[]> sortedStringValues = new ThreadLocal<>();
    private static final ThreadLocal<Row[]> srcData = new ThreadLocal<>();
    private static final ThreadLocal<Row[]> dstData = new ThreadLocal<>();

    /**
     * Multi-field radix sort.
     */
    public static void multiFieldRadixSort(List<Row> data, SortInfo sortInfo) {
        if (data == null || data.size() <= 1) {
            return;
        }
        int size = data.size();

        try {
            dataSize.set(size);
            intValues.set(new int[size]);
            sortedIntValues.set(new int[size]);
            charCodes.set(new int[size]);
            digits.set(new byte[size]);
            stringValues.set(new String[size]);
            sortedStringValues.set(new String[size]);
            srcData.set(data.toArray(new Row[0]));
            dstData.set(new Row[size]);

            // Sort by field with the lowest priority.
            List<OrderByField> fields = sortInfo.orderByFields;

            for (int i = fields.size() - 1; i >= 0; i--) {
                OrderByField field = fields.get(i);
                if (field.expression.getOutputType().getTypeClass() == Integer.class) {
                    radixSortByIntField(field);
                } else {
                    radixSortByStringField(field);
                }
            }

            Row[] finalData = srcData.get();
            for (int j = 0; j < size; j++) {
                data.set(j, finalData[j]);
            }
        } finally {
            dataSize.remove();
            intValues.remove();
            sortedIntValues.remove();
            charCodes.remove();
            digits.remove();
            stringValues.remove();
            sortedStringValues.remove();
            srcData.remove();
            dstData.remove();
        }
    }

    /**
     * Radix sort by integer field.
     */
    private static void radixSortByIntField(OrderByField field) {
        int size = dataSize.get();
        int[] intVals = intValues.get();
        byte[] digs = digits.get();
        Row[] src = srcData.get();
        
        // Determine the number of digits.
        int max = Integer.MIN_VALUE;
        int min = Integer.MAX_VALUE;
        boolean hasNull = false;
        
        for (int i = 0; i < size; i++) {
            Integer value = (Integer) field.expression.evaluate(src[i]);
            if (value != null) {
                intVals[i] = value;
                max = value > max ? value : max;
                min = value < min ? value : min;
            } else {
                intVals[i] = Integer.MIN_VALUE;
                hasNull = true;
            }
        }
        if (hasNull) {
            min--;
        }
        
        // Handling negative numbers: Add the offset to all numbers to make them positive.
        final int offset = min < 0 ? -min : 0;
        max += offset;

        for (int i = 0; i < size; i++) {
            if (intVals[i] == Integer.MIN_VALUE) {
                intVals[i] = min;
            }
            intVals[i] += offset;
        }

        // Bitwise sorting.
        for (int exp = 1; max / exp > 0; exp *= 10) {
            for (int j = 0; j < size; j++) {
                digs[j] = (byte) (intVals[j] / exp % 10);
            }
            countingSortByDigit(field.order.value > 0);
        }
    }

    /**
     * Radix sorting by string field.
     */
    private static void radixSortByStringField(OrderByField field) {
        int size = dataSize.get();
        String[] strVals = stringValues.get();
        Row[] src = srcData.get();
        
        // Precompute all strings to avoid repeated evaluation and toString.
        int maxLength = 0;
        
        for (int i = 0; i < size; i++) {
            BinaryString binaryString = (BinaryString) field.expression.evaluate(src[i]);
            strVals[i] = binaryString != null ? binaryString.toString() : "";
            maxLength = Math.max(maxLength, strVals[i].length());
        }

        // Sort from the last digit of the string.
        for (int pos = maxLength - 1; pos >= 0; pos--) {
            countingSortByChar(field.order.value > 0, pos);
        }
    }

    /**
     * Sort by the specified number of digits (integer).
     */
    private static void countingSortByDigit(boolean ascending) {
        int size = dataSize.get();
        byte[] digs = digits.get();
        int[] intVals = intValues.get();
        int[] sortedIntVals = sortedIntValues.get();
        Row[] src = srcData.get();
        Row[] dst = dstData.get();

        int[] count = new int[10];

        // Count the number of times each number appears.
        for (int i = 0; i < size; i++) {
            count[digs[i]]++;
        }

        // Calculate cumulative count.
        if (ascending) {
            for (int i = 1; i < 10; i++) {
                count[i] += count[i - 1];
            }
        } else {
            for (int i = 8; i >= 0; i--) {
                count[i] += count[i + 1];
            }
        }

        // Build the output array from back to front (to ensure stability).
        for (int i = size - 1; i >= 0; i--) {
            int index = --count[digs[i]];
            dst[index] = src[i];
            sortedIntVals[index] = intVals[i];
        }

        int[] intTmp = intVals;
        intValues.set(sortedIntVals);
        sortedIntValues.set(intTmp);
        
        Row[] rowTmp = src;
        srcData.set(dst);
        dstData.set(rowTmp);
    }

    /**
     * Sort by the specified number of digits (string).
     */
    private static void countingSortByChar(boolean ascending, int pos) {
        int size = dataSize.get();
        String[] strVals = stringValues.get();
        String[] sortedStrVals = sortedStringValues.get();
        int[] charCds = charCodes.get();
        Row[] src = srcData.get();
        Row[] dst = dstData.get();

        // Precompute all strings and character codes to avoid repeated evaluate and toString.
        int minChar = Integer.MAX_VALUE;
        int maxChar = Integer.MIN_VALUE;

        for (int i = 0; i < size; i++) {
            String value = strVals[i];
            if (pos < value.length()) {
                int charCode = value.codePointAt(pos);
                charCds[i] = charCode;
                minChar = Math.min(minChar, charCode);
                maxChar = Math.max(maxChar, charCode);
            }
        }
        int range = maxChar - minChar + 2;
        int[] count = new int[range];

        for (int i = 0; i < size; i++) {
            if (pos < strVals[i].length()) {
                charCds[i] -= (minChar - 1);
            } else {
                charCds[i] = 0; // null character
            }
            count[charCds[i]]++;
        }

        if (ascending) {
            for (int i = 1; i < range; i++) {
                count[i] += count[i - 1];
            }
        } else {
            for (int i = range - 2; i >= 0; i--) {
                count[i] += count[i + 1];
            }
        }

        for (int i = size - 1; i >= 0; i--) {
            int index = --count[charCds[i]];
            dst[index] = src[i];
            sortedStrVals[index] = strVals[i];
        }

        String[] stringTmp = strVals;
        stringValues.set(sortedStrVals);
        sortedStringValues.set(stringTmp);
        
        Row[] rowTmp = src;
        srcData.set(dst);
        dstData.set(rowTmp);
    }
}