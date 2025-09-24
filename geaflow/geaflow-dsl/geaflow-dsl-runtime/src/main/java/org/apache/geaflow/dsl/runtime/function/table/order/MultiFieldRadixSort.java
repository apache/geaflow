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

    private static int dataSize;

    private static int[] intValues;

    private static int[] sortedIntValues;

    private static int[] charCodes;

    private static byte[] digits;

    private static String[] stringValues;

    private static String[] sortedStringValues;

    private static Row[] srcData;

    private static Row[] dstData;

    /**
     * Multi-field radix sort.
     */
    public static void multiFieldRadixSort(List<Row> data, SortInfo sortInfo) {
        dataSize = data.size();
        if (data == null || dataSize <= 1) {
            return;
        }

        // Init arrays.
        intValues = new int[dataSize];
        sortedIntValues = new int[dataSize];
        charCodes = new int[dataSize];
        digits = new byte[dataSize];
        stringValues = new String[dataSize];
        sortedStringValues = new String[dataSize];
        srcData = data.toArray(new Row[0]);
        dstData = new Row[dataSize];

        // Sort by field with the lowest priority.
        List<OrderByField> fields = sortInfo.orderByFields;

        for (int i = fields.size() - 1; i >= 0; i--) {
            OrderByField field = fields.get(i);
            if (field.expression.getOutputType().getTypeClass() == Integer.class) {
                radixSortByIntField(data, field);
            } else {
                radixSortByStringField(data, field);
            }
            for (int j = 0; j < dataSize; j++) {
                data.set(j, srcData[j]);
            }
        }
    }

    /**
     * Radix sort by integer field.
     */
    private static void radixSortByIntField(List<Row> data, OrderByField field) {
        // Determine the number of digits.
        int max = Integer.MIN_VALUE;
        int min = Integer.MAX_VALUE;
        boolean hasNull = false;
        
        for (int i = 0; i < dataSize; i++) {
            Integer value = (Integer) field.expression.evaluate(data.get(i));
            if (value != null) {
                intValues[i] = value;
                max = value > max ? value : max;
                min = value < min ? value : min;
            } else {
                intValues[i] = Integer.MIN_VALUE;
                hasNull = true;
            }
        }
        if (hasNull) {
            min--;
        }
        
        // Handling negative numbers: Add the offset to all numbers to make them positive.
        final int offset = min < 0 ? -min : 0;
        max += offset;

        for (int i = 0; i < dataSize; i++) {
            if (intValues[i] == Integer.MIN_VALUE) {
                intValues[i] = min;
            }
            intValues[i] += offset;
        }

        // Bitwise sorting.
        for (int exp = 1; max / exp > 0; exp *= 10) {
            for (int j = 0; j < dataSize; j++) {
                digits[j] = (byte) (intValues[j] / exp % 10);
            }
            countingSortByDigit(field.order.value > 0);
        }
    }

    /**
     * Radix sorting by string field.
     */
    private static void radixSortByStringField(List<Row> data, OrderByField field) {
        // Precompute all strings to avoid repeated evaluation and toString.
        int maxLength = 0;
        
        for (int i = 0; i < dataSize; i++) {
            BinaryString binaryString = (BinaryString) field.expression.evaluate(data.get(i));
            stringValues[i] = binaryString != null ? binaryString.toString() : "";
            maxLength = Math.max(maxLength, stringValues[i].length());
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
        int[] count = new int[10];

        // Count the number of times each number appears.
        for (int i = 0; i < dataSize; i++) {
            count[digits[i]]++;
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
        for (int i = dataSize - 1; i >= 0; i--) {
            int index = --count[digits[i]];
            dstData[index] = srcData[i];
            sortedIntValues[index] = intValues[i];
        }

        int[] intTmp = intValues;
        intValues = sortedIntValues;
        sortedIntValues = intTmp;
        
        Row[] rowTmp = srcData;
        srcData = dstData;
        dstData = rowTmp;
    }

    /**
     * Sort by the specified number of digits (string).
     */
    private static void countingSortByChar(boolean ascending, int pos) {
        // Precompute all strings and character codes to avoid repeated evaluate and toString.
        int minChar = Integer.MAX_VALUE;
        int maxChar = Integer.MIN_VALUE;

        for (int i = 0; i < dataSize; i++) {
            String value = stringValues[i];
            if (pos < value.length()) {
                int charCode = value.codePointAt(pos);
                charCodes[i] = charCode;
                minChar = Math.min(minChar, charCode);
                maxChar = Math.max(maxChar, charCode);
            }
        }
        int range = maxChar - minChar + 2;
        int[] count = new int[range];

        for (int i = 0; i < dataSize; i++) {
            if (pos < stringValues[i].length()) {
                charCodes[i] -= (minChar - 1);
            } else {
                charCodes[i] = 0; // null character
            }
            count[charCodes[i]]++;
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

        for (int i = dataSize - 1; i >= 0; i--) {
            int index = --count[charCodes[i]];
            dstData[index] = srcData[i];
            sortedStringValues[index] = stringValues[i];
        }

        String[] stringTmp = stringValues;
        stringValues = sortedStringValues;
        sortedStringValues = stringTmp;
        
        Row[] rowTmp = srcData;
        srcData = dstData;
        dstData = rowTmp;
    }
}
