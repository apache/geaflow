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

package org.apache.geaflow.dsl.udf.table.string;

final class StringPadUtil {

    private StringPadUtil() {
    }

    static String pad(String str, Integer length, String pad, boolean left) {
        if (str == null || length == null || pad == null) {
            return null;
        }
        if (length < 0) {
            return null;
        }
        if (length == 0) {
            return "";
        }

        int strLength = str.codePointCount(0, str.length());
        if (length <= strLength) {
            return substring(str, length);
        }
        if (pad.isEmpty()) {
            return str;
        }

        String padding = repeat(pad, length - strLength);
        return left ? padding + str : str + padding;
    }

    private static String repeat(String pad, int length) {
        int padLength = pad.codePointCount(0, pad.length());
        int repeatCount = length / padLength;
        int remainder = length % padLength;
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < repeatCount; i++) {
            result.append(pad);
        }
        if (remainder > 0) {
            result.append(substring(pad, remainder));
        }
        return result.toString();
    }

    private static String substring(String value, int codePointCount) {
        return value.substring(0, value.offsetByCodePoints(0, codePointCount));
    }
}
