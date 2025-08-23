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

package org.apache.geaflow.common.utils;

import java.math.BigDecimal;

public class MemoryUtils {

    public static final long KB = 1024L;
    public static final long MB = 1024L * KB;
    public static final long GB = 1024L * MB;
    public static final long TB = 1024L * GB;
    public static final long PB = 1024L * TB;
    private static final String BYTE_UNITS = "KMGTPE";

    public static String humanReadableByteCount(long bytes) {
        int unit = 1024;
        if (bytes < unit) {
            return bytes + "B";
        }
        int exp = (int) (Math.log(bytes) / Math.log(unit));
        char pre = BYTE_UNITS.charAt(exp - 1);
        BigDecimal bd = new BigDecimal(bytes / Math.pow(unit, exp));
        return String.format("%s%cB", bd.setScale(2, BigDecimal.ROUND_DOWN), pre);
    }
}
