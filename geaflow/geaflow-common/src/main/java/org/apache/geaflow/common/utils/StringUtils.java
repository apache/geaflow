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

import java.util.Arrays;

/**
 * Created by eagle on 2017/8/30.
 */
public class StringUtils {

    public static String arrayAwareToString(Object o) {
        if (o == null) {
            return "null";
        }
        if (o.getClass().isArray()) {
            return arrayToString(o);
        }

        return o.toString();
    }

    public static String arrayToString(Object array) {
        if (array == null) {
            throw new NullPointerException();
        }

        if (array instanceof int[]) {
            return Arrays.toString((int[]) array);
        }
        if (array instanceof long[]) {
            return Arrays.toString((long[]) array);
        }
        if (array instanceof Object[]) {
            return Arrays.toString((Object[]) array);
        }
        if (array instanceof byte[]) {
            return Arrays.toString((byte[]) array);
        }
        if (array instanceof double[]) {
            return Arrays.toString((double[]) array);
        }
        if (array instanceof float[]) {
            return Arrays.toString((float[]) array);
        }
        if (array instanceof boolean[]) {
            return Arrays.toString((boolean[]) array);
        }
        if (array instanceof char[]) {
            return Arrays.toString((char[]) array);
        }
        if (array instanceof short[]) {
            return Arrays.toString((short[]) array);
        }

        if (array.getClass().isArray()) {
            return "<unknown array type>";
        } else {
            throw new IllegalArgumentException("The given argument is no array.");
        }
    }
}
