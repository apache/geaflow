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

package org.apache.geaflow.dsl.udf.table.array;

import org.apache.geaflow.dsl.common.function.Description;
import org.apache.geaflow.dsl.common.function.UDF;

@Description(name = "array_max", description = "Return the maximum element of input array.")
public class ArrayMax extends UDF {

    @SuppressWarnings({"rawtypes", "unchecked"})
    public Object eval(Object[] input) {
        if (input == null || input.length == 0) {
            return null;
        }
        Comparable max = null;
        for (Object element : input) {
            if (element == null) {
                continue;
            }
            if (!(element instanceof Comparable)) {
                throw new IllegalArgumentException(
                    "array_max only supports comparable element types");
            }
            Comparable comparable = (Comparable) element;
            if (max == null || comparable.compareTo(max) > 0) {
                max = comparable;
            }
        }
        return max;
    }
}
