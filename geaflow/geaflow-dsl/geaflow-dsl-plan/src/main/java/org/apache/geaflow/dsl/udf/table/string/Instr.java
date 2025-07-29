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

import org.apache.geaflow.common.binary.BinaryString;
import org.apache.geaflow.dsl.common.function.Description;
import org.apache.geaflow.dsl.common.function.UDF;

@Description(name = "instr", description = "Returns the position of the first occurrence of sub string in str.")
public class Instr extends UDF {

    public Long eval(String str, String target) {
        return eval(str, target, 1L, 1L);
    }

    public Long eval(String str, String target, Long from) {
        return eval(str, target, from, 1L);
    }

    public Long eval(String str, String target, Long from, Long nth) {
        if (str == null || target == null || from == null || nth == null) {
            return null;
        }
        if (nth <= 0) {
            return null;
        }
        int fromIndex = from.intValue() - 1;
        if (fromIndex < 0) {
            return null;
        }
        for (int i = 0; i < nth; ++i) {
            fromIndex = str.indexOf(target, fromIndex) + 1;
        }
        return (long) fromIndex;
    }

    public Long eval(BinaryString str, BinaryString target) {
        return eval(str, target, 1L, 1L);
    }

    public Long eval(BinaryString str, BinaryString target, Long from) {
        return eval(str, target, from, 1L);
    }

    public Long eval(BinaryString str, BinaryString target, Long from, Long nth) {
        if (str == null || target == null || from == null || nth == null) {
            return null;
        }
        if (nth <= 0) {
            return null;
        }
        int fromIndex = from.intValue() - 1;
        if (fromIndex < 0) {
            return null;
        }
        for (int i = 0; i < nth; ++i) {
            fromIndex = str.indexOf(target, fromIndex) + 1;
        }
        return (long) fromIndex;
    }
}
