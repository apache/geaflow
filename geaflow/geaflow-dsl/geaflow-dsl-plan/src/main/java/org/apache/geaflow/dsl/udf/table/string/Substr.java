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

@Description(name = "substr", description = "Returns the substring for start position with specified length"
    + ". The position start from 1.")
public class Substr extends UDF {

    public String eval(String str, Integer pos, Integer length) {
        if (str == null || pos == null || length == null) {
            return null;
        }
        if ((Math.abs(pos) > str.length())) {
            return str;
        }

        int start;
        int end;
        if (pos > 0) {
            start = pos - 1;
        } else if (pos < 0) {
            start = str.length() + pos;
        } else {
            start = 0;
        }
        if (length == -1) {
            end = str.length();
        } else {
            end = Math.min(start + length, str.length());
        }
        return str.substring(start, end);
    }

    public String eval(String str, Integer start) {
        return eval(str, start, -1);
    }

    public BinaryString eval(BinaryString str, Integer start) {
        return eval(str, start, -1);
    }

    public BinaryString eval(BinaryString str, Integer pos, Integer length) {
        if (str == null || pos == null || length == null) {
            return null;
        }
        if (Math.abs(pos) > str.getLength()) {
            return null;
        }
        int start;
        int end;
        if (pos > 0) {
            start = pos - 1;
        } else if (pos < 0) {
            start = str.getLength() + pos;
        } else {
            start = 0;
        }
        if (length == -1) {
            end = str.getLength();
        } else {
            end = Math.min(start + length, str.getLength());
        }
        return str.substring(start, end);
    }
}
