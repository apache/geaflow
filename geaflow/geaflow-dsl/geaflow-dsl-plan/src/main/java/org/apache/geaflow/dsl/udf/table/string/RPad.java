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

import org.apache.geaflow.dsl.common.function.Description;
import org.apache.geaflow.dsl.common.function.UDF;

@Description(name = "rpad", description = "Returns the string right-padded to the given length "
    + "with the specified pad string.")
public class RPad extends UDF {

    public String eval(String str, Integer len, String pad) {
        if (str == null || len == null || pad == null) {
            return null;
        }
        if (len <= 0) {
            return "";
        }
        if (len <= str.length()) {
            return str.substring(0, len);
        }
        if (pad.isEmpty()) {
            return str;
        }
        StringBuilder sb = new StringBuilder();
        sb.append(str);
        while (sb.length() < len) {
            sb.append(pad);
        }
        sb.setLength(len);
        return sb.toString();
    }
}