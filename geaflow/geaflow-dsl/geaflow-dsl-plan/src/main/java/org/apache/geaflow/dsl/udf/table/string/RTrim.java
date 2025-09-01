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

import org.apache.commons.lang3.StringUtils;
import org.apache.geaflow.common.binary.BinaryString;
import org.apache.geaflow.dsl.common.function.Description;
import org.apache.geaflow.dsl.common.function.UDF;

@Description(name = "rtrim", description = "Returns a string with the right space removed.")
public class RTrim extends UDF {

    public String eval(String s) {
        if (s == null) {
            return null;
        }
        return StringUtils.stripEnd(s, " ");
    }

    public BinaryString eval(BinaryString s) {
        if (s == null) {
            return null;
        }
        int r = s.getLength() - 1;
        while (r >= 0) {
            if (s.getByte(r) == ' ') {
                r--;
            } else {
                break;
            }
        }
        return s.substring(0, r + 1);
    }
}
