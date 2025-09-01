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

@Description(name = "reverse", description = "Returns the reversed string.")
public class Reverse extends UDF {

    public String eval(String s) {
        return StringUtils.reverse(s);
    }

    public BinaryString eval(BinaryString s) {
        if (s == null) {
            return null;
        }
        String reverse = StringUtils.reverse(s.toString());
        return BinaryString.fromString(reverse);
    }
}
