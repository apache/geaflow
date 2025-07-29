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

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.geaflow.dsl.common.function.Description;
import org.apache.geaflow.dsl.common.function.UDF;

@Description(name = "regexp", description = "Returns whether the string match the regular expression.")
public class RegExp extends UDF {

    private String lastRegex;
    private Pattern p = null;

    public Boolean eval(String s, String regex) {
        if (s == null || regex == null) {
            return null;
        }
        if (regex.length() == 0) {
            return false;
        }
        if (!regex.equals(lastRegex) || p == null) {
            lastRegex = regex;
            p = Pattern.compile(regex);
        }
        Matcher m = p.matcher(s);
        return m.find(0);
    }
}
