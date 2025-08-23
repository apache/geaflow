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

@Description(name = "regexp_count", description = "Returns the count that the pattern matched in the string.")
public class RegexpCount extends UDF {

    private String lastPattern;
    private Pattern p = null;

    public Long eval(String source, String pattern, Long startPos) {
        if (source == null || pattern == null || startPos == null) {
            return null;
        }
        if (lastPattern == null || !lastPattern.equals(pattern)) {
            p = Pattern.compile(pattern);
            lastPattern = pattern;
        }
        Matcher matcher = p.matcher(source.substring(startPos.intValue()));
        long c = 0;
        while (matcher.find()) {
            c++;
        }
        return c;
    }

    public Long eval(String source, String pattern) {
        return eval(source, pattern, 0L);
    }
}
