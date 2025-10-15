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

package org.apache.geaflow.dsl.udf.table.date;

import org.apache.geaflow.dsl.common.function.Description;
import org.apache.geaflow.dsl.common.function.UDF;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

@Description(name = "from_unixtime", description = "Translate unix timestamp to the date string.")
public class FromUnixTime extends UDF {

    private DateTimeFormatter formatter;
    private String lastFormat;

    private static final String DEFAULT_FORMAT = "yyyy-MM-dd HH:mm:ss";

    public String eval(String unixTime, String format) {
        return eval(Long.valueOf(unixTime), format);
    }

    public String eval(Long unixTime, String format) {
        return evaluate(unixTime, format);
    }

    public String eval(Long unixTime) {
        return eval(unixTime, DEFAULT_FORMAT);
    }

    private String evaluate(Long unixTime, String format) {
        if (unixTime == null || format == null) {
            return null;
        }
        if (!format.equals(lastFormat)) {
            formatter = DateTimeFormat.forPattern(format);
            lastFormat = format;
        }
        return formatter.print(unixTime * 1000L);
    }
}
