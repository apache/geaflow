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

import java.util.Date;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.dsl.common.function.Description;
import org.apache.geaflow.dsl.common.function.UDF;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

@Description(name = "unix_timestamp", description = "Returns the UNIX timestamp.")
public class UnixTimeStamp extends UDF {

    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd");
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormat.forPattern(
        "yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter MILLS_FORMATTER = DateTimeFormat.forPattern(
        "yyyy-MM-dd HH:mm:ss.SSSSSS");
    private String lastPatternText;
    private DateTimeFormatter formatter;

    public Long eval() {
        Date date = new Date();
        return date.getTime() / 1000;
    }

    public Long eval(Object dateText) {
        if (dateText == null) {
            return eval();
        }
        return eval(String.valueOf(dateText));
    }

    public Long eval(String dateText) {
        if (dateText == null) {
            return eval();
        }

        DateTimeFormatter formatter;

        if (dateText.length() <= 10) {
            formatter = DATE_FORMATTER;
        } else if (dateText.length() <= 19) {
            formatter = DATE_TIME_FORMATTER;
        } else {
            formatter = MILLS_FORMATTER;
        }
        try {
            return formatter.parseDateTime(dateText).getMillis() / 1000;
        } catch (Exception e) {
            throw new GeaflowRuntimeException(e);
        }
    }

    public Long eval(String dateText, String patternText) {
        if (dateText == null || patternText == null) {
            return null;
        }
        try {
            if (!patternText.equals(lastPatternText)) {
                formatter = DateTimeFormat.forPattern(patternText);
                lastPatternText = patternText;
            }
            return formatter.parseDateTime(dateText).getMillis() / 1000;

        } catch (Exception e) {
            throw new GeaflowRuntimeException(e);
        }
    }
}
