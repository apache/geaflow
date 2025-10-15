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

import java.sql.Timestamp;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.dsl.common.function.Description;
import org.apache.geaflow.dsl.common.function.UDF;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

@Description(name = "day", description =
    "Get number of days of the date or datetime expression expr.")
public class Day extends UDF {

    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormat.forPattern(
        "yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd");

    public Integer eval(String dateString) {
        if (dateString == null) {
            return null;
        }
        DateTimeFormatter formatter;
        try {
            if (dateString.length() <= 10) {
                formatter = DATE_FORMATTER;
            } else {
                formatter = DATE_TIME_FORMATTER;
            }

            java.util.Date date = formatter.parseDateTime(dateString).toDate();
            return date.getDate();
        } catch (Exception e) {
            throw new GeaflowRuntimeException(e);
        }
    }

    public Integer eval(Timestamp i) {
        if (i == null) {
            return null;
        }
        return i.getDate();
    }
}
