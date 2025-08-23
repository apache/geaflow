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

import java.util.Calendar;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.dsl.common.function.Description;
import org.apache.geaflow.dsl.common.function.UDF;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

@Description(name = "date_part", description = "Returns part of the date by date part "
    + "format.")
public class DatePart extends UDF {

    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormat.forPattern(
        "yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd");
    private final Calendar calendar = Calendar.getInstance();

    public Integer eval(String date, String datepart) {
        if (date == null || datepart == null) {
            return null;
        }
        DateTimeFormatter formatter;
        if (date.length() <= 10) {
            formatter = DATE_FORMATTER;
        } else {
            formatter = DATE_TIME_FORMATTER;
        }
        try {
            calendar.setTime(formatter.parseDateTime(date).toDate());
        } catch (Exception e) {
            throw new GeaflowRuntimeException(e);
        }
        switch (datepart) {
            case "yyyy":
            case "year":
                return calendar.get(Calendar.YEAR);
            case "mm":
            case "mon":
            case "month":
                return 1 + calendar.get(Calendar.MONTH);
            case "dd":
            case "day":
                return calendar.get(Calendar.DAY_OF_MONTH);
            case "hh":
            case "hour":
                return calendar.get(Calendar.HOUR_OF_DAY);
            case "mi":
            case "minute":
                return calendar.get(Calendar.MINUTE);
            case "ss":
            case "second":
                return calendar.get(Calendar.SECOND);
            default:
                throw new RuntimeException("unknown datepart:" + datepart);

        }
    }
}
