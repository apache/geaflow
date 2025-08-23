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
import java.util.Date;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.dsl.common.function.Description;
import org.apache.geaflow.dsl.common.function.UDF;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

@Description(name = "date_diff", description = "Returns the number of days from startdate to end date.")
public class DateDiff extends UDF {

    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormat.forPattern(
        "yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd");

    public Integer eval(String dateString1, String dateString2) {
        return eval(toDate(dateString1), toDate(dateString2));
    }

    public Integer eval(Timestamp t1, Timestamp t2) {
        return eval(toDate(t1), toDate(t2));
    }

    public Integer eval(Timestamp t, String dateString) {
        return eval(toDate(t), toDate(dateString));
    }

    public Integer eval(String dateString, Timestamp t) {
        return eval(toDate(dateString), toDate(t));
    }

    private Integer eval(Date date1, Date date2) {
        if (date1 == null || date2 == null) {
            return null;
        }

        long diffInMilliSeconds = date1.getTime() - date2.getTime();
        return (int) (diffInMilliSeconds / (86400 * 1000));
    }

    private Date format(String dateString) {
        try {
            DateTimeFormatter formatter;
            if (dateString.length() <= 10) {
                formatter = DATE_FORMATTER;
            } else {
                formatter = DATE_TIME_FORMATTER;
            }
            return formatter.parseDateTime(dateString).toDate();
        } catch (Exception e) {
            throw new GeaflowRuntimeException(e);
        }
    }

    private Date toDate(String dateString) {
        if (dateString == null) {
            return null;
        }
        return format(dateString);
    }

    private Date toDate(Timestamp t) {
        return t;
    }
}
