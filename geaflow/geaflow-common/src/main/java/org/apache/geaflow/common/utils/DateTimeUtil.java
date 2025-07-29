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

package org.apache.geaflow.common.utils;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class DateTimeUtil {

    public static long toUnixTime(String dateStr, String format) {
        if (dateStr == null || dateStr.isEmpty()) {
            return -1;
        } else {
            DateTimeFormatter dateTimeFormat = DateTimeFormat.forPattern(format);
            return dateTimeFormat.parseMillis(dateStr);
        }
    }

    public static String fromUnixTime(long unixTime, String format) {
        return DateTimeFormat.forPattern(format).print(unixTime);
    }
}
