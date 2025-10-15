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

package org.apache.geaflow.console.common.util;

import com.google.common.base.Preconditions;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.commons.lang3.time.DateUtils;

public class DateTimeUtil {

    public static final String DATE_TIME_FORMATTER = "yyyy-MM-dd HH:mm:ss";

    public static String format(Date date) {
        return date == null ? null : new SimpleDateFormat(DATE_TIME_FORMATTER).format(date);
    }

    public static boolean isExpired(Date date, int liveSeconds) {
        Preconditions.checkNotNull(date, "Invalid date");
        return DateUtils.addSeconds(date, liveSeconds).before(new Date());
    }

}
