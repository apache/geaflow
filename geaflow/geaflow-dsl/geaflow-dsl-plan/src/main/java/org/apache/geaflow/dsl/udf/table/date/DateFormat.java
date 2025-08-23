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
import org.apache.geaflow.dsl.common.function.FunctionContext;
import org.apache.geaflow.dsl.common.function.UDF;

@Description(name = "date_format", description = "Returns convert the date from a format to "
    + "another.")
public class DateFormat extends UDF {

    private FromUnixTime fromUnixTime;
    private UnixTimeStamp unixTimeStamp;

    public void open(FunctionContext context) {
        super.open(context);
        fromUnixTime = new FromUnixTime();
        unixTimeStamp = new UnixTimeStamp();
        fromUnixTime.open(context);
        unixTimeStamp.open(context);
    }

    public String eval(String dateText, String toFormat) {
        String format = "yyyy-MM-dd HH:mm:ss";

        if (dateText != null && dateText.length() > 19) {
            char sep = dateText.charAt(19);
            format = "yyyy-MM-dd HH:mm:ss" + sep + "SSSSSS";
        }
        return eval(dateText, format, toFormat);
    }

    public String eval(String dateText) {
        return eval(dateText, "yyyy-MM-dd HH:mm:ss");
    }

    public String eval(java.sql.Timestamp timestamp, String toFormat) {
        return eval(timestamp.toString(), toFormat);
    }

    public String eval(java.sql.Timestamp timestamp) {
        return eval(timestamp.toString());
    }

    public String eval(String dateText, String fromFormat, String toFormat) {
        if (dateText == null || fromFormat == null || toFormat == null) {
            return null;
        }
        return fromUnixTime.eval(unixTimeStamp.eval(dateText, fromFormat), toFormat);
    }
}
