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

package org.apache.geaflow.dsl.udf.date;

import static org.testng.Assert.assertEquals;

import org.apache.geaflow.dsl.udf.table.date.UnixTimeStampMillis;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class UnixTimeStampMillisTest {

    UnixTimeStampMillis udf;

    @BeforeMethod
    public void setUp() throws Exception {
        udf = new UnixTimeStampMillis();
    }

    @Test
    public void test() {

        assertEquals(udf.eval("1993", "yyyy"), new Long(725817600000L));

        assertEquals(udf.eval("1993-12", "yyyy-MM"), new Long(754675200000L));

        assertEquals(udf.eval("1993-12-01", "yyyy-MM-dd"), new Long(754675200000L));

        assertEquals(udf.eval("1993-12-01 12", "yyyy-MM-dd HH"), new Long(754718400000L));

        assertEquals(udf.eval("1993-12-01 12:03", "yyyy-MM-dd HH:mm"), new Long(754718580000L));

        assertEquals(udf.eval("1993-12-01 12:03:01"), new Long(754718581000L));

        assertEquals(udf.eval("1993-12-01 12:03:01", "yyyy-MM-dd HH:mm:ss"),
            new Long(754718581000L));

        assertEquals(udf.eval("1993-12-01 12:03:01.111"), new Long(754718581111L));

        assertEquals(udf.eval("1993-12-01 12:03:01.111", "yyyy-MM-dd HH:mm:ss.SSS"),
            new Long(754718581111L));

        DateTimeFormatter millisFormatter = DateTimeFormat.forPattern(
            "yyyy-MM-dd'T'HH:mm:ss.SSS+00:00");
        assertEquals(millisFormatter.parseMillis("2010-04-13T15:39:24.399+00:00"),
            1271144364399L);

    }

}
