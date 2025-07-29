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
import static org.testng.Assert.assertNull;

import java.util.Date;
import org.apache.geaflow.dsl.udf.table.date.UnixTimeStamp;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class UnixTimeStampTest {

    UnixTimeStamp udf;

    @BeforeMethod
    public void setUp() throws Exception {
        udf = new UnixTimeStamp();
    }

    @Test
    public void test() {

        assertEquals(udf.eval("1993", "yyyy"), new Long(725817600));

        assertEquals(udf.eval("1993-12", "yyyy-MM"), new Long(754675200));

        assertEquals(udf.eval("1993-12-01", "yyyy-MM-dd"), new Long(754675200));

        assertEquals(udf.eval("1993-12-01 12", "yyyy-MM-dd HH"), new Long(754718400));

        assertEquals(udf.eval("1993-12-01 12:03", "yyyy-MM-dd HH:mm"), new Long(754718580));

        assertEquals(udf.eval("1993-12-01 12:03:01"), new Long(754718581));

        assertEquals(udf.eval("1993-12-01 12:03:01", "yyyy-MM-dd HH:mm:ss"), new Long(754718581));

        assertEquals(udf.eval("1993-12-01 12:03:01.111000"), new Long(754718581));

        assertEquals(udf.eval("1993-12-01 12:03:01,111", "yyyy-MM-dd HH:mm:ss,SSS"),
            new Long(754718581));

        assertEquals(udf.eval(null), new Long(new Date().getTime() / 1000));

        assertNull(udf.eval(null, null));

        assertEquals(udf.eval("1993-12-01 00:00:00"), new Long(754675200));
    }

}
