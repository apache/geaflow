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
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import org.apache.geaflow.dsl.udf.table.date.AddMonths;
import org.apache.geaflow.dsl.udf.table.date.DateAdd;
import org.apache.geaflow.dsl.udf.table.date.DateDiff;
import org.apache.geaflow.dsl.udf.table.date.DateFormat;
import org.apache.geaflow.dsl.udf.table.date.DatePart;
import org.apache.geaflow.dsl.udf.table.date.DateSub;
import org.apache.geaflow.dsl.udf.table.date.DateTrunc;
import org.apache.geaflow.dsl.udf.table.date.Day;
import org.apache.geaflow.dsl.udf.table.date.DayOfMonth;
import org.apache.geaflow.dsl.udf.table.date.FromUnixTime;
import org.apache.geaflow.dsl.udf.table.date.FromUnixTimeMillis;
import org.apache.geaflow.dsl.udf.table.date.Hour;
import org.apache.geaflow.dsl.udf.table.date.IsDate;
import org.apache.geaflow.dsl.udf.table.date.LastDay;
import org.apache.geaflow.dsl.udf.table.date.Minute;
import org.apache.geaflow.dsl.udf.table.date.Month;
import org.apache.geaflow.dsl.udf.table.date.Now;
import org.apache.geaflow.dsl.udf.table.date.Second;
import org.apache.geaflow.dsl.udf.table.date.UnixTimeStamp;
import org.apache.geaflow.dsl.udf.table.date.UnixTimeStampMillis;
import org.apache.geaflow.dsl.udf.table.date.WeekDay;
import org.apache.geaflow.dsl.udf.table.date.WeekOfYear;
import org.apache.geaflow.dsl.udf.table.date.Year;
import org.testng.annotations.Test;

public class UDFDateTest {

    @Test
    public void testAddMonths() {
        AddMonths addMonths = new AddMonths();
        addMonths.open(null);
        assertEquals(addMonths.eval("1987-06-05 00:11:22", 5), "1987-11-05 00:11:22");
        assertEquals(addMonths.eval("1987-06-05", 5), "1987-11-05");
        assertEquals(addMonths.eval("1987-06-05 00:11:22", null), null);
    }

    @Test
    public void testDateAdd() {
        DateAdd test = new DateAdd();
        test.open(null);
        assertEquals(test.eval("1987-06-05 00:11:22", 5), "1987-06-10");
        assertEquals(test.eval(new java.sql.Timestamp(1667900725), 5), "1970-01-25 15:18:20");
        assertEquals(test.eval("1987-06-05", 5), "1987-06-10");
        assertEquals(test.eval(new java.sql.Timestamp(1667900725), null), null);
        assertEquals(test.eval("1987-06-05", null), null);
    }

    @Test
    public void testDateDiff() {
        DateDiff test = new DateDiff();
        test.open(null);
        assertEquals((long) test.eval("1987-06-05 00:11:22", "2022-06-05 00:11:22"), -12784);
        assertEquals((long) test.eval(new java.sql.Timestamp(1667900725), "2022-06-05"), -19128);
        assertEquals((long) test.eval("1987-06-05", new java.sql.Timestamp(1667900725)), 6344);
        assertNull(test.eval((String) null, (String) null));
        assertNull(test.eval(new java.sql.Timestamp(1867900725), (java.sql.Timestamp) null));
    }

    @Test
    public void testDateFormat() {
        DateFormat test = new DateFormat();
        test.open(null);
        assertEquals(test.eval("1987-06-05 00:11:22", "MM-dd-yyyy"), "06-05-1987");
        assertEquals(test.eval("1987-06-05 00:11:22"), "1987-06-05 00:11:22");
        assertEquals(test.eval(new java.sql.Timestamp(1867900725), "MM-dd-yyyy"), "01-22-1970");
        assertEquals(test.eval(new java.sql.Timestamp(1867900725)), "1970-01-22 22:51:40");
        assertEquals(test.eval("1987-06-05 00:11:22", null), null);
    }

    @Test
    public void testDatePart() {
        DatePart test = new DatePart();
        test.open(null);
        assertEquals((int) test.eval("1987-06-05 00:11:22", "yyyy"), 1987);
        assertEquals((int) test.eval("1987-06-05 00:11:22", "mm"), 6);
        assertEquals((int) test.eval("1987-06-05 00:11:22", "dd"), 5);
        assertEquals((int) test.eval("1987-06-05 00:11:22", "hh"), 0);
        assertEquals((int) test.eval("1987-06-05 00:11:22", "mi"), 11);
        assertEquals((int) test.eval("1987-06-05 00:11:22", "ss"), 22);
        assertNull(test.eval("1987-06-05 00:11:22", null));
        assertEquals((int) test.eval("1987-06-05", "ss"), 0);
    }

    @Test
    public void testDateSub() {
        DateSub test = new DateSub();
        test.open(null);
        assertEquals(test.eval("1987-06-05 00:11:22", 5), "1987-05-31");
        assertEquals(test.eval((String) null, 5), null);
        assertEquals(test.eval("1987-06-05", 5), "1987-05-31");
        assertEquals(test.eval(new java.sql.Timestamp(1867900725), 5), "1970-01-17 22:51:40");
        assertNull(test.eval(new java.sql.Timestamp(1867900725), null));
    }

    @Test
    public void testDateTrunc() {
        DateTrunc test = new DateTrunc();
        test.open(null);
        assertEquals(test.eval("1987-06-05 00:11:22", "yyyy"), "1987-01-01 00:00:00");
        assertEquals(test.eval("1987-06-05 00:11:22", "mm"), "1987-06-01 00:00:00");
        assertEquals(test.eval("1987-06-05 00:11:22", "dd"), "1987-06-05 00:00:00");
        assertEquals(test.eval("1987-06-05 00:11:22", "hh"), "1987-06-05 00:00:00");
        assertEquals(test.eval("1987-06-05 00:11:22", "mi"), "1987-06-05 00:11:00");
        assertEquals(test.eval("1987-06-05 00:11:22", "ss"), "1987-06-05 00:11:22");
        assertNull(test.eval("1987-06-05 00:11:22", null));
        assertEquals(test.eval("1987-06-05", "yyyy"), "1987-01-01 00:00:00");
    }

    @Test
    public void testDay() {
        Day test = new Day();
        test.open(null);
        assertEquals((int) test.eval("1987-06-05 00:11:22"), 5);
        assertEquals((int) test.eval("1987-06-05"), 5);
        assertNull(test.eval((String) null));
        assertNull(test.eval((java.sql.Timestamp) null));
        assertEquals((int) test.eval(new java.sql.Timestamp(1867900725)), 22);
    }

    @Test
    public void testDayOfMonth() {
        DayOfMonth test = new DayOfMonth();
        test.open(null);
        assertEquals((int) test.eval("1987-06-05 00:11:22"), 5);
        assertNull(test.eval((String) null));
        assertNull(test.eval((java.sql.Timestamp) null));
        assertEquals((int) test.eval(new java.sql.Timestamp(1867900725)), 22);
    }

    @Test
    public void testFromUnixTime() {
        FromUnixTime test = new FromUnixTime();
        test.open(null);
        assertEquals(test.eval(11111111L), "1970-05-09 22:25:11");
        assertNull(test.eval(null));
        assertEquals(test.eval("11111111", "yyyy-MM-dd HH:mm:ss"), "1970-05-09 22:25:11");
    }

    @Test
    public void testFromUnixTimeMillis() {
        FromUnixTimeMillis test = new FromUnixTimeMillis();
        test.open(null);
        assertEquals(test.eval(11111111L), "1970-01-01 11:05:11.111");
        assertEquals(test.eval("11111111"), "1970-01-01 11:05:11.111");
        assertNull(test.eval((Long) null));
        assertNull(test.eval(11111111L, null));
        assertEquals(test.eval(11111111L, "yyyy-MM-dd HH:mm:ss"), "1970-01-01 11:05:11");
    }

    @Test
    public void testHour() {
        Hour test = new Hour();
        test.open(null);
        assertEquals((int) test.eval("1987-06-05 00:11:22"), 0);
        assertEquals((int) test.eval(new java.sql.Timestamp(1667900725)), 3);
        assertNull(test.eval((String) null));
        assertNull(test.eval((java.sql.Timestamp) null));
    }

    @Test
    public void testIsDate() {
        IsDate test = new IsDate();
        test.open(null);
        assertEquals((boolean) test.eval("1987-06-05 00:11:22"), true);
        assertTrue(test.eval("1987-06-05 00:11:22"));
        assertFalse(test.eval("xxxxxxxxxxxxx"));
    }

    @Test
    public void testLastDay() {
        LastDay test = new LastDay();
        test.open(null);
        assertEquals(test.eval("1987-06-05"), "1987-06-30 00:00:00");
    }

    @Test
    public void testMinute() {
        Minute test = new Minute();
        test.open(null);
        assertEquals((int) test.eval("1987-06-05 00:11:22"), 11);
        assertEquals((int) test.eval(new java.sql.Timestamp(1667900725)), 18);
        assertNull(test.eval((String) null));
        assertNull(test.eval((java.sql.Timestamp) null));
    }

    @Test
    public void testMonth() {
        Month test = new Month();
        test.open(null);
        assertEquals((int) test.eval("1987-06-05 00:11:22"), 6);
        assertEquals((int) test.eval(new java.sql.Timestamp(1667900725)), 1);
        assertNull(test.eval((String) null));
        assertNull(test.eval((java.sql.Timestamp) null));
    }

    @Test
    public void testNow() {
        Now test = new Now();
        test.open(null);
        test.eval();
        test.eval(3);
        test.eval(3L);
    }

    @Test
    public void testSecond() {
        Second test = new Second();
        test.open(null);
        assertEquals((int) test.eval("1987-06-05 00:11:22"), 22);
        assertEquals((int) test.eval(new java.sql.Timestamp(1667900725)), 20);
        assertNull(test.eval((String) null));
        assertNull(test.eval((java.sql.Timestamp) null));
    }

    @Test
    public void testUnixTimeStamp() {
        UnixTimeStamp test = new UnixTimeStamp();
        test.open(null);
        assertEquals((long) test.eval("1987-06-05 00:11:22"), 549817882);
        assertEquals((long) test.eval((Object) "1987-06-05 00:11:22.33"), 549817882);
        assertEquals((long) test.eval((Object) "1987-06-05"), 549817200);
        assertNull(test.eval("1987-06-05 00:11:22", null));
    }

    @Test
    public void testUnixTimeStampMillis() {
        UnixTimeStampMillis test = new UnixTimeStampMillis();
        test.open(null);
        assertEquals((long) test.eval("1987-06-05 00:11:22"), 549817882000L);
        assertEquals((long) test.eval("1987-06-05", "yyyy-mm-dd"), 536774760000L);
        assertNull(test.eval("1987-06-05", null));
        assertEquals((long) test.eval("1987-06-05"), 549817200000L);
        assertEquals((long) test.eval((Object) "1987-06-05"), 549817200000L);
        assertEquals((long) test.eval("1987-06-05 00:11:22.111"), 549817882111L);
    }

    @Test
    public void testWeekDay() {
        WeekDay test = new WeekDay();
        test.open(null);
        assertEquals((long) test.eval("1987-06-05 00:11:22"), 5);
        assertEquals((long) test.eval("1987-06-05"), 5);
        assertNull(test.eval(null));
    }

    @Test
    public void testWeekOfYear() {
        WeekOfYear test = new WeekOfYear();
        test.open(null);
        assertEquals((long) test.eval("1987-06-05 00:11:22"), 23);
        assertNull(test.eval((String) null));
        assertNull(test.eval((java.sql.Timestamp) null));
        assertEquals((long) test.eval("1987-06-05"), 23);
        assertEquals((long) test.eval(new java.sql.Timestamp(1667900725)), 4);
    }

    @Test
    public void testYear() {
        Year test = new Year();
        test.open(null);
        assertEquals((long) test.eval("1987-06-05 00:11:22"), 1987);
        assertNull(test.eval((String) null));
        assertNull(test.eval((java.sql.Timestamp) null));
        assertEquals((long) test.eval("1987-06-05"), 1987);
        assertEquals((long) test.eval(new java.sql.Timestamp(1667900725)), 1970);
    }
}
