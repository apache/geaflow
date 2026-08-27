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

package org.apache.geaflow.dsl.runtime.query;

import java.util.TimeZone;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TypesTest {

    private static TimeZone originalTimeZone;

    /**
     * Fix the JVM timezone to Asia/Shanghai (UTC+8) for the duration of this
     * test class so that timestamp-to-string conversions produce the values
     * stored in the expect files, regardless of the host machine's locale.
     */
    @BeforeClass
    public static void setUpTimeZone() {
        originalTimeZone = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"));
    }

    @AfterClass
    public static void tearDownTimeZone() {
        TimeZone.setDefault(originalTimeZone);
    }

    @Test
    public void testBooleanType_001() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/query/type_boolean_001.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testTimestampType_001() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/query/type_timestamp_001.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testDateType_001() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/query/type_date_001.sql")
            .execute()
            .checkSinkResult();
    }
}
