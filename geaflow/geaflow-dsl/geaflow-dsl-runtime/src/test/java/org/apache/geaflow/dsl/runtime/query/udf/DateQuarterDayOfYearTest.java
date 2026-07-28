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

package org.apache.geaflow.dsl.runtime.query.udf;

import org.apache.geaflow.dsl.runtime.query.QueryTester;
import org.testng.annotations.Test;

/**
 * End-to-end SQL tests for the QUARTER and DAY_OF_YEAR date UDFs.
 *
 * <p>Unlike the unit tests in {@code UDFDateTest} which invoke the Java
 * {@code eval()} methods directly, these tests exercise the full SQL
 * pipeline: function registration in {@code BuildInSqlFunctionTable},
 * overload resolution, type inference, and runtime invocation through
 * the GeaFlow query engine.</p>
 */
public class DateQuarterDayOfYearTest {

    @Test
    public void testQuarterDayOfYear() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/query/date_quarter_dayofyear_001.sql")
            .execute()
            .checkSinkResult();
    }
}
