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

import org.testng.annotations.Test;

public class AggregateTest {

    @Test
    public void testAggregate_001() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/query/aggregate_001.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testAggregate_002() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/query/aggregate_002.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testAggregate_003() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/query/aggregate_003.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testAggregate_004() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/query/aggregate_004.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testAggregate_005() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/query/aggregate_005.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testAggregate_006() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/query/aggregate_006.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testAggregate_007() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/query/aggregate_007.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testAggregate_008() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/query/aggregate_008.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testAggregate_009() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/query/aggregate_009.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testAggregate_010() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/query/aggregate_010.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testAggregate_011() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/query/aggregate_011.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testAggregate_012() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/query/aggregate_012.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testStreamAggregate_001() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/query/stream_aggregate_001.sql")
            .execute()
            .checkSinkResult();
    }

}
