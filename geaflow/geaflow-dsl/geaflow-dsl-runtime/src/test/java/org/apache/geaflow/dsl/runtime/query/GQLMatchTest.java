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

public class GQLMatchTest {

    @Test
    public void testMatch_001() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_match_001.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testMatch_002() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_match_002.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testMatch_003() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_match_003.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testMatch_004() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_match_004.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testMatch_005() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_match_005.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testMatch_006() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_match_006.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testMatch_007() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/query/gql_match_007.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testMatch_008() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/query/gql_match_008.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testMatch_009() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/query/gql_match_009.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testMatch_010() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_match_010.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testMatch_011() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_match_011.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testMatch_012() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/query/gql_match_012.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testMatch_013() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_match_013.sql")
            .execute()
            .checkSinkResult();
    }
}
