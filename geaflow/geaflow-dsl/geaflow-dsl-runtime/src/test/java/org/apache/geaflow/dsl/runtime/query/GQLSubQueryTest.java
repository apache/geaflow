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

public class GQLSubQueryTest {

    @Test
    public void testSubQuery_001() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_subquery_001.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testSubQuery_002() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_subquery_002.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testSubQuery_003() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_subquery_003.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testSubQuery_004() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_subquery_004.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testSubQuery_005() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_subquery_005.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testSubQuery_006() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_subquery_006.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testSubQuery_007() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_subquery_007.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testSubQuery_008() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_subquery_008.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testSubQuery_009() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_subquery_009.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testSubQuery_010() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_subquery_010.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testSubQuery_011() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_subquery_011.sql")
            .execute()
            .checkSinkResult();
    }

}
