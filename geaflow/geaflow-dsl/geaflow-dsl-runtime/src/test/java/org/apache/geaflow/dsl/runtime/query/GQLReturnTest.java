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

public class GQLReturnTest {

    @Test
    public void testReturn_001() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_return_001.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testReturn_002() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_return_002.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testReturn_003() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_return_003.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testReturn_004() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_return_004.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testReturn_005() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_return_005.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testReturn_006() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_return_006.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testReturn_007() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_return_007.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testReturn_008() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_return_008.sql")
            .compareWithOrder()
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testReturn_009() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_return_009.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testReturn_010() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_return_010.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testReturn_011() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_return_011.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testReturn_012() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_return_012.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testReturn_013() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_return_013.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testReturn_014() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_return_014.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testReturn_015() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_return_015.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testReturn_016() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_return_016.sql")
            .execute()
            .checkSinkResult();
    }
}
