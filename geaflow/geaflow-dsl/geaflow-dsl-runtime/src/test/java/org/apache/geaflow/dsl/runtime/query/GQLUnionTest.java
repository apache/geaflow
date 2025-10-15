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

public class GQLUnionTest {

    @Test
    public void testUnion_001() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_union_001.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testUnion_002() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_union_002.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testUnion_003() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_union_003.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testUnion_004() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_union_004.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testUnion_005() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_union_005.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testUnion_006() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_union_006.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testUnion_007() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_union_007.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testUnion_008() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_union_008.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testUnion_009() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_union_009.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testUnion_010() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_union_010.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testUnion_011() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_union_011.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testUnion_012() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_union_012.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testUnion_013() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_union_013.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testUnion_014() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_union_014.sql")
            .execute()
            .checkSinkResult();
    }
}
