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

/**
 * Integration tests for ISO-GQL Value Type Predicate (IS TYPED / IS NOT TYPED).
 * Tests the complete SQL execution path from parsing to runtime evaluation.
 */
public class GQLValueTypePredicateTest {

    @Test
    public void testIsTyped_001() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_is_typed_001.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testIsTyped_002() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_is_typed_002.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testIsTyped_003() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_is_typed_003.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testIsTyped_004() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_is_typed_004.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testIsTyped_005() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_is_typed_005.sql")
            .execute()
            .checkSinkResult();
    }
}

