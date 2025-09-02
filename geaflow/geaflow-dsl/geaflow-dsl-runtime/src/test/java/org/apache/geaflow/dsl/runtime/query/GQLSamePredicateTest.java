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
 * Integration tests for Same Predicate feature.
 * These tests verify the complete functionality of the SAME_PREDICATE pattern
 * from parsing to execution.
 */
public class GQLSamePredicateTest {

    /**
     * Test basic same predicate with simple condition.
     * Tests: (a:person) -> (b) | (a:person) -> (c) WHERE SAME(a.age > 25)
     */
    @Test
    public void testSamePredicate_001() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_same_predicate_001.sql")
            .execute()
            .checkSinkResult();
    }

    /**
     * Test same predicate with distinct semantics.
     * Tests: (a:person) -> (b) | (a:person) -> (c) WHERE SAME(a.age > 25) DISTINCT
     */
    @Test
    public void testSamePredicate_002() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_same_predicate_002.sql")
            .execute()
            .checkSinkResult();
    }

    /**
     * Test same predicate with complex condition involving multiple variables.
     * Tests: (a:person) -> (b) | (a:person) -> (c) WHERE SAME(a.age > 25 AND b.id != c.id)
     */
    @Test
    public void testSamePredicate_003() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_same_predicate_003.sql")
            .execute()
            .checkSinkResult();
    }

    /**
     * Test same predicate with edge conditions.
     * Tests: (a:person) -[e1:knows]-> (b) | (a:person) -[e2:created]-> (c) WHERE SAME(e1.weight > 0.5)
     */
    @Test
    public void testSamePredicate_004() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_same_predicate_004.sql")
            .execute()
            .checkSinkResult();
    }

    /**
     * Test same predicate with nested path patterns.
     * Tests: (a:person) -> (b) -> (c) | (a:person) -> (d) WHERE SAME(a.age > 25 AND b.id != d.id)
     */
    @Test
    public void testSamePredicate_005() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_same_predicate_005.sql")
            .execute()
            .checkSinkResult();
    }

    /**
     * Test same predicate with union all semantics.
     * Tests: (a:person) -> (b) |+| (a:person) -> (c) WHERE SAME(a.age > 25)
     */
    @Test
    public void testSamePredicate_006() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_same_predicate_006.sql")
            .execute()
            .checkSinkResult();
    }

    /**
     * Test same predicate with complex nested conditions.
     * Tests: (a:person) -> (b) | (a:person) -> (c) WHERE SAME(a.age > 25 OR (b.id = 1 AND c.id = 2))
     */
    @Test
    public void testSamePredicate_007() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_same_predicate_007.sql")
            .execute()
            .checkSinkResult();
    }

    /**
     * Test same predicate with multiple path patterns.
     * Tests: (a:person) -> (b) | (a:person) -> (c) | (a:person) -> (d) WHERE SAME(a.age > 25)
     */
    @Test
    public void testSamePredicate_008() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_same_predicate_008.sql")
            .execute()
            .checkSinkResult();
    }

    /**
     * Test same predicate with edge direction conditions.
     * Tests: (a:person) -[e1:knows]-> (b) | (a:person) <-[e2:created]- (c) WHERE SAME(e1.weight = e2.weight)
     */
    @Test
    public void testSamePredicate_009() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_same_predicate_009.sql")
            .execute()
            .checkSinkResult();
    }

    /**
     * Test same predicate with vertex type conditions.
     * Tests: (a:person) -> (b:person) | (a:person) -> (c:software) WHERE SAME(a.age > 25)
     */
    @Test
    public void testSamePredicate_010() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_same_predicate_010.sql")
            .execute()
            .checkSinkResult();
    }

    /**
     * Test same predicate with complex arithmetic expressions.
     * Tests: (a:person) -> (b) | (a:person) -> (c) WHERE SAME(a.age * 2 > 50)
     */
    @Test
    public void testSamePredicate_011() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_same_predicate_011.sql")
            .execute()
            .checkSinkResult();
    }

    /**
     * Test same predicate with string conditions.
     * Tests: (a:person) -> (b) | (a:person) -> (c) WHERE SAME(a.name = 'marko')
     */
    @Test
    public void testSamePredicate_012() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_same_predicate_012.sql")
            .execute()
            .checkSinkResult();
    }

    /**
     * Test same predicate with null handling.
     * Tests: (a:person) -> (b) | (a:person) -> (c) WHERE SAME(a.age IS NOT NULL)
     */
    @Test
    public void testSamePredicate_013() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_same_predicate_013.sql")
            .execute()
            .checkSinkResult();
    }

    /**
     * Test same predicate with function calls.
     * Tests: (a:person) -> (b) | (a:person) -> (c) WHERE SAME(LENGTH(a.name) > 4)
     */
    @Test
    public void testSamePredicate_014() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_same_predicate_014.sql")
            .execute()
            .checkSinkResult();
    }

    /**
     * Test same predicate with subquery integration.
     * Tests: (a:person) -> (b) | (a:person) -> (c) WHERE SAME(a.id IN (SELECT id FROM person WHERE age > 25))
     */
    @Test
    public void testSamePredicate_015() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_same_predicate_015.sql")
            .execute()
            .checkSinkResult();
    }
}
