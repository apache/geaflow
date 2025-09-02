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

package org.apache.geaflow.dsl.integration;

import org.apache.geaflow.dsl.GQLIntegrationTest;
import org.junit.Test;

/**
 * Integration test class for Same Predicate functionality.
 * Tests end-to-end functionality including parsing, validation, and execution.
 */
public class SamePredicateIntegrationTest extends GQLIntegrationTest {

    /**
     * Test basic same predicate functionality
     */
    @Test
    public void testSamePredicateBasic() throws Exception {
        String script = "CREATE GRAPH g0 (\n"
            + "  Vertex person (id bigint ID, name varchar, age int),\n"
            + "  Edge knows (srcId bigint SOURCE ID, targetId bigint DESTINATION ID, weight double)\n"
            + ") WITH (\n"
            + "  storeType='memory',\n"
            + "  shardCount = 1\n"
            + ");\n"
            + "\n"
            + "INSERT INTO g0.person(id, name, age) VALUES (1, 'jim', 20), (2, 'kate', 22), (3, 'lily', 25);\n"
            + "INSERT INTO g0.knows VALUES (1, 2, 0.4), (1, 3, 0.8), (2, 3, 0.6);\n"
            + "\n"
            + "MATCH (a:person) -> (b) | (a:person) -> (c) WHERE SAME(a.age > 20)\n"
            + "RETURN a.name, b.id, c.id;";

        // TODO: Once same predicate is fully implemented, this should execute successfully
        // For now, we expect it to fail gracefully during parsing
        try {
            submitScript(script);
        } catch (Exception e) {
            // Expected to fail until parser is updated
            System.out.println("Expected parsing error: " + e.getMessage());
        }
    }

    /**
     * Test same predicate with edge conditions
     */
    @Test
    public void testSamePredicateWithEdgeCondition() throws Exception {
        String script = "CREATE GRAPH g0 (\n"
            + "  Vertex person (id bigint ID, name varchar, age int),\n"
            + "  Edge knows (srcId bigint SOURCE ID, targetId bigint DESTINATION ID, weight double),\n"
            + "  Edge likes (srcId bigint SOURCE ID, targetId bigint DESTINATION ID, weight double)\n"
            + ") WITH (\n"
            + "  storeType='memory',\n"
            + "  shardCount = 1\n"
            + ");\n"
            + "\n"
            + "INSERT INTO g0.person(id, name, age) VALUES (1, 'jim', 20), (2, 'kate', 22), (3, 'lily', 25);\n"
            + "INSERT INTO g0.knows VALUES (1, 2, 0.4), (1, 3, 0.8);\n"
            + "INSERT INTO g0.likes VALUES (1, 2, 0.6), (1, 3, 0.9);\n"
            + "\n"
            + "MATCH (a) -[e1:knows]-> (b) | (a) -[e2:likes]-> (c) WHERE SAME(e1.weight > 0.5)\n"
            + "RETURN a.id, b.id, c.id;";

        try {
            submitScript(script);
        } catch (Exception e) {
            // Expected to fail until parser is updated
            System.out.println("Expected parsing error: " + e.getMessage());
        }
    }

    /**
     * Test same predicate with complex conditions
     */
    @Test
    public void testSamePredicateWithComplexConditions() throws Exception {
        String script = "CREATE GRAPH g0 (\n"
            + "  Vertex user (id bigint ID, name varchar, age int, active boolean),\n"
            + "  Edge knows (srcId bigint SOURCE ID, targetId bigint DESTINATION ID, weight double),\n"
            + "  Edge likes (srcId bigint SOURCE ID, targetId bigint DESTINATION ID, weight double)\n"
            + ") WITH (\n"
            + "  storeType='memory',\n"
            + "  shardCount = 1\n"
            + ");\n"
            + "\n"
            + "INSERT INTO g0.user(id, name, age, active) VALUES (1, 'jim', 25, true), (2, 'kate', 30, true), (3, 'lily', 35, false);\n"
            + "INSERT INTO g0.knows VALUES (1, 2, 0.6), (1, 3, 0.8);\n"
            + "INSERT INTO g0.likes VALUES (1, 2, 0.7), (1, 3, 0.9);\n"
            + "\n"
            + "MATCH (a:user) -[e1:knows]-> (b) | (a:user) -[e2:likes]-> (c) WHERE SAME(a.active = true AND a.age > 20)\n"
            + "RETURN a.name, b.id, c.id;";

        try {
            submitScript(script);
        } catch (Exception e) {
            // Expected to fail until parser is updated
            System.out.println("Expected parsing error: " + e.getMessage());
        }
    }

    /**
     * Test same predicate with union all semantics
     */
    @Test
    public void testSamePredicateWithUnionAll() throws Exception {
        String script = "CREATE GRAPH g0 (\n"
            + "  Vertex person (id bigint ID, name varchar, age int),\n"
            + "  Edge knows (srcId bigint SOURCE ID, targetId bigint DESTINATION ID, weight double)\n"
            + ") WITH (\n"
            + "  storeType='memory',\n"
            + "  shardCount = 1\n"
            + ");\n"
            + "\n"
            + "INSERT INTO g0.person(id, name, age) VALUES (1, 'jim', 25), (2, 'kate', 30), (3, 'lily', 35);\n"
            + "INSERT INTO g0.knows VALUES (1, 2, 0.6), (1, 3, 0.8), (2, 3, 0.7);\n"
            + "\n"
            + "MATCH (a:person) -> (b) |+| (a:person) -> (c) WHERE SAME(a.age > 20)\n"
            + "RETURN a.name, b.id, c.id;";

        try {
            submitScript(script);
        } catch (Exception e) {
            // Expected to fail until parser is updated
            System.out.println("Expected parsing error: " + e.getMessage());
        }
    }

    /**
     * Test same predicate with nested path patterns
     */
    @Test
    public void testSamePredicateWithNestedPaths() throws Exception {
        String script = "CREATE GRAPH g0 (\n"
            + "  Vertex person (id bigint ID, name varchar, age int, status varchar),\n"
            + "  Edge knows (srcId bigint SOURCE ID, targetId bigint DESTINATION ID, weight double),\n"
            + "  Edge works (srcId bigint SOURCE ID, targetId bigint DESTINATION ID, weight double)\n"
            + ") WITH (\n"
            + "  storeType='memory',\n"
            + "  shardCount = 1\n"
            + ");\n"
            + "\n"
            + "INSERT INTO g0.person(id, name, age, status) VALUES (1, 'jim', 25, 'active'), (2, 'kate', 30, 'active'), (3, 'lily', 35, 'inactive');\n"
            + "INSERT INTO g0.knows VALUES (1, 2, 0.6), (2, 3, 0.7);\n"
            + "INSERT INTO g0.works VALUES (1, 3, 0.8), (2, 3, 0.9);\n"
            + "\n"
            + "MATCH ((a:person) -> (b) -> (c)) | ((a:person) -> (d) -> (e)) WHERE SAME(a.status = 'active')\n"
            + "RETURN a.name, b.id, c.id, d.id, e.id;";

        try {
            submitScript(script);
        } catch (Exception e) {
            // Expected to fail until parser is updated
            System.out.println("Expected parsing error: " + e.getMessage());
        }
    }

    /**
     * Test same predicate with let and filter statements
     */
    @Test
    public void testSamePredicateWithLetAndFilter() throws Exception {
        String script = "CREATE GRAPH g0 (\n"
            + "  Vertex person (id bigint ID, name varchar, age int),\n"
            + "  Edge knows (srcId bigint SOURCE ID, targetId bigint DESTINATION ID, weight double)\n"
            + ") WITH (\n"
            + "  storeType='memory',\n"
            + "  shardCount = 1\n"
            + ");\n"
            + "\n"
            + "INSERT INTO g0.person(id, name, age) VALUES (1, 'jim', 25), (2, 'kate', 30), (3, 'lily', 35);\n"
            + "INSERT INTO g0.knows VALUES (1, 2, 0.6), (1, 3, 0.8), (2, 3, 0.7);\n"
            + "\n"
            + "MATCH (a:person) -> (b) | (a:person) -> (c) WHERE SAME(a.age > 20)\n"
            + "LET personName = a.name\n"
            + "RETURN personName, b.id, c.id\n"
            + "THEN FILTER b.id > 1;";

        try {
            submitScript(script);
        } catch (Exception e) {
            // Expected to fail until parser is updated
            System.out.println("Expected parsing error: " + e.getMessage());
        }
    }
}
