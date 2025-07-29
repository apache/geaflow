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

package org.apache.geaflow.dsl;

import org.testng.annotations.Test;

public class GQLValidateReturnStatementTest {
    private static final String graphDDL = "create graph g1("
        + "vertex user("
        + " id bigint ID,"
        + "name varchar"
        + "),"
        + "vertex person("
        + " id bigint ID,"
        + "name varchar,"
        + "gender int,"
        + "age integer"
        + "),"
        + "edge knows("
        + " src_id bigint SOURCE ID,"
        + " target_id bigint DESTINATION ID,"
        + " time bigint TIMESTAMP,"
        + " weight double"
        + ")"
        + ")";

    @Test
    public void testValidatedReturnVertex() {
        String script = "MATCH (a:user where id = 1)-[e:knows where e.weight > 0.4]->(b:user) RETURN a";
        String expectType = "RecordType("
            + "Vertex:RecordType:peek(BIGINT id, VARCHAR ~label, VARCHAR name) a)";
        PlanTester.build().registerGraph(graphDDL)
            .gql(script)
            .validate()
            .expectValidateType(expectType);
    }

    @Test
    public void testValidatedReturnEdge() {
        String script = "Match (a:user WHERE name = 'marko')<-[e:knows]-(b:person) "
            + "WHERE a.name <> b.name RETURN e";
        String expectType = "RecordType(Edge: RecordType:peek(BIGINT src_id, BIGINT target_id, VARCHAR ~label, BIGINT time, "
            + "DOUBLE weight) e)";
        PlanTester.build().registerGraph(graphDDL)
            .gql(script)
            .validate()
            .expectValidateType(expectType);
    }

    @Test
    public void testValidatedReturnVertex2() {
        String script = "MATCH (a:person WHERE a.age > 18) - (b: person) RETURN b";
        String expectType = "RecordType("
            + "Vertex:RecordType:peek(BIGINT id, VARCHAR ~label, VARCHAR name, INTEGER gender, INTEGER age) b)";
        PlanTester.build().registerGraph(graphDDL)
            .gql(script)
            .validate()
            .expectValidateType(expectType);
    }

    @Test
    public void testValidatedReturnFields() {
        String script = "MATCH (a:person WHERE a.id = '1')-[e:knows]->(b:person) "
            + "RETURN a.name as name, b.id as b_id";
        String expectType = "RecordType(VARCHAR name, BIGINT b_id)";
        PlanTester.build().registerGraph(graphDDL)
            .gql(script)
            .validate()
            .expectValidateType(expectType);
    }

    @Test
    public void testValidatedReturnMultiplication() {
        String script = "MATCH (a:person WHERE a.id = '1')-[e:knows]->(b:person) "
            + "RETURN a.name as name, b.id as b_id, b.age * 10 as amt";
        String expectType = "RecordType(VARCHAR name, BIGINT b_id, JavaType(class java.lang.Long) amt)";
        PlanTester.build().registerGraph(graphDDL)
            .gql(script)
            .validate()
            .expectValidateType(expectType);
    }

    @Test
    public void testValidatedReturnSum() {
        String script = "MATCH (a:person)-[e:knows where e.weight > 0.4]->(b:person) "
            + "RETURN a.id as a_id, SUM(ALL e.weight) * 10 as amt GROUP BY a_id";
        String expectType = "RecordType(BIGINT a_id, JavaType(class java.lang.Double) amt)";
        PlanTester.build().registerGraph(graphDDL)
            .gql(script)
            .validate()
            .expectValidateType(expectType);
    }

    @Test
    public void testValidatedReturnGroupByAlias() {
        String script = "MATCH (a:person)-[e:knows]->(b:person) "
            + "RETURN b.name as name, SUM(ALL b.age) as amt group by name";
        String expectType = "RecordType(VARCHAR name, INTEGER amt)";
        PlanTester.build().registerGraph(graphDDL)
            .gql(script)
            .validate()
            .expectValidateType(expectType);
    }

    @Test
    public void testValidatedReturnGroupByAlias2() {
        String script = "MATCH (a:person)-[e:knows]->(b:person) "
            + "RETURN a.id as a_id, b.id as b_id, SUM(ALL e.weight) as e_sum GROUP BY a_id, b_id";
        String expectType = "RecordType(BIGINT a_id, BIGINT b_id, DOUBLE e_sum)";
        PlanTester.build().registerGraph(graphDDL)
            .gql(script)
            .validate()
            .expectValidateType(expectType);
    }

    @Test
    public void testValidatedReturnOrderByAlias() {
        String script = "MATCH (a:person WHERE a.id = '1')-[e:knows]->(b:person) "
            + "RETURN b.name as name, b.id as _id, b.age as age order by age DESC";
        String expectType = "RecordType(VARCHAR name, BIGINT _id, INTEGER age)";
        PlanTester.build().registerGraph(graphDDL)
            .gql(script)
            .validate()
            .expectValidateType(expectType);
    }

    @Test
    public void testValidatedReturnOrderByAlias2() {
        String script = "MATCH (a:person WHERE a.id = '1')-[e:knows]->(b:person) "
            + "RETURN b.name as name, b.id as _id, b.age as age order by age DESC Limit 10";
        String expectType = "RecordType(VARCHAR name, BIGINT _id, INTEGER age)";
        PlanTester.build().registerGraph(graphDDL)
            .gql(script)
            .validate()
            .expectValidateType(expectType);
    }

    @Test
    public void testValidatedReturnCast() {
        String script = "MATCH (a:person WHERE a.id = '1')-[e:knows]->(b:person) "
            + "RETURN b.name as name, cast(b.id as int) as _id";
        String expectType = "RecordType(VARCHAR name, INTEGER _id)";
        PlanTester.build().registerGraph(graphDDL)
            .gql(script)
            .validate()
            .expectValidateType(expectType);
    }

    @Test
    public void testValidatedReturnCase() {
        String script = "MATCH (a:person WHERE a.id = '1')-[e:knows]->(b:person) "
            + "RETURN b.name as name, case when b.gender = 0 then '0' else '1' end as _id";
        String expectType = "RecordType(VARCHAR name, CHAR(1) _id)";
        PlanTester.build().registerGraph(graphDDL)
            .gql(script)
            .validate()
            .expectValidateType(expectType);
    }

    @Test
    public void testValidatedReturnFields2() {
        String script = "Match (a1:user WHERE name like 'marko')-[e1]->(b1:person) "
            + "return b1.name AS b_id";
        String expectType = "RecordType(VARCHAR b_id)";
        PlanTester.build().registerGraph(graphDDL)
            .gql(script)
            .validate()
            .expectValidateType(expectType);
    }

    @Test
    public void testValidatedReturnFields3() {
        String script = "Match (a:user)-[e]->(b:person WHERE name = 'lop') return a.id";
        String expectType = "RecordType(BIGINT id)";
        PlanTester.build().registerGraph(graphDDL)
            .gql(script)
            .validate()
            .expectValidateType(expectType);
    }

    @Test
    public void testValidatedReturnGroupByAlias3() {
        String script = "MATCH (a:user)-[e:knows]->(b:user)\n"
            + "RETURN a as _a, b.id as b_id, e.weight as e_weight GROUP BY a, b_id, e_weight";
        String expectType = "RecordType("
            + "Vertex:RecordType:peek(BIGINT id, VARCHAR ~label, VARCHAR name) _a, BIGINT b_id, DOUBLE e_weight)";
        PlanTester.build().registerGraph(graphDDL)
            .gql(script)
            .validate()
            .expectValidateType(expectType);
    }

    @Test
    public void testValidatedReturnOrderBy() {
        String script = "MATCH (a:user WHERE a.id = 1)-[e:knows]->(b:user)\n"
            + "RETURN a as _a, b.id as b_id, weight Order by _a.id DESC Limit 10";
        String expectType = "RecordType("
            + "Vertex:RecordType:peek(BIGINT id, VARCHAR ~label, VARCHAR name) _a, BIGINT b_id, DOUBLE weight)";
        PlanTester.build().registerGraph(graphDDL)
            .gql(script)
            .validate()
            .expectValidateType(expectType);
    }

    @Test
    public void testReturnAggregate1() {
        String script = "MATCH (a:person)-[e:knows]->(b:person) "
            + "RETURN COUNT(a.name) as a_name, SUM(e.weight) as e_weight, "
            + "MAX(b.age) + 1 as b_age_max, MIN(b.age) - 1 as b_age_min, "
            + "AVG(b.age) as b_age_avg, b as _b "
            + "group by _b order by _b";
        String expectType = "RecordType(BIGINT a_name, DOUBLE e_weight, "
            + "JavaType(class java.lang.Integer) b_age_max, "
            + "JavaType(class java.lang.Integer) b_age_min, DOUBLE b_age_avg, "
            + "Vertex:RecordType:peek(BIGINT id, VARCHAR ~label, VARCHAR name, INTEGER gender, INTEGER age) _b)";
        PlanTester.build().registerGraph(graphDDL)
            .gql(script)
            .validate()
            .expectValidateType(expectType);
    }

    @Test
    public void testDuplicatedAlias() {
        PlanTester.build().registerGraph(graphDDL)
            .gql("MATCH (a:user WHERE a.id = 1)-[e:knows]->(b:user)\n"
                + "RETURN a as _a, b.id as _a, weight as e_weight Order by a.id "
                + "DESC Limit 10")
            .validate()
            .expectException("Duplicated alias in an Return statement at line 2, column 15");
    }

    @Test
    public void testAmbiguousColumn() {
        PlanTester.build().registerGraph(graphDDL)
            .gql("MATCH (a:user WHERE a.id = 1)-[e1:knows]->(o:person)-[e2]->(b:user)\n"
                + "RETURN a as _a, b.id as b_id, weight Order by _a.id "
                + "DESC Limit 10")
            .validate()
            .expectException("From line 2, column 31 to line 2, column 36: Column 'weight' is ambiguous");
    }

    @Test
    public void testReturnVertexColumnNotExists() {
        PlanTester.build().registerGraph(graphDDL)
            .gql("MATCH (a:user WHERE a.id = 1)-[e:knows]->(b:user)\n"
                + "RETURN a as _a, b.test as b_test, weight as e_weight Order by a.id "
                + "DESC Limit 10")
            .validate()
            .expectException("From line 2, column 17 to line 2, column 22: Column 'b.test' not found in table 'col_1'");
    }

    @Test
    public void testReturnEdgeColumnNotExists() {
        PlanTester.build().registerGraph(graphDDL)
            .gql("MATCH (a:user WHERE a.id = 1)-[e:knows]->(b:user)\n"
                + "RETURN a as _a, e.test as e_test, weight as e_weight Order by a.id "
                + "DESC Limit 10")
            .validate()
            .expectException("From line 2, column 17 to line 2, column 22: Column 'e.test' not found in table 'col_1'");
    }

    @Test
    public void testVertexScopeNotExists() {
        PlanTester.build().registerGraph(graphDDL)
            .gql("MATCH (a:user WHERE a.id = 1)-[e:knows]->(b:user)\n"
                + "RETURN a as _a, user.name as user_name, weight as e_weight Order by a.id "
                + "DESC Limit 10")
            .validate()
            .expectException("From line 2, column 17 to line 2, column 20: Column 'user' not found in any table");
    }

    @Test
    public void testEdgeScopeNotExists() {
        PlanTester.build().registerGraph(graphDDL)
            .gql("MATCH (a:user WHERE a.id = 1)-[e:knows]->(b:user)\n"
                + "RETURN a as _a, knows.weight as weight, weight as e_weight Order by a.id "
                + "DESC Limit 10")
            .validate()
            .expectException("From line 2, column 17 to line 2, column 21: Table 'knows' not found");
    }

    @Test
    public void testExpressionNotBeingGrouped1() {
        PlanTester.build().registerGraph(graphDDL)
            .gql("MATCH (a:person)-[e:knows where e.weight > 0.4]->(b:person) "
                + "RETURN a.id as a_id, e.weight as amt GROUP BY amt ORDER BY a_id")
            .validate()
            .expectException("From line 1, column 68 to line 1, column 71: Expression 'a.id' is not being grouped");
    }

    @Test
    public void testExpressionNotBeingGrouped2() {
        PlanTester.build().registerGraph(graphDDL)
            .gql("MATCH (a:person)-[e:knows]->(b:person) "
                + "RETURN a as _a, e as _e, b as _b group by _a.id, _e.time, _b.id")
            .validate()
            .expectException("At line 1, column 47: Expression 'a' is not being grouped");
    }
}
