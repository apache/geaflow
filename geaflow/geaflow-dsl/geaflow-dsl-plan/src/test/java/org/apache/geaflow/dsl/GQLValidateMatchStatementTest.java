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

public class GQLValidateMatchStatementTest {

    @Test
    public void testValidateMatchWhere1() {
        PlanTester.build()
            .gql("MATCH (a:user where id = 1)-[e:knows where e.weight > 0.4]->(b:user) RETURN a")
            .validate()
            .expectValidateType("RecordType(Vertex:RecordType:peek"
                + "(BIGINT id, VARCHAR ~label, VARCHAR name, INTEGER age) a)");
    }

    @Test
    public void testValidateMatchWhere2() {
        PlanTester.build()
            .gql("Match (a WHERE name = 'marko')<-[e]-(b) WHERE a.name <> b.name RETURN e")
            .validate()
            .expectValidateType("RecordType(Edge: RecordType:peek"
                + "(BIGINT src_id, BIGINT target_id, VARCHAR ~label, DOUBLE weight) e)");
    }

    @Test
    public void testValidateMatchWhere3() {
        PlanTester.build()
            .gql("Match (a:user WHERE name = 'where')-[e]-(b) RETURN b")
            .validate()
            .expectValidateType("RecordType(Vertex:RecordType:peek"
                + "(BIGINT id, VARCHAR ~label, VARCHAR name, INTEGER age) b)");
    }

    @Test
    public void testValidateMatchWhere4() {
        PlanTester.build()
            .gql("Match (a WHERE name = 'match')-[e]->(b) RETURN a.name")
            .validate()
            .expectValidateType("RecordType(VARCHAR name)");
    }

    @Test
    public void testValidateMatchWhere5() {
        PlanTester.build()
            .gql("Match (a WHERE name = 'knows')<-[e]->(b) RETURN e.weight")
            .validate()
            .expectValidateType("RecordType(DOUBLE weight)");
    }

    @Test
    public void testValidateMatch1() {
        PlanTester.build()
            .gql("MATCH (a)->(b) - (c) RETURN a, b")
            .validate()
            .expectValidateType("RecordType(Vertex:RecordType:peek"
                + "(BIGINT id, VARCHAR ~label, VARCHAR name, INTEGER age) a, "
                + "Vertex:RecordType:peek"
                + "(BIGINT id, VARCHAR ~label, VARCHAR name, INTEGER age) b)");
    }

    @Test
    public void testValidateMatch2() {
        PlanTester.build()
            .gql("MATCH (a)<-(b) <->(c) RETURN b, c, a")
            .validate()
            .expectValidateType("RecordType(Vertex:RecordType:peek"
                + "(BIGINT id, VARCHAR ~label, VARCHAR name, INTEGER age) b, "
                + "Vertex:RecordType:peek"
                + "(BIGINT id, VARCHAR ~label, VARCHAR name, INTEGER age) c, "
                + "Vertex:RecordType:peek"
                + "(BIGINT id, VARCHAR ~label, VARCHAR name, INTEGER age) a)");
    }

    @Test
    public void testValidateMatch3() {
        PlanTester.build()
            .gql("MATCH (e) -> (d) <- (f) RETURN d, e")
            .validate()
            .expectValidateType("RecordType(Vertex:RecordType:peek"
                + "(BIGINT id, VARCHAR ~label, VARCHAR name, INTEGER age) d, "
                + "Vertex:RecordType:peek"
                + "(BIGINT id, VARCHAR ~label, VARCHAR name, INTEGER age) e)");
    }

    @Test
    public void testValidateMatch4() {
        PlanTester.build()
            .gql("MATCH (e) -> (d) - (f) RETURN e, f, d")
            .validate()
            .expectValidateType("RecordType(Vertex:RecordType:peek"
                + "(BIGINT id, VARCHAR ~label, VARCHAR name, INTEGER age) e, "
                + "Vertex:RecordType:peek"
                + "(BIGINT id, VARCHAR ~label, VARCHAR name, INTEGER age) f, "
                + "Vertex:RecordType:peek"
                + "(BIGINT id, VARCHAR ~label, VARCHAR name, INTEGER age) d)");
    }

    @Test
    public void testValidateMatch5() {
        PlanTester.build()
            .gql("MATCH (n) RETURN n")
            .validate()
            .expectValidateType("RecordType(Vertex:RecordType:peek"
                + "(BIGINT id, VARCHAR ~label, VARCHAR name, INTEGER age) n)");
    }

    @Test
    public void testValidateMatch6() {
        PlanTester.build()
            .gql("   MATCH (n:user) RETURN n")
            .validate()
            .expectValidateType("RecordType(Vertex:RecordType:peek"
                + "(BIGINT id, VARCHAR ~label, VARCHAR name, INTEGER age) n)");
    }

    @Test
    public void testValidateMatch7() {
        PlanTester.build()
            .gql("MATCH (a) -[e]->(b) RETURN b    ")
            .validate()
            .expectValidateType("RecordType(Vertex:RecordType:peek"
                + "(BIGINT id, VARCHAR ~label, VARCHAR name, INTEGER age) b)");
    }

    @Test
    public void testValidateMatch8() {
        PlanTester.build()
            .gql("     MATCH (a) - (b) RETURN a")
            .validate()
            .expectValidateType("RecordType(Vertex:RecordType:peek"
                + "(BIGINT id, VARCHAR ~label, VARCHAR name, INTEGER age) a)");
    }

    @Test
    public void testValidateMatchWhere6() {
        PlanTester.build()
            .gql("MATCH (a:user WHERE a.age > 18) - (b: user) RETURN b")
            .validate()
            .expectValidateType("RecordType(Vertex:RecordType:peek"
                + "(BIGINT id, VARCHAR ~label, VARCHAR name, INTEGER age) b)");
    }

    @Test
    public void testVertexColumnNotExists() {
        PlanTester.build()
            .gql("MATCH (a:user where c = 1)->(b) - (c) RETURN c")
            .validate()
            .expectException("At line 1, column 21: Column 'c' not found in any table");
    }

    @Test
    public void testEdgeColumnNotExists() {
        PlanTester.build()
            .gql("MATCH (a:user where id = 1)-[e where id = 1]->(b) - (c) RETURN c")
            .validate()
            .expectException("From line 1, column 38 to line 1, column 39: "
                + "Column 'id' not found in any table");
    }

    @Test
    public void testVertexTypeNotExists() {
        PlanTester.build()
            .gql("MATCH (a:user|person where id = 1)->(b) - (c) RETURN c")
            .validate()
            .expectException("Cannot find vertex type: 'person'.");
    }

    @Test
    public void testEdgeTypeNotExists() {
        PlanTester.build()
            .gql("MATCH (a:user where id = 1)-[e:test]->(b) - (c) RETURN c")
            .validate()
            .expectException("Cannot find edge type: 'test'.");
    }

    @Test
    public void testVertexScopeNotExists() {
        PlanTester.build()
            .gql("MATCH (a:user where user.id = 1)->(b) - (c) RETURN c")
            .validate()
            .expectException("From line 1, column 21 to line 1, column 24: "
                + "Column 'user' not found in any table");
    }

    @Test
    public void testEdgeScopeNotExists() {
        PlanTester.build()
            .gql("MATCH (a:user where id = 1)-[e where knows.src_id = 1]->(b) - (c) RETURN c")
            .validate()
            .expectException("From line 1, column 38 to line 1, column 42: "
                + "Table 'knows' not found");
    }

    @Test
    public void testDuplicatedVariablesInMatchPattern() {
        PlanTester.build()
            .gql("MATCH (a where id = 1)-[e]->(b) -[e]- (a) RETURN c")
            .validate()
            .expectException("At line 1, column 37: Duplicated node label: e in the path pattern.");
    }

    @Test
    public void testVertexScopeExists() {
        String graphDDL = "create graph g1("
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
            + "),"
            + "edge follow("
            + " src_id bigint SOURCE ID,"
            + " target_id bigint DESTINATION ID,"
            + " time bigint TIMESTAMP,"
            + " weight double"
            + ")"
            + ")";
        PlanTester.build().registerGraph(graphDDL)
            .gql("MATCH (person:person|user WHERE person.id = 1)-[e:knows|follow]->(b:user)\n"
                + "RETURN person, e, b Order by person.id "
                + "DESC Limit 10")
            .validate()
            .expectValidateType("RecordType(Vertex:RecordType:peek"
                + "(BIGINT id, VARCHAR ~label, VARCHAR name, INTEGER gender, INTEGER age) person, "
                + "Edge: RecordType:peek"
                + "(BIGINT src_id, BIGINT target_id, VARCHAR ~label, BIGINT time, DOUBLE weight) e,"
                + " Vertex:RecordType:peek(BIGINT id, VARCHAR ~label, VARCHAR name) b)");
    }

}
