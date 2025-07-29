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

public class GQLValidateComplexMatchTest {

    @Test
    public void testMultiMatch() {
        String script = "Match(a)-(b), (b) - (c)"
            + "RETURN a.id, b, c";

        PlanTester.build()
            .gql(script)
            .validate()
            .expectValidateType(
                "RecordType(BIGINT id, "
                    + "Vertex:RecordType:peek(BIGINT id, VARCHAR ~label, VARCHAR name, INTEGER age) b, "
                    + "Vertex:RecordType:peek(BIGINT id, VARCHAR ~label, VARCHAR name, INTEGER age) c)");
    }

    @Test
    public void testMultiMatchWithLet() {
        String script = "Match(a)-(b), (b) - (c)"
            + "Let a.weight = a.age / 10,"
            + "Let b.weight = b.age / 10"
            + "RETURN a, b, c.id";

        PlanTester.build()
            .gql(script)
            .validate()
            .expectValidateType(
                "RecordType(Vertex:RecordType:peek(BIGINT id, VARCHAR ~label, VARCHAR name, INTEGER age, "
                    + "JavaType(class java.lang.Integer) weight) a, "
                    + "Vertex:RecordType:peek(BIGINT id, VARCHAR ~label, VARCHAR name, INTEGER age, "
                    + "JavaType(class java.lang.Integer) weight) b, "
                    + "BIGINT id)");
    }

    @Test
    public void testUnion() {
        String script = "Match(a) - (b) |+| (c) - (d) |+| (e) - (f)"
            + "RETURN a.id, b, c, d, e, f";

        PlanTester.build()
            .gql(script)
            .validate()
            .expectValidateType(
                "RecordType(BIGINT id, "
                    + "Vertex:RecordType:peek(BIGINT id, VARCHAR ~label, VARCHAR name, INTEGER age) b, "
                    + "Vertex:RecordType:peek(BIGINT id, VARCHAR ~label, VARCHAR name, INTEGER age) c, "
                    + "Vertex:RecordType:peek(BIGINT id, VARCHAR ~label, VARCHAR name, INTEGER age) d, "
                    + "Vertex:RecordType:peek(BIGINT id, VARCHAR ~label, VARCHAR name, INTEGER age) e, "
                    + "Vertex:RecordType:peek(BIGINT id, VARCHAR ~label, VARCHAR name, INTEGER age) f)"
            );
    }

    @Test
    public void testUnionWithDuplicatedAlias() {
        String script = "Match(a) - (b) |+| (a) - (c) |+| (e) - (f)"
            + "RETURN a.id, a, b, e, f";

        PlanTester.build()
            .gql(script)
            .validate()
            .expectValidateType(
                "RecordType(BIGINT id, "
                    + "Vertex:RecordType:peek(BIGINT id, VARCHAR ~label, VARCHAR name, INTEGER age) a, "
                    + "Vertex:RecordType:peek(BIGINT id, VARCHAR ~label, VARCHAR name, INTEGER age) b, "
                    + "Vertex:RecordType:peek(BIGINT id, VARCHAR ~label, VARCHAR name, INTEGER age) e, "
                    + "Vertex:RecordType:peek(BIGINT id, VARCHAR ~label, VARCHAR name, INTEGER age) f)"
            );
    }

    @Test
    public void testUnionWithLet() {
        String script = "MATCH (a:user where a.id = 1) -[e1:knows]->(b1:user) |+| (a:user where a.id = 2) -[e2:knows]->(b2:user)\n"
            + "LET a.weight = a.age / cast(100.0 as double),\n" + "LET a.weight = a.weight * 2,\n"
            + "LET e1.weight = e1.weight * 10\n" + "MATCH(b2) -[]->(c1) | (b2) <-[]-(c2)\n"
            + "RETURN a.weight AS a_weight, b1.id AS b1_id, c1.id AS c1_id, b2.id AS b2_id, c2.id "
            + "AS c2_id";

        PlanTester.build()
            .gql(script)
            .validate()
            .expectValidateType(
                "RecordType(JavaType(class java.lang.Double) a_weight, "
                    + "BIGINT b1_id, BIGINT c1_id, BIGINT b2_id, BIGINT c2_id)"
            );
    }
}
