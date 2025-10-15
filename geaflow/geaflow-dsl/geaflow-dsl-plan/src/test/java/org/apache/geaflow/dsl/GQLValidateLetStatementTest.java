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

public class GQLValidateLetStatementTest {

    @Test
    public void testLet_001() {
        PlanTester.build()
            .gql("Match(a: user)"
                + "Let a.cnt = a.age / 2")
            .validate()
            .expectValidateType("Path:RecordType:peek(Vertex:RecordType:peek(BIGINT id, VARCHAR ~label, "
                + "VARCHAR name, INTEGER age, JavaType(class java.lang.Integer) cnt) a)");
    }

    @Test
    public void testLet_002() {
        PlanTester.build()
            .gql("Match(a: user) -[e]->(b)"
                + "Let a.cnt = a.age / 2,"
                + "Let e.ratio = e.weight * 100")
            .validate()
            .expectValidateType("Path:RecordType:peek(Vertex:RecordType:peek(BIGINT id, VARCHAR ~label, "
                + "VARCHAR name, INTEGER age, JavaType(class java.lang.Integer) cnt) a, "
                + "Edge: RecordType:peek(BIGINT src_id, BIGINT target_id, VARCHAR ~label, "
                + "DOUBLE weight, JavaType(class java.lang.Double) ratio) e, "
                + "Vertex:RecordType:peek(BIGINT id, VARCHAR ~label, VARCHAR name, INTEGER age) b)");
    }
}
