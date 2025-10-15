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

public class GQLValidateLoopMatchTest {

    @Test
    public void testValidateLoopMatch() {
        PlanTester.build()
            .gql("MATCH (a:user where id = 1)-[e:knows where e.weight > 0.4]->{1, 5} (b:user)\n"
                + " RETURN a")
            .validate()
            .expectValidateType(
                "RecordType(Vertex:RecordType:peek(BIGINT id, VARCHAR ~label, VARCHAR name, INTEGER age) a)");

        PlanTester.build()
            .gql("MATCH (a:user where id = 1)-[e:knows where e.weight > 0.4]->{1, } (b:user)\n"
                + " RETURN a, b")
            .validate()
            .expectValidateType(
                "RecordType(Vertex:RecordType:peek(BIGINT id, VARCHAR ~label, VARCHAR name, INTEGER age) a,"
                    + " Vertex:RecordType:peek(BIGINT id, VARCHAR ~label, VARCHAR name, INTEGER age) b)");
    }

    @Test
    public void testValidateLoopMatchException() {
        PlanTester.build()
            .gql("MATCH (a:user where id = 1)-[e:knows where e.weight > 0.4]->{1, 0} (b:user)\n"
                + " RETURN e")
            .validate()
            .expectException("At line 1, column 32: The max hop: 0 count should greater than min hop: 1.");
    }
}
