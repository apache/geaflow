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

public class GQLValidateFilterStatementTest {

    @Test
    public void testSimpleFilter() {
        PlanTester.build()
            .gql("MATCH (a:user WHERE a.id = '1')-[e:knows]->(b:user)\n"
                + "RETURN b.name, b as _b\n" + "THEN\n" + "FILTER name IS NOT NULL AND _b.id "
                + "> 10")
            .validate()
            .expectValidateType("RecordType(VARCHAR name, Vertex:RecordType:peek(BIGINT id, VARCHAR ~label, VARCHAR name, INTEGER age) _b)");
    }

    @Test
    public void testNotUseAlias() {
        PlanTester.build()
            .gql("MATCH (a:user WHERE a.id = '1')-[e:knows]->(b:user)\n"
                + "RETURN b.name, b as _b\n" + "THEN\n" + "FILTER b.name IS NOT NULL AND _b.id "
                + "> 10")
            .validate()
            .expectException("At line 4, column 8: Table 'b' not found");
    }
}
