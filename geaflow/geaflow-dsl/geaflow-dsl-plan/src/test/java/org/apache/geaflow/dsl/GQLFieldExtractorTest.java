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

import org.apache.geaflow.dsl.optimize.rule.GraphMatchFieldPruneRule;
import org.apache.geaflow.dsl.optimize.rule.ProjectFieldPruneRule;
import org.testng.annotations.Test;

public class GQLFieldExtractorTest {

    private static final String GRAPH_G1 = "create graph g1("
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
    public void testGraphMatchFieldPrune() {
        PlanTester.build()
            .registerGraph(GRAPH_G1)
            .gql("MATCH (a:person WHERE a.age > 18)" +
                "-[e:knows WHERE e.weight > 0.5]" +
                "->(b:user WHERE b.id != 0 AND name like 'MARKO')\n")
            .toRel()
            .checkFilteredFields("{a=[null], b=[null], e=[null]}")
            .opt(GraphMatchFieldPruneRule.INSTANCE)
            .checkFilteredFields("{a=[a.age], b=[b.id, b.name], e=[e.weight]}");
    }

    @Test
    public void testProjectFieldPrune() {
        PlanTester.build()
            .registerGraph(GRAPH_G1)
            .gql("MATCH (a:person)-[e:knows]->(b:user)\n" +
                " RETURN e.src_id as src_id, e.target_id as target_id," +
                " a.gender as a_gender, b.id as b_id")
            .toRel()
            .checkFilteredFields("{a=[null], b=[null], e=[null]}")
            .opt(ProjectFieldPruneRule.INSTANCE)
            .checkFilteredFields("{a=[a.gender], b=[b.id], e=[e.src_id, e.target_id]}");
    }

}
