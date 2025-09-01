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

public class GQLValidateGraphRecordTypeTest {

    @Test
    public void testDifferentVertexFieldType() {
        String graphDDL = "create graph g1("
            + "vertex user("
            + " id bigint ID,"
            + "name varchar"
            + "),"
            + "vertex person("
            + " id bigint ID,"
            + "name bigint,"
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
            .gql("MATCH (a:user|person WHERE a.id = 1)-[e:knows]->(b:user)\n"
                + "RETURN a, e, b Order by a.id "
                + "DESC Limit 10")
            .validate()
            .expectException("Same name field between vertex tables shouldn't have different type.");
    }

    @Test
    public void testDifferentEdgeFieldType() {
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
            + " weight int"
            + ")"
            + ")";
        PlanTester.build().registerGraph(graphDDL)
            .gql("MATCH (a:user|person WHERE a.id = 1)-[e:knows|follow]->(b:user)\n"
                + "RETURN a, e, b Order by a.id "
                + "DESC Limit 10")
            .validate()
            .expectException("Same name field between edge tables shouldn't have different type.");
    }

    @Test(enabled = false)
    public void testSameIdFieldNameValidation() {
        String graphDDL = "create graph g1("
            + "vertex user("
            + " id bigint ID,"
            + "name varchar"
            + "),"
            + "vertex person("
            + " _id bigint ID,"
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
            .expectException("Id field name should be same between vertex tables");
    }

    @Test(enabled = false)
    public void testSameSourceIdFieldNameValidation() {
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
            + " _src_id bigint SOURCE ID,"
            + " _target_id bigint DESTINATION ID,"
            + " _time bigint TIMESTAMP,"
            + " weight double"
            + ")"
            + ")";
        PlanTester.build().registerGraph(graphDDL)
            .expectException("SOURCE ID field name should be same between edge tables");
    }

    @Test(enabled = false)
    public void testSameDestinationIdFieldNameValidation() {
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
            + " _target_id bigint DESTINATION ID,"
            + " _time bigint TIMESTAMP,"
            + " weight double"
            + ")"
            + ")";

        PlanTester.build().registerGraph(graphDDL)
            .expectException("DESTINATION ID field name should be same between edge tables");
    }


    @Test(enabled = false)
    public void testSameTimestampFieldNameValidation() {
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
            + " _time bigint TIMESTAMP,"
            + " weight double"
            + ")"
            + ")";
        PlanTester.build().registerGraph(graphDDL)
            .expectException("TIMESTAMP field name should be same between edge tables");
    }
}
