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

public class GQLParameterRequestTest {

    @Test
    public void testParameterRequest_001() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_parameter_request_001.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testParameterRequest_002() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_parameter_request_002.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testParameterRequest_003() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_parameter_request_003.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testIdOnlyParameterRequest_001() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_idonly_parameter_request_001.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testIdOnlyParameterRequest_002() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_idonly_parameter_request_002.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testIdOnlyParameterRequest_003() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_idonly_parameter_request_003.sql")
            .execute()
            .checkSinkResult();
    }
}
