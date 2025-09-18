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

import org.apache.geaflow.common.config.keys.FrameworkConfigKeys;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.store.paimon.config.PaimonConfigKeys;
import org.testng.annotations.Test;

public class GQLInsertTest {

    @Test
    public void testInsertAndQuery_001() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/query/gql_insert_and_graph_001.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testInsertAndQuery_002() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/query/gql_insert_and_graph_002.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testInsertAndQuery_003() throws Exception {
        QueryTester
            .build()
            .withConfig(FrameworkConfigKeys.BATCH_NUMBER_PER_CHECKPOINT.getKey(), "2")
            .withQueryPath("/query/gql_insert_and_graph_003.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testInsertAndQuery_004() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/query/gql_insert_and_graph_004.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testInsertAndQueryWithRequest_001() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/query/gql_insert_and_query_with_request_001.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testInsertAndQueryWithRequest_002() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/query/gql_insert_and_query_with_request_002.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testInsertAndQueryWithRequest_003() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/query/gql_insert_and_query_with_request_003.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testInsertAndQueryWithSubQuery_001() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/query/gql_insert_and_query_with_subquery_001.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testInsertAndQueryWithSubQuery_002() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/query/gql_insert_and_query_with_subquery_002.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testInsertAndQuery_005() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/query/gql_insert_and_graph_005.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testInsertAndQuery_006() throws Exception {
        QueryTester
            .build()
            .withConfig(PaimonConfigKeys.PAIMON_STORE_TABLE_AUTO_CREATE_ENABLE.getKey(), "true")
            .withQueryPath("/query/gql_insert_and_graph_006.sql")
            .execute()
            .checkSinkResult();
    }

    @Test(expectedExceptions = GeaflowRuntimeException.class)
    public void testInsertAndQuery_007() throws Exception {
        QueryTester
            .build()
            .withConfig(PaimonConfigKeys.PAIMON_STORE_TABLE_AUTO_CREATE_ENABLE.getKey(), "true")
            .withQueryPath("/query/gql_insert_and_graph_007.sql")
            .execute()
            .checkSinkResult();
    }
}
