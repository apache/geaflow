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

import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.apache.geaflow.common.config.keys.DSLConfigKeys;
import org.apache.geaflow.common.config.keys.FrameworkConfigKeys;
import org.apache.geaflow.file.FileConfigKeys;
import org.testng.annotations.Test;

public class GQLAlgorithmTest {

    private final String TEST_GRAPH_PATH = "/tmp/geaflow/dsl/algorithm/test/graph";

    @Test
    public void testAlgorithm_001() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_algorithm_001.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testAlgorithm_002() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_algorithm_002.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testAlgorithm_003() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_algorithm_003.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testAlgorithm_004() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/query/gql_algorithm_004.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testAlgorithm_005() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/query/gql_algorithm_005.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testAlgorithm_006() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/query/gql_algorithm_006.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testAlgorithm_008() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/query/find_loop.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testAlgorithmKHop() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/query/gql_algorithm_007.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testAlgorithmKCore() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/query/gql_algorithm_kcore.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testAlgorithmClosenessCentrality() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/query/gql_algorithm_closeness_centrality.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testAlgorithmWeakConnectedComponents() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/query/gql_algorithm_wcc.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testAlgorithmTriangleCount() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_algorithm_tc.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testIncGraphAlgorithm_001() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/query/gql_algorithm_inc_001.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testIncGraphAlgorithm_002() throws Exception {
        clearGraph();
        QueryTester
            .build()
            .withConfig(FileConfigKeys.ROOT.getKey(), TEST_GRAPH_PATH)
            .withConfig(FrameworkConfigKeys.BATCH_NUMBER_PER_CHECKPOINT.getKey(), 1)
            .withQueryPath("/query/gql_using_001_ddl.sql")
            .execute()
            .withConfig(DSLConfigKeys.GEAFLOW_DSL_WINDOW_SIZE.getKey(), 1)
            .withQueryPath("/query/gql_algorithm_inc_002.sql")
            .execute()
            .checkSinkResult();
        clearGraph();
    }

    @Test
    public void testIncGraphAlgorithm_003() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/query/gql_algorithm_inc_003.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testIncGraphAlgorithm_004() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/query/gql_algorithm_inc_004.sql")
            .execute()
            .compareWithOrder()
            .checkSinkResult();
    }

    @Test
    public void testIncGraphAlgorithm_assp() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/query/gql_algorithm_inc_assp.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testIncWccVsSpark() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/query/gql_test_demo_case_vs_spark.sql")
            .execute()
            .checkSinkResult();
    }

    public void testIncGraphAlgorithm_005() throws Exception {
        QueryTester
            .build()
            .withWorkerNum(20)
            .withDedupe(true)
            .withQueryPath("/query/gql_algorithm_inc_005.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testIncGraphAlgorithm_006() throws Exception {
        QueryTester
            .build()
            .withWorkerNum(20)
            .withDedupe(true)
            .withQueryPath("/query/gql_algorithm_inc_006.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testIncGraphAlgorithm_007() throws Exception {
        QueryTester
            .build()
            .withWorkerNum(20)
            .withDedupe(true)
            .withQueryPath("/query/gql_algorithm_inc_007.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testIncGraphAlgorithm_008() throws Exception {
        QueryTester
            .build()
            .withWorkerNum(20)
            .withDedupe(true)
            .withQueryPath("/query/gql_algorithm_inc_008.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testAlgorithmCommonNeighbors() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_algorithm_common_neighbors.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testEdgeIterator() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/query/gql_edge_iterator_test.sql")
            .execute()
            .checkSinkResult();
    }

    private void clearGraph() throws IOException {
        File file = new File(TEST_GRAPH_PATH);
        if (file.exists()) {
            FileUtils.deleteDirectory(file);
        }
    }
}
