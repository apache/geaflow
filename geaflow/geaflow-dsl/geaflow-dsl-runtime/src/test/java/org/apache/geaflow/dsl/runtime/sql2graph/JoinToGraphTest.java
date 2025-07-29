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

package org.apache.geaflow.dsl.runtime.sql2graph;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.geaflow.common.config.keys.DSLConfigKeys;
import org.apache.geaflow.dsl.runtime.query.QueryTester;
import org.apache.geaflow.file.FileConfigKeys;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class JoinToGraphTest {

    private final String TEST_GRAPH_PATH = "/tmp/geaflow/dsl/join2Graph/test/graph";

    private final Map<String, String> testConfig = new HashMap<String, String>() {
        {
            put(FileConfigKeys.PERSISTENT_TYPE.getKey(), "DFS");
            put(FileConfigKeys.ROOT.getKey(), TEST_GRAPH_PATH);
            put(FileConfigKeys.JSON_CONFIG.getKey(), "{\"fs.defaultFS\":\"local\"}");
            // If the test is conducted using the console catalog, the appended config is required.
            // put(DSLConfigKeys.GEAFLOW_DSL_CATALOG_TYPE.getKey(), "console");
            // put(DSLConfigKeys.GEAFLOW_DSL_CATALOG_TOKEN_KEY.getKey(), "");
            // put(DSLConfigKeys.GEAFLOW_DSL_CATALOG_INSTANCE_NAME.getKey(), "test1");
            // put(ExecutionConfigKeys.GEAFLOW_GW_ENDPOINT.getKey(), "http://127.0.0.1:8888");
        }
    };

    @BeforeClass
    public void prepare() throws Exception {
        File file = new File(TEST_GRAPH_PATH);
        if (file.exists()) {
            FileUtils.deleteDirectory(file);
        }
        QueryTester
            .build()
            .withConfig(testConfig)
            .withConfig(DSLConfigKeys.GEAFLOW_DSL_WINDOW_SIZE.getKey(), "1")
            .withConfig(FileConfigKeys.PERSISTENT_TYPE.getKey(), "DFS")
            .withConfig(FileConfigKeys.ROOT.getKey(), TEST_GRAPH_PATH)
            .withConfig(FileConfigKeys.JSON_CONFIG.getKey(), "{\"fs.defaultFS\":\"local\"}")
            .withQueryPath("/sql2graph/graph_student_v_insert.sql").execute()
            .withQueryPath("/sql2graph/graph_student_e_insert.sql").execute();
    }

    @AfterClass
    public void tearDown() throws Exception {
        File file = new File(TEST_GRAPH_PATH);
        if (file.exists()) {
            FileUtils.deleteDirectory(file);
        }
    }

    @Test
    public void testVertexJoinEdge_001() throws Exception {
        QueryTester
            .build()
            .withConfig(testConfig)
            .withGraphDefine("/sql2graph/graph_student.sql")
            .withQueryPath("/sql2graph/vertex_join_edge_001.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testVertexJoinEdge_002() throws Exception {
        QueryTester
            .build()
            .withConfig(testConfig)
            .withGraphDefine("/sql2graph/graph_student.sql")
            .withQueryPath("/sql2graph/vertex_join_edge_002.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testEdgeJoinVertex_001() throws Exception {
        QueryTester
            .build()
            .withConfig(testConfig)
            .withGraphDefine("/sql2graph/graph_student.sql")
            .withQueryPath("/sql2graph/edge_join_vertex_001.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testEdgeJoinVertex_002() throws Exception {
        QueryTester
            .build()
            .withConfig(testConfig)
            .withGraphDefine("/sql2graph/graph_student.sql")
            .withQueryPath("/sql2graph/edge_join_vertex_002.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testMatchJoinVertex_001() throws Exception {
        QueryTester
            .build()
            .withConfig(testConfig)
            .withGraphDefine("/sql2graph/graph_student.sql")
            .withQueryPath("/sql2graph/match_join_vertex_001.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testMatchJoinVertex_002() throws Exception {
        QueryTester
            .build()
            .withConfig(testConfig)
            .withGraphDefine("/sql2graph/graph_student.sql")
            .withQueryPath("/sql2graph/match_join_vertex_002.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testMatchJoinVertex_003() throws Exception {
        QueryTester
            .build()
            .withConfig(testConfig)
            .withGraphDefine("/sql2graph/graph_student.sql")
            .withQueryPath("/sql2graph/match_join_vertex_003.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testMatchJoinEdge_001() throws Exception {
        QueryTester
            .build()
            .withConfig(testConfig)
            .withGraphDefine("/sql2graph/graph_student.sql")
            .withQueryPath("/sql2graph/match_join_edge_001.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testMatchJoinEdge_002() throws Exception {
        QueryTester
            .build()
            .withConfig(testConfig)
            .withGraphDefine("/sql2graph/graph_student.sql")
            .withQueryPath("/sql2graph/match_join_edge_002.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testMatchJoinEdge_003() throws Exception {
        QueryTester
            .build()
            .withConfig(testConfig)
            .withGraphDefine("/sql2graph/graph_student.sql")
            .withQueryPath("/sql2graph/match_join_edge_003.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testMatchJoinEdge_004() throws Exception {
        QueryTester
            .build()
            .withConfig(testConfig)
            .withGraphDefine("/sql2graph/graph_student.sql")
            .withQueryPath("/sql2graph/match_join_edge_004.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testMatchJoinEdge_005() throws Exception {
        QueryTester
            .build()
            .withConfig(testConfig)
            .withGraphDefine("/sql2graph/graph_student.sql")
            .withQueryPath("/sql2graph/match_join_edge_005.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testJoinToMatch_001() throws Exception {
        QueryTester
            .build()
            .withConfig(testConfig)
            .withGraphDefine("/sql2graph/graph_student.sql")
            .withQueryPath("/sql2graph/join_to_match_001.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testJoinToMatch_002() throws Exception {
        QueryTester
            .build()
            .withConfig(testConfig)
            .withGraphDefine("/sql2graph/graph_student.sql")
            .withQueryPath("/sql2graph/join_to_match_002.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testJoinToMatch_003() throws Exception {
        QueryTester
            .build()
            .withConfig(testConfig)
            .withGraphDefine("/sql2graph/graph_student.sql")
            .withQueryPath("/sql2graph/join_to_match_003.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testJoinToMatch_004() throws Exception {
        QueryTester
            .build()
            .withConfig(testConfig)
            .withGraphDefine("/sql2graph/graph_student.sql")
            .withQueryPath("/sql2graph/join_to_match_004.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testJoinToMatch_005() throws Exception {
        QueryTester
            .build()
            .withConfig(testConfig)
            .withGraphDefine("/sql2graph/graph_student.sql")
            .withQueryPath("/sql2graph/join_to_match_005.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testJoinToMatch_006() throws Exception {
        QueryTester
            .build()
            .withConfig(testConfig)
            .withGraphDefine("/sql2graph/graph_student.sql")
            .withQueryPath("/sql2graph/join_to_match_006.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testJoinToMatch_007() throws Exception {
        QueryTester
            .build()
            .withConfig(testConfig)
            .withGraphDefine("/sql2graph/graph_student.sql")
            .withQueryPath("/sql2graph/join_to_match_007.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testJoinToMatch_008() throws Exception {
        QueryTester
            .build()
            .withConfig(testConfig)
            .withGraphDefine("/sql2graph/graph_student.sql")
            .withQueryPath("/sql2graph/join_to_match_008.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testJoinToMatch_009() throws Exception {
        QueryTester
            .build()
            .withConfig(testConfig)
            .withGraphDefine("/sql2graph/graph_student.sql")
            .withQueryPath("/sql2graph/join_to_match_009.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testJoinToMatch_010() throws Exception {
        QueryTester
            .build()
            .withConfig(testConfig)
            .withGraphDefine("/sql2graph/graph_student.sql")
            .withQueryPath("/sql2graph/join_to_match_010.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testJoinToMatch_011() throws Exception {
        QueryTester
            .build()
            .withConfig(testConfig)
            .withGraphDefine("/sql2graph/graph_student.sql")
            .withQueryPath("/sql2graph/join_to_match_011.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testAggregateToMatch_001() throws Exception {
        QueryTester
            .build()
            .withConfig(testConfig)
            .withGraphDefine("/sql2graph/graph_student.sql")
            .withQueryPath("/sql2graph/aggregate_to_match_001.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testAggregateToMatch_002() throws Exception {
        QueryTester
            .build()
            .withConfig(testConfig)
            .withGraphDefine("/sql2graph/graph_student.sql")
            .withQueryPath("/sql2graph/aggregate_to_match_002.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testAggregateToMatch_003() throws Exception {
        QueryTester
            .build()
            .withConfig(testConfig)
            .withGraphDefine("/sql2graph/graph_student.sql")
            .withQueryPath("/sql2graph/aggregate_to_match_003.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testLeftJoin_001() throws Exception {
        QueryTester
            .build()
            .withConfig(testConfig)
            .withGraphDefine("/sql2graph/graph_student.sql")
            .withQueryPath("/sql2graph/left_join_001.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testLeftJoin_002() throws Exception {
        QueryTester
            .build()
            .withConfig(testConfig)
            .withGraphDefine("/sql2graph/graph_student.sql")
            .withQueryPath("/sql2graph/left_join_002.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testLeftJoin_003() throws Exception {
        QueryTester
            .build()
            .withConfig(testConfig)
            .withGraphDefine("/sql2graph/graph_student.sql")
            .withQueryPath("/sql2graph/left_join_003.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testLeftJoin_004() throws Exception {
        QueryTester
            .build()
            .withConfig(testConfig)
            .withGraphDefine("/sql2graph/graph_student.sql")
            .withQueryPath("/sql2graph/left_join_004.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testLeftJoin_005() throws Exception {
        QueryTester
            .build()
            .withConfig(testConfig)
            .withGraphDefine("/sql2graph/graph_student.sql")
            .withQueryPath("/sql2graph/left_join_005.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testLeftJoin_006() throws Exception {
        //di_join_001
        QueryTester
            .build()
            .withConfig(testConfig)
            .withGraphDefine("/sql2graph/graph_student.sql")
            .withQueryPath("/sql2graph/left_join_006.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testLeftJoin_007() throws Exception {
        //di_join_0011
        QueryTester
            .build()
            .withConfig(testConfig)
            .withGraphDefine("/sql2graph/graph_student.sql")
            .withQueryPath("/sql2graph/left_join_007.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testLeftJoin_008() throws Exception {
        QueryTester
            .build()
            .withConfig(testConfig)
            .withGraphDefine("/sql2graph/graph_student.sql")
            .withQueryPath("/sql2graph/left_join_008.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testTableScan_001() throws Exception {
        QueryTester
            .build()
            .withConfig(testConfig)
            .withGraphDefine("/sql2graph/graph_student.sql")
            .withQueryPath("/sql2graph/table_scan_001.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testTableScan_002() throws Exception {
        QueryTester
            .build()
            .withConfig(testConfig)
            .withGraphDefine("/sql2graph/graph_student.sql")
            .withQueryPath("/sql2graph/table_scan_002.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testTableScan_003() throws Exception {
        QueryTester
            .build()
            .withConfig(testConfig)
            .withGraphDefine("/sql2graph/graph_student.sql")
            .withQueryPath("/sql2graph/table_scan_003.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testJoinEdgeWithFilter_001() throws Exception {
        QueryTester
            .build()
            .withConfig(testConfig)
            .withGraphDefine("/sql2graph/graph_student.sql")
            .withQueryPath("/sql2graph/match_join_edge_with_filter_001.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testJoinEdgeWithGroup_001() throws Exception {
        QueryTester
            .build()
            .withConfig(testConfig)
            .withGraphDefine("/sql2graph/graph_student.sql")
            .withQueryPath("/sql2graph/match_join_edge_with_group_001.sql")
            .execute()
            .checkSinkResult();
    }
}
