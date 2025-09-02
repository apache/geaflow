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

import org.apache.geaflow.common.config.keys.DSLConfigKeys;
import org.apache.geaflow.common.config.keys.FrameworkConfigKeys;
import org.apache.geaflow.file.FileConfigKeys;
import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.testng.annotations.Test;

/**
 * 增量最小生成树算法测试类
 * 包含基础功能测试、增量更新测试、连通性验证等
 * 
 * @author Geaflow Team
 */
public class IncMSTTest {

    private final String TEST_GRAPH_PATH = "/tmp/geaflow/dsl/inc_mst/test/graph";

    @Test
    public void testIncMST_001_Basic() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_inc_mst_001.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testIncMST_002_IncrementalUpdate() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_inc_mst_002.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testIncMST_003_LargeGraph() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/large_graph.sql")
            .withQueryPath("/query/gql_inc_mst_003.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testIncMST_004_EdgeAddition() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/dynamic_graph.sql")
            .withQueryPath("/query/gql_inc_mst_004.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testIncMST_005_EdgeDeletion() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/dynamic_graph.sql")
            .withQueryPath("/query/gql_inc_mst_005.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testIncMST_006_ConnectedComponents() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/disconnected_graph.sql")
            .withQueryPath("/query/gql_inc_mst_006.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testIncMST_007_Performance() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/performance_graph.sql")
            .withQueryPath("/query/gql_inc_mst_007.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testIncMST_008_Convergence() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_inc_mst_008.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testIncMST_009_CustomParameters() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_inc_mst_009.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testIncMST_010_ComplexTopology() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/complex_graph.sql")
            .withQueryPath("/query/gql_inc_mst_010.sql")
            .execute()
            .checkSinkResult();
    }
}
