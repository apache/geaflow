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

package org.apache.geaflow.example.dsl;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.geaflow.common.config.keys.DSLConfigKeys;
import org.apache.geaflow.example.base.BaseQueryTest;
import org.apache.geaflow.file.FileConfigKeys;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

public class DemoCaseTest {

    private final String TEST_GRAPH_PATH = "/tmp/geaflow/dsl/join2Graph/test/graph";

    private final Map<String, String> testConfig = new HashMap<String, String>() {
        {
            put(FileConfigKeys.PERSISTENT_TYPE.getKey(), "DFS");
            put(FileConfigKeys.ROOT.getKey(), TEST_GRAPH_PATH);
            put(FileConfigKeys.JSON_CONFIG.getKey(), "{\"fs.defaultFS\":\"local\"}");
            put(DSLConfigKeys.GEAFLOW_DSL_QUERY_PATH_TYPE.getKey(), "file_path");
            // If the test is conducted using the console catalog, the appended config is required.
            // put(DSLConfigKeys.GEAFLOW_DSL_CATALOG_TYPE.getKey(), "console");
            // put(DSLConfigKeys.GEAFLOW_DSL_CATALOG_TOKEN_KEY.getKey(), "");
            // put(DSLConfigKeys.GEAFLOW_DSL_CATALOG_INSTANCE_NAME.getKey(), "test1");
            // put(ExecutionConfigKeys.GEAFLOW_GW_ENDPOINT.getKey(), "http://127.0.0.1:8080");
        }
    };

    @AfterClass
    public void tearDown() throws Exception {
        File file = new File(TEST_GRAPH_PATH);
        if (file.exists()) {
            FileUtils.deleteDirectory(file);
        }
    }

    @Test
    public void testQuickStartSqlJoinDemo_001() throws Exception {
        String resultPath = "/tmp/geaflow/sql_join_to_graph_demo_result";
        File file = new File(resultPath);
        if (file.exists()) {
            FileUtils.deleteDirectory(file);
        }
        BaseQueryTest
            .build()
            .withoutPrefix()
            .withConfig(testConfig)
            .withQueryPath(System.getProperty("user.dir") + "/gql/sql_join_to_graph_demo.sql")
            .execute()
            .checkSinkResult(resultPath);
    }

    @Test
    public void testQuickStartSqlJoinDemo_002() throws Exception {
        String resultPath = "/tmp/geaflow/sql_join_to_graph_demo_02_result";
        File file = new File(resultPath);
        if (file.exists()) {
            FileUtils.deleteDirectory(file);
        }
        BaseQueryTest
            .build()
            .withoutPrefix()
            .withConfig(testConfig)
            .withQueryPath(System.getProperty("user.dir") + "/gql/sql_join_to_graph_demo_02.sql")
            .execute()
            .checkSinkResult(resultPath);
    }

}
