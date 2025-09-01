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
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.geaflow.common.config.keys.DSLConfigKeys;
import org.apache.geaflow.file.FileConfigKeys;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class LdbcTest {

    private final String TEST_GRAPH_PATH = "/tmp/geaflow/dsl/bi/test/graph";

    private final Map<String, String> testConfig = new HashMap<String, String>() {
        {
            put(FileConfigKeys.PERSISTENT_TYPE.getKey(), "DFS");
            put(FileConfigKeys.ROOT.getKey(), TEST_GRAPH_PATH);
            put(FileConfigKeys.JSON_CONFIG.getKey(), "{\"fs.defaultFS\":\"local\"}");
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
            .withConfig(DSLConfigKeys.GEAFLOW_DSL_WINDOW_SIZE.getKey(), "1")
            .withConfig(FileConfigKeys.PERSISTENT_TYPE.getKey(), "DFS")
            .withConfig(FileConfigKeys.ROOT.getKey(), TEST_GRAPH_PATH)
            .withConfig(FileConfigKeys.JSON_CONFIG.getKey(), "{\"fs.defaultFS\":\"local\"}")
            .withQueryPath("/ldbc/bi_insert_01.sql")
            .execute()
            .withQueryPath("/ldbc/bi_insert_02.sql")
            .execute()
            .withQueryPath("/ldbc/bi_insert_03.sql")
            .execute()
            .withQueryPath("/ldbc/bi_insert_04.sql")
            .execute()
            .withQueryPath("/ldbc/bi_insert_05.sql")
            .execute()
            .withQueryPath("/ldbc/bi_insert_06.sql")
            .execute();
    }

    @AfterClass
    public void tearDown() throws Exception {
        File file = new File(TEST_GRAPH_PATH);
        if (file.exists()) {
            FileUtils.deleteDirectory(file);
        }
    }

    @Test
    public void testLdbcBi_01() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/ldbc/bi_01.sql")
            .withConfig(testConfig)
            .execute()
            .checkSinkResult();

        QueryTester
            .build()
            .withQueryPath("/ldbc/bi_01.sql")
            .withConfig(testConfig)
            .withConfig(DSLConfigKeys.GEAFLOW_DSL_TRAVERSAL_SPLIT_ENABLE.getKey(), String.valueOf(true))
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testLdbcBi_02() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/ldbc/bi_02.sql")
            .withConfig(testConfig)
            .execute()
            .checkSinkResult();

        QueryTester
            .build()
            .withQueryPath("/ldbc/bi_02.sql")
            .withConfig(testConfig)
            .withConfig(DSLConfigKeys.GEAFLOW_DSL_TRAVERSAL_SPLIT_ENABLE.getKey(), String.valueOf(true))
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testLdbcBi_03() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/ldbc/bi_03.sql")
            .withConfig(testConfig)
            .execute()
            .checkSinkResult();

        QueryTester
            .build()
            .withQueryPath("/ldbc/bi_03.sql")
            .withConfig(testConfig)
            .withConfig(DSLConfigKeys.GEAFLOW_DSL_TRAVERSAL_SPLIT_ENABLE.getKey(), String.valueOf(true))
            .execute()
            .checkSinkResult();
    }

    @Test(enabled = false)
    public void testLdbcBi_04() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/ldbc/bi_04.sql")
            .withConfig(testConfig)
            .execute()
            .checkSinkResult();

        QueryTester
            .build()
            .withQueryPath("/ldbc/bi_04.sql")
            .withConfig(testConfig)
            .withConfig(DSLConfigKeys.GEAFLOW_DSL_TRAVERSAL_SPLIT_ENABLE.getKey(), String.valueOf(true))
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testLdbcBi_05() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/ldbc/bi_05.sql")
            .withConfig(testConfig)
            .execute()
            .checkSinkResult();

        QueryTester
            .build()
            .withQueryPath("/ldbc/bi_05.sql")
            .withConfig(testConfig)
            .withConfig(DSLConfigKeys.GEAFLOW_DSL_TRAVERSAL_SPLIT_ENABLE.getKey(), String.valueOf(true))
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testLdbcBi_06() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/ldbc/bi_06.sql")
            .withConfig(testConfig)
            .execute()
            .checkSinkResult();

        QueryTester
            .build()
            .withQueryPath("/ldbc/bi_06.sql")
            .withConfig(testConfig)
            .withConfig(DSLConfigKeys.GEAFLOW_DSL_TRAVERSAL_SPLIT_ENABLE.getKey(), String.valueOf(true))
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testLdbcBi_07() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/ldbc/bi_07.sql")
            .withConfig(testConfig)
            .execute()
            .checkSinkResult();

        QueryTester
            .build()
            .withQueryPath("/ldbc/bi_07.sql")
            .withConfig(testConfig)
            .withConfig(DSLConfigKeys.GEAFLOW_DSL_TRAVERSAL_SPLIT_ENABLE.getKey(), String.valueOf(true))
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testLdbcBi_08() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/ldbc/bi_08.sql")
            .withConfig(testConfig)
            .execute()
            .checkSinkResult();

        QueryTester
            .build()
            .withQueryPath("/ldbc/bi_08.sql")
            .withConfig(testConfig)
            .withConfig(DSLConfigKeys.GEAFLOW_DSL_TRAVERSAL_SPLIT_ENABLE.getKey(), String.valueOf(true))
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testLdbcBi_09() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/ldbc/bi_09.sql")
            .withConfig(testConfig)
            .execute()
            .checkSinkResult();

        QueryTester
            .build()
            .withQueryPath("/ldbc/bi_09.sql")
            .withConfig(testConfig)
            .withConfig(DSLConfigKeys.GEAFLOW_DSL_TRAVERSAL_SPLIT_ENABLE.getKey(), String.valueOf(true))
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testLdbcBi_10() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/ldbc/bi_10.sql")
            .withConfig(DSLConfigKeys.GEAFLOW_DSL_WINDOW_SIZE.getKey(), "-1")
            .withConfig(testConfig)
            .execute()
            .checkSinkResult();

        QueryTester
            .build()
            .withQueryPath("/ldbc/bi_10.sql")
            .withConfig(testConfig)
            .withConfig(DSLConfigKeys.GEAFLOW_DSL_TRAVERSAL_SPLIT_ENABLE.getKey(), String.valueOf(true))
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testLdbcBi_11() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/ldbc/bi_11.sql")
            .withConfig(testConfig)
            .execute()
            .checkSinkResult();

        QueryTester
            .build()
            .withQueryPath("/ldbc/bi_11.sql")
            .withConfig(testConfig)
            .withConfig(DSLConfigKeys.GEAFLOW_DSL_TRAVERSAL_SPLIT_ENABLE.getKey(), String.valueOf(true))
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testLdbcBi_12() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/ldbc/bi_12.sql")
            .withConfig(testConfig)
            .execute()
            .checkSinkResult();

        QueryTester
            .build()
            .withQueryPath("/ldbc/bi_12.sql")
            .withConfig(testConfig)
            .withConfig(DSLConfigKeys.GEAFLOW_DSL_TRAVERSAL_SPLIT_ENABLE.getKey(), String.valueOf(true))
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testLdbcBi_13() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/ldbc/bi_13.sql")
            .withConfig(testConfig)
            .execute()
            .checkSinkResult();

        QueryTester
            .build()
            .withQueryPath("/ldbc/bi_13.sql")
            .withConfig(testConfig)
            .withConfig(DSLConfigKeys.GEAFLOW_DSL_TRAVERSAL_SPLIT_ENABLE.getKey(), String.valueOf(true))
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testLdbcBi_14() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/ldbc/bi_14.sql")
            .withConfig(testConfig)
            .execute()
            .checkSinkResult();

        QueryTester
            .build()
            .withQueryPath("/ldbc/bi_14.sql")
            .withConfig(testConfig)
            .withConfig(DSLConfigKeys.GEAFLOW_DSL_TRAVERSAL_SPLIT_ENABLE.getKey(), String.valueOf(true))
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testLdbcBi_15() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/ldbc/bi_15.sql")
            .withConfig(testConfig)
            .execute()
            .checkSinkResult();

        QueryTester
            .build()
            .withQueryPath("/ldbc/bi_15.sql")
            .withConfig(testConfig)
            .withConfig(DSLConfigKeys.GEAFLOW_DSL_TRAVERSAL_SPLIT_ENABLE.getKey(), String.valueOf(true))
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testLdbcBi_16() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/ldbc/bi_16.sql")
            .withConfig(testConfig)
            .execute()
            .checkSinkResult();

        QueryTester
            .build()
            .withQueryPath("/ldbc/bi_16.sql")
            .withConfig(testConfig)
            .withConfig(DSLConfigKeys.GEAFLOW_DSL_TRAVERSAL_SPLIT_ENABLE.getKey(), String.valueOf(true))
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testLdbcBi_17() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/ldbc/bi_17.sql")
            .withConfig(testConfig)
            .execute()
            .checkSinkResult();

        QueryTester
            .build()
            .withQueryPath("/ldbc/bi_17.sql")
            .withConfig(testConfig)
            .withConfig(DSLConfigKeys.GEAFLOW_DSL_TRAVERSAL_SPLIT_ENABLE.getKey(), String.valueOf(true))
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testLdbcBi_18() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/ldbc/bi_18.sql")
            .withConfig(testConfig)
            .execute()
            .checkSinkResult();

        QueryTester
            .build()
            .withQueryPath("/ldbc/bi_18.sql")
            .withConfig(testConfig)
            .withConfig(DSLConfigKeys.GEAFLOW_DSL_TRAVERSAL_SPLIT_ENABLE.getKey(), String.valueOf(true))
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testLdbcBi_19() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/ldbc/bi_19.sql")
            .withConfig(testConfig)
            .execute()
            .checkSinkResult();

        QueryTester
            .build()
            .withQueryPath("/ldbc/bi_19.sql")
            .withConfig(testConfig)
            .withConfig(DSLConfigKeys.GEAFLOW_DSL_TRAVERSAL_SPLIT_ENABLE.getKey(), String.valueOf(true))
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testLdbcBi_20() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/ldbc/bi_20.sql")
            .withConfig(testConfig)
            .execute()
            .checkSinkResult();

        QueryTester
            .build()
            .withQueryPath("/ldbc/bi_20.sql")
            .withConfig(testConfig)
            .withConfig(DSLConfigKeys.GEAFLOW_DSL_TRAVERSAL_SPLIT_ENABLE.getKey(), String.valueOf(true))
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testLdbcIs_01() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/ldbc/is_01.sql")
            .withConfig(testConfig)
            .execute()
            .checkSinkResult();

        QueryTester
            .build()
            .withQueryPath("/ldbc/is_01.sql")
            .withConfig(testConfig)
            .withConfig(DSLConfigKeys.GEAFLOW_DSL_TRAVERSAL_SPLIT_ENABLE.getKey(), String.valueOf(true))
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testLdbcIs_02() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/ldbc/is_02.sql")
            .withConfig(testConfig)
            .execute()
            .checkSinkResult();

        QueryTester
            .build()
            .withQueryPath("/ldbc/is_02.sql")
            .withConfig(testConfig)
            .withConfig(DSLConfigKeys.GEAFLOW_DSL_TRAVERSAL_SPLIT_ENABLE.getKey(), String.valueOf(true))
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testLdbcIs_03() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/ldbc/is_03.sql")
            .withConfig(testConfig)
            .execute()
            .checkSinkResult();

        QueryTester
            .build()
            .withQueryPath("/ldbc/is_03.sql")
            .withConfig(testConfig)
            .withConfig(DSLConfigKeys.GEAFLOW_DSL_TRAVERSAL_SPLIT_ENABLE.getKey(), String.valueOf(true))
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testLdbcIs_04() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/ldbc/is_04.sql")
            .withConfig(testConfig)
            .execute()
            .checkSinkResult();

        QueryTester
            .build()
            .withQueryPath("/ldbc/is_04.sql")
            .withConfig(testConfig)
            .withConfig(DSLConfigKeys.GEAFLOW_DSL_TRAVERSAL_SPLIT_ENABLE.getKey(), String.valueOf(true))
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testLdbcIs_05() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/ldbc/is_05.sql")
            .withConfig(testConfig)
            .execute()
            .checkSinkResult();

        QueryTester
            .build()
            .withQueryPath("/ldbc/is_05.sql")
            .withConfig(testConfig)
            .withConfig(DSLConfigKeys.GEAFLOW_DSL_TRAVERSAL_SPLIT_ENABLE.getKey(), String.valueOf(true))
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testLdbcIs_06() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/ldbc/is_06.sql")
            .withConfig(testConfig)
            .execute()
            .checkSinkResult();

        QueryTester
            .build()
            .withQueryPath("/ldbc/is_06.sql")
            .withConfig(testConfig)
            .withConfig(DSLConfigKeys.GEAFLOW_DSL_TRAVERSAL_SPLIT_ENABLE.getKey(), String.valueOf(true))
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testLdbcIs_07() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/ldbc/is_07.sql")
            .withConfig(testConfig)
            .execute()
            .checkSinkResult();

        QueryTester
            .build()
            .withQueryPath("/ldbc/is_07.sql")
            .withConfig(testConfig)
            .withConfig(DSLConfigKeys.GEAFLOW_DSL_TRAVERSAL_SPLIT_ENABLE.getKey(), String.valueOf(true))
            .execute()
            .checkSinkResult();
    }
}
