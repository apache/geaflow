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
import org.apache.commons.io.FileUtils;
import org.apache.geaflow.common.config.keys.DSLConfigKeys;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.file.FileConfigKeys;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Issue #363 SF1 Dataset Test - Optimized Query Only
 *
 * Tests the optimized query performance with LDBC SF1 dataset (660x scale):
 * - 9,892 Person vertices
 * - 180,623 Person_knows_Person edges
 * - 2.05M Comments, 1.00M Posts, 90K Forums
 *
 * <p>NOTE: This test is disabled in CI because the SF1 dataset files are not included
 * in the repository due to their large size. To run this test manually:
 * 1. Download LDBC SF1 dataset
 * 2. Convert data using scripts/generate_ldbc_test_data.py
 * 3. Place data files in src/test/resources/data_sf1/
 * 4. Enable the test by removing (enabled = false)
 */
public class Issue363SF1OptimizedOnlyTest {

    private final String TEST_GRAPH_PATH = "/tmp/geaflow/dsl/issue363/sf1/optimized_only";

    private final Map<String, String> testConfig = new HashMap<String, String>() {
        {
            put(FileConfigKeys.PERSISTENT_TYPE.getKey(), "DFS");
            put(FileConfigKeys.ROOT.getKey(), TEST_GRAPH_PATH);
            put(FileConfigKeys.JSON_CONFIG.getKey(), "{\"fs.defaultFS\":\"local\"}");
            put(DSLConfigKeys.GEAFLOW_DSL_WINDOW_SIZE.getKey(), "1");
            put(ExecutionConfigKeys.CONTAINER_WORKER_NUM.getKey(), "24");
        }
    };

    @BeforeClass
    public void setUp() throws Exception {
        FileUtils.deleteQuietly(new File(TEST_GRAPH_PATH));
    }

    @AfterClass
    public void tearDown() throws Exception {
        FileUtils.deleteQuietly(new File(TEST_GRAPH_PATH));
    }

    /**
     * Test optimized query with SF1 dataset
     */
    @Test(enabled = false)
    public void testOptimizedQuerySF1() throws Exception {
        System.out.println("\n======================================================================");
        System.out.println("Issue #363 SF1 Optimized Query Test");
        System.out.println("Dataset: 9,892 Person vertices, 180,623 edges (660x scale)");
        System.out.println("======================================================================\n");

        int iterations = 5;
        long[] executionTimes = new long[iterations];

        for (int i = 0; i < iterations; i++) {
            System.out.println("Iteration " + (i + 1) + "/" + iterations);
            long startTime = System.currentTimeMillis();

            QueryTester.build()
                .withGraphDefine("/ldbc/bi_graph_schema_sf1.sql")
                .withQueryPath("/ldbc/issue_363_optimized.sql")
                .withConfig(testConfig)
                .execute();

            long executionTime = System.currentTimeMillis() - startTime;
            executionTimes[i] = executionTime;
            System.out.println("  Execution time: " + executionTime + "ms\n");
        }

        // Calculate statistics
        long min = executionTimes[0];
        long max = executionTimes[0];
        long sum = 0;

        for (long time : executionTimes) {
            min = Math.min(min, time);
            max = Math.max(max, time);
            sum += time;
        }

        double average = (double) sum / iterations;

        System.out.println("\n======================================================================");
        System.out.println("SF1 Optimized Query Performance Statistics");
        System.out.println("======================================================================\n");
        System.out.println("Iterations: " + iterations);
        System.out.println("Min:        " + min + "ms");
        System.out.println("Max:        " + max + "ms");
        System.out.println("Average:    " + String.format("%.2f", average) + "ms");
        System.out.println("\n======================================================================\n");
    }
}
