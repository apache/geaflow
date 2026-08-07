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
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.geaflow.common.config.keys.DSLConfigKeys;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.file.FileConfigKeys;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Issue #363 SF1 Dataset Test - Optimized Query Only
 *
 * Tests the optimized query performance with LDBC SF1 dataset (660x scale):
 * - 9,892 Person vertices
 * - 180,623 Person_knows_Person edges
 * - 2.05M Comments, 1.00M Posts, 90K Forums
 *
 * <p>NOTE: This test is skipped in CI by default because the SF1 dataset files are not
 * included in the repository due to their large size. To run this test manually:
 * 1. Prepare the required dataset files under src/test/resources/data_sf1/
 * 2. Run this test locally (it will be skipped if data is missing)
 */
public class Issue363SF1OptimizedOnlyTest {

    private final String TEST_GRAPH_PATH = "/tmp/geaflow/dsl/issue363/sf1/optimized_only";

    private static final String SF1_DATA_ROOT_KEY = "sf1_data_root";
    private static final String SF1_DATA_ROOT_DEFAULT = "resource:///data_sf1";

    private static final String ISSUE363_SF1_SHARD_COUNT_KEY = "issue363_sf1_shard_count";

    private static final String ISSUE363_SF1_CONTAINER_HEAP_MB_KEY = "issue363.sf1.container.heap.mb";
    private static final int ISSUE363_SF1_CONTAINER_HEAP_MB_DEFAULT = 8192;

    private static final String ISSUE363_A_ID_KEY = "issue363_a_id";
    private static final String ISSUE363_D_ID_KEY = "issue363_d_id";
    // Defaults chosen from official BI SF1 dataset (small creator to keep results bounded).
    private static final String ISSUE363_A_ID_DEFAULT = "32985348834678";
    private static final String ISSUE363_D_ID_DEFAULT = "4398046519310";

    private static final String[] REQUIRED_SF1_DATA_ENTRIES = {
        "bi_person",
        "bi_person_knows_person",
        "bi_comment_hasCreator_person",
        "bi_post_hasCreator_person"
    };

    private final Map<String, String> testConfig = new HashMap<String, String>() {
        {
            put(FileConfigKeys.PERSISTENT_TYPE.getKey(), "DFS");
            put(FileConfigKeys.ROOT.getKey(), TEST_GRAPH_PATH);
            put(FileConfigKeys.JSON_CONFIG.getKey(), "{\"fs.defaultFS\":\"local\"}");
            put(DSLConfigKeys.GEAFLOW_DSL_WINDOW_SIZE.getKey(), "-1");
            int workers = Math.max(1, Integer.getInteger("issue363.sf1.workers", 8));
            put(ExecutionConfigKeys.CONTAINER_WORKER_NUM.getKey(), String.valueOf(workers));
            put(ExecutionConfigKeys.CONTAINER_JVM_OPTION.getKey(), resolveSf1ContainerJvmOptions());
            put(SF1_DATA_ROOT_KEY, resolveSf1DataRoot());
            put(ISSUE363_SF1_SHARD_COUNT_KEY, String.valueOf(Integer.highestOneBit(workers)));
            put(ISSUE363_A_ID_KEY, resolveIssue363Id(ISSUE363_A_ID_KEY, ISSUE363_A_ID_DEFAULT));
            put(ISSUE363_D_ID_KEY, resolveIssue363Id(ISSUE363_D_ID_KEY, ISSUE363_D_ID_DEFAULT));
        }
    };

    @BeforeClass
    public void setUp() throws Exception {
        FileUtils.deleteQuietly(new File(TEST_GRAPH_PATH));
        // Pre-load graph once to avoid including graph ingestion time in query measurements.
        ensureSf1DatasetPresent(resolveSf1DataRoot());
        System.out.println("\n======================================================================");
        System.out.println("Issue #363 SF1 Dataset Setup (Optimized Only)");
        System.out.println("Loading graph into: " + TEST_GRAPH_PATH);
        System.out.println("======================================================================\n");
        QueryTester.build()
            .withGraphDefine("/ldbc/bi_graph_schema_sf1_issue363.sql")
            .withQueryPath("/ldbc/issue_363_sf1_setup.sql")
            .withConfig(testConfig)
            .execute();
    }

    @AfterClass
    public void tearDown() throws Exception {
        FileUtils.deleteQuietly(new File(TEST_GRAPH_PATH));
    }

    /**
     * Test optimized query with SF1 dataset
     */
    @Test
    public void testOptimizedQuerySF1() throws Exception {
        System.out.println("\n======================================================================");
        System.out.println("Issue #363 SF1 Optimized Query Test");
        System.out.println("Dataset: 9,892 Person vertices, 180,623 edges (660x scale)");
        System.out.println("======================================================================\n");

        int iterations = Math.max(1, Integer.getInteger("issue363.sf1.iterations", 5));
        long[] executionTimes = new long[iterations];

        for (int i = 0; i < iterations; i++) {
            System.out.println("Iteration " + (i + 1) + "/" + iterations);
            long startTime = System.currentTimeMillis();

            QueryTester.build()
                .withGraphDefine("/ldbc/bi_graph_schema_sf1_issue363_ddl.sql")
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

    private static String resolveSf1DataRoot() {
        String fromSystemProperty = System.getProperty(SF1_DATA_ROOT_KEY);
        if (fromSystemProperty != null && !fromSystemProperty.trim().isEmpty()) {
            return fromSystemProperty.trim();
        }
        String fromEnv = System.getenv("GEAFLOW_SF1_DATA_ROOT");
        if (fromEnv != null && !fromEnv.trim().isEmpty()) {
            return fromEnv.trim();
        }
        return SF1_DATA_ROOT_DEFAULT;
    }

    private static String resolveIssue363Id(String key, String defaultValue) {
        String fromSystemProperty = System.getProperty(key);
        if (fromSystemProperty != null && !fromSystemProperty.trim().isEmpty()) {
            return fromSystemProperty.trim();
        }
        String envKey = "GEAFLOW_" + key.toUpperCase();
        String fromEnv = System.getenv(envKey);
        if (fromEnv != null && !fromEnv.trim().isEmpty()) {
            return fromEnv.trim();
        }
        return defaultValue;
    }

    private static void ensureSf1DatasetPresent(String sf1DataRoot) {
        if (sf1DataRoot != null && sf1DataRoot.startsWith("resource:///")) {
            String base = sf1DataRoot.substring("resource:///".length());
            if (!base.startsWith("/")) {
                base = "/" + base;
            }
            for (String entry : REQUIRED_SF1_DATA_ENTRIES) {
                String resource = base + "/" + entry;
                if (Issue363SF1OptimizedOnlyTest.class.getResource(resource) == null) {
                    throw new SkipException(
                        "LDBC SF1 dataset not found on classpath (missing resource: " + resource + "). "
                            + "Either place data under src/test/resources" + base
                            + ", or run with -D" + SF1_DATA_ROOT_KEY + "=file:///path/to/sf1-data (or GEAFLOW_SF1_DATA_ROOT).");
                }
            }
            return;
        }

        Path rootPath = toLocalPath(sf1DataRoot);
        if (rootPath == null) {
            throw new SkipException(
                "LDBC SF1 dataset root is not configured. "
                    + "Run with -D" + SF1_DATA_ROOT_KEY + "=file:///path/to/sf1-data (or GEAFLOW_SF1_DATA_ROOT).");
        }
        for (String entry : REQUIRED_SF1_DATA_ENTRIES) {
            Path entryPath = rootPath.resolve(entry);
            if (!Files.exists(entryPath)) {
                throw new SkipException(
                    "LDBC SF1 dataset not found (missing path: " + entryPath + "). "
                        + "Run with -D" + SF1_DATA_ROOT_KEY + "=file:///path/to/sf1-data (or GEAFLOW_SF1_DATA_ROOT).");
            }
        }
    }

    private static Path toLocalPath(String sf1DataRoot) {
        if (sf1DataRoot == null || sf1DataRoot.trim().isEmpty()) {
            return null;
        }
        String root = sf1DataRoot.trim();
        if (root.startsWith("file:")) {
            return Paths.get(URI.create(root));
        }
        return Paths.get(root);
    }

    private static String resolveSf1ContainerJvmOptions() {
        String fromSystemProperty = System.getProperty(ExecutionConfigKeys.CONTAINER_JVM_OPTION.getKey());
        if (fromSystemProperty != null && !fromSystemProperty.trim().isEmpty()) {
            return fromSystemProperty.trim();
        }
        String fromEnv = System.getenv("GEAFLOW_CONTAINER_JVM_OPTIONS");
        if (fromEnv != null && !fromEnv.trim().isEmpty()) {
            return fromEnv.trim();
        }

        int heapMb = Math.max(1024, Integer.getInteger(
            ISSUE363_SF1_CONTAINER_HEAP_MB_KEY,
            ISSUE363_SF1_CONTAINER_HEAP_MB_DEFAULT));
        return "-Xmx" + heapMb + "m,-Xms" + heapMb + "m";
    }
}
