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
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.testng.annotations.Test;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.AfterClass;

/**
 * IncMST algorithm performance test class
 * Test algorithm performance in large graph scenarios
 * 
 * @author Geaflow Team
 */
public class IncMSTPerformanceTest {

    private final String TEST_GRAPH_PATH = "/tmp/geaflow/dsl/inc_mst/perf_test/graph";
    private long startTime;
    private long endTime;

    @BeforeClass
    public void setUp() throws IOException {
        // Clean up test directory
        FileUtils.deleteDirectory(new File(TEST_GRAPH_PATH));
        System.out.println("=== IncMST Performance Test Setup Complete ===");
    }

    @AfterClass
    public void tearDown() throws IOException {
        // Clean up test directory
        FileUtils.deleteDirectory(new File(TEST_GRAPH_PATH));
        System.out.println("=== IncMST Performance Test Cleanup Complete ===");
    }

    @Test
    public void testIncMST_001_SmallGraphPerformance() throws Exception {
        System.out.println("Starting Small Graph Performance Test...");
        startTime = System.nanoTime();

        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_inc_mst_perf_001.sql")
            .execute()
            .checkSinkResult();

        endTime = System.nanoTime();
        printPerformanceMetrics("Small Graph (Modern)", startTime, endTime);
    }

    @Test
    public void testIncMST_002_MediumGraphPerformance() throws Exception {
        System.out.println("Starting Medium Graph Performance Test...");
        startTime = System.nanoTime();

        QueryTester
            .build()
            .withGraphDefine("/query/medium_graph.sql")
            .withQueryPath("/query/gql_inc_mst_perf_002.sql")
            .execute()
            .checkSinkResult();

        endTime = System.nanoTime();
        printPerformanceMetrics("Medium Graph (1K vertices)", startTime, endTime);
    }

    @Test
    public void testIncMST_003_LargeGraphPerformance() throws Exception {
        System.out.println("Starting Large Graph Performance Test...");
        startTime = System.nanoTime();

        QueryTester
            .build()
            .withGraphDefine("/query/large_graph.sql")
            .withQueryPath("/query/gql_inc_mst_perf_003.sql")
            .execute()
            .checkSinkResult();

        endTime = System.nanoTime();
        printPerformanceMetrics("Large Graph (10K vertices)", startTime, endTime);
    }

    @Test
    public void testIncMST_004_IncrementalUpdatePerformance() throws Exception {
        System.out.println("Starting Incremental Update Performance Test...");
        startTime = System.nanoTime();

        QueryTester
            .build()
            .withGraphDefine("/query/dynamic_graph.sql")
            .withQueryPath("/query/gql_inc_mst_perf_004.sql")
            .execute()
            .checkSinkResult();

        endTime = System.nanoTime();
        printPerformanceMetrics("Incremental Update", startTime, endTime);
    }

    @Test
    public void testIncMST_005_ConvergencePerformance() throws Exception {
        System.out.println("Starting Convergence Performance Test...");
        startTime = System.nanoTime();

        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_inc_mst_perf_005.sql")
            .execute()
            .checkSinkResult();

        endTime = System.nanoTime();
        printPerformanceMetrics("Convergence Test", startTime, endTime);
    }

    @Test
    public void testIncMST_006_MemoryEfficiency() throws Exception {
        System.out.println("Starting Memory Efficiency Test...");
        long initialMemory = getCurrentMemoryUsage();
        startTime = System.nanoTime();

        QueryTester
            .build()
            .withGraphDefine("/query/large_graph.sql")
            .withQueryPath("/query/gql_inc_mst_perf_006.sql")
            .execute()
            .checkSinkResult();

        endTime = System.nanoTime();
        long finalMemory = getCurrentMemoryUsage();
        printPerformanceMetrics("Memory Efficiency", startTime, endTime);
        printMemoryMetrics("Memory Efficiency", initialMemory, finalMemory);
    }

    @Test
    public void testIncMST_007_ScalabilityTest() throws Exception {
        System.out.println("Starting Scalability Test...");
        startTime = System.nanoTime();

        QueryTester
            .build()
            .withGraphDefine("/query/scalability_graph.sql")
            .withQueryPath("/query/gql_inc_mst_perf_007.sql")
            .execute()
            .checkSinkResult();

        endTime = System.nanoTime();
        printPerformanceMetrics("Scalability Test (100K vertices)", startTime, endTime);
    }

    /**
     * Print performance metrics
     * @param testName Test name
     * @param startTime Start time (nanoseconds)
     * @param endTime End time (nanoseconds)
     */
    private void printPerformanceMetrics(String testName, long startTime, long endTime) {
        long durationNano = endTime - startTime;
        long durationMs = TimeUnit.NANOSECONDS.toMillis(durationNano);
        long durationSec = TimeUnit.NANOSECONDS.toSeconds(durationNano);

        System.out.println("=== Performance Metrics for " + testName + " ===");
        System.out.println("Execution Time: " + durationMs + " ms (" + durationSec + " seconds)");
        System.out.println("Throughput: " + String.format("%.2f", 1000.0 / durationMs) + " operations/ms");
        System.out.println("========================================");
    }

    /**
     * Print memory metrics
     * @param testName Test name
     * @param initialMemory Initial memory usage (bytes)
     * @param finalMemory Final memory usage (bytes)
     */
    private void printMemoryMetrics(String testName, long initialMemory, long finalMemory) {
        long memoryUsed = finalMemory - initialMemory;
        double memoryUsedMB = memoryUsed / (1024.0 * 1024.0);

        System.out.println("=== Memory Metrics for " + testName + " ===");
        System.out.println("Memory Used: " + String.format("%.2f", memoryUsedMB) + " MB");
        System.out.println("Initial Memory: " + formatMemorySize(initialMemory));
        System.out.println("Final Memory: " + formatMemorySize(finalMemory));
        System.out.println("========================================");
    }

    /**
     * Get current memory usage
     * @return Memory usage (bytes)
     */
    private long getCurrentMemoryUsage() {
        Runtime runtime = Runtime.getRuntime();
        return runtime.totalMemory() - runtime.freeMemory();
    }

    /**
     * Format memory size
     * @param bytes Number of bytes
     * @return Formatted string
     */
    private String formatMemorySize(long bytes) {
        if (bytes < 1024) {
            return bytes + " B";
        } else if (bytes < 1024 * 1024) {
            return String.format("%.2f KB", bytes / 1024.0);
        } else if (bytes < 1024 * 1024 * 1024) {
            return String.format("%.2f MB", bytes / (1024.0 * 1024.0));
        } else {
            return String.format("%.2f GB", bytes / (1024.0 * 1024.0 * 1024.0));
        }
    }
}
