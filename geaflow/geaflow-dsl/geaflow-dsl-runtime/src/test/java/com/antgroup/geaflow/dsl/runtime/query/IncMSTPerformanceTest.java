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

package com.antgroup.geaflow.dsl.runtime.query;

import com.antgroup.geaflow.common.config.keys.DSLConfigKeys;
import com.antgroup.geaflow.common.config.keys.FrameworkConfigKeys;
import com.antgroup.geaflow.file.FileConfigKeys;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.testng.annotations.Test;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.AfterClass;

/**
 * IncMST算法性能测试类
 * 测试算法在大图场景下的性能表现
 * 
 * @author TuGraph Analytics Team
 */
public class IncMSTPerformanceTest {

    private final String TEST_GRAPH_PATH = "/tmp/geaflow/dsl/inc_mst/perf_test/graph";
    private long startTime;
    private long endTime;

    @BeforeClass
    public void setUp() throws IOException {
        // 清理测试目录
        FileUtils.deleteDirectory(new File(TEST_GRAPH_PATH));
        System.out.println("=== IncMST Performance Test Setup Complete ===");
    }

    @AfterClass
    public void tearDown() throws IOException {
        // 清理测试目录
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
     * 打印性能指标
     * @param testName 测试名称
     * @param startTime 开始时间（纳秒）
     * @param endTime 结束时间（纳秒）
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
     * 打印内存指标
     * @param testName 测试名称
     * @param initialMemory 初始内存使用（字节）
     * @param finalMemory 最终内存使用（字节）
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
     * 获取当前内存使用情况
     * @return 内存使用量（字节）
     */
    private long getCurrentMemoryUsage() {
        Runtime runtime = Runtime.getRuntime();
        return runtime.totalMemory() - runtime.freeMemory();
    }

    /**
     * 格式化内存大小
     * @param bytes 字节数
     * @return 格式化后的字符串
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
