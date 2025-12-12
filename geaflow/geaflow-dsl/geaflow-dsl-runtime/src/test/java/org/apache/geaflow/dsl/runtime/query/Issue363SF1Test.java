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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.geaflow.common.config.keys.DSLConfigKeys;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.file.FileConfigKeys;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Issue #363 SF1 Dataset Test
 *
 * Tests the performance optimization with LDBC SF1 dataset (660x scale):
 * - 9,892 Person vertices
 * - 180,623 Person_knows_Person edges
 * - 2.05M Comments, 1.00M Posts, 90K Forums
 *
 * Expected performance improvement: 30-50% for optimized query
 */
public class Issue363SF1Test {

    private final String TEST_GRAPH_PATH = "/tmp/geaflow/dsl/issue363/sf1/graph";

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
     * Comprehensive performance benchmark with SF1 dataset
     */
    @Test
    public void testSF1Performance() throws Exception {
        System.out.println("\n======================================================================");
        System.out.println("Issue #363 SF1 Performance Benchmark");
        System.out.println("Dataset: 9,892 Person vertices, 180,623 edges (660x scale)");
        System.out.println("======================================================================\n");

        int warmupIterations = 2;
        int measurementIterations = 5;

        // Warm-up phase
        System.out.println("--- Warm-up Phase ---");
        for (int i = 0; i < warmupIterations; i++) {
            System.out.println("Warm-up " + (i + 1) + "/" + warmupIterations);
            runQuery("/ldbc/issue_363_original.sql");
            runQuery("/ldbc/issue_363_optimized.sql");
        }

        // Measurement phase - Original query
        System.out.println("\n--- Measuring Original Query (SF1 Dataset) ---");
        long[] originalTimes = new long[measurementIterations];
        for (int i = 0; i < measurementIterations; i++) {
            long time = runQuery("/ldbc/issue_363_original.sql");
            originalTimes[i] = time;
            System.out.println("  Run " + (i + 1) + "/" + measurementIterations + ": " + time + "ms");
        }

        // Measurement phase - Optimized query
        System.out.println("\n--- Measuring Optimized Query (SF1 Dataset) ---");
        long[] optimizedTimes = new long[measurementIterations];
        for (int i = 0; i < measurementIterations; i++) {
            long time = runQuery("/ldbc/issue_363_optimized.sql");
            optimizedTimes[i] = time;
            System.out.println("  Run " + (i + 1) + "/" + measurementIterations + ": " + time + "ms");
        }

        // Calculate and display statistics
        System.out.println("\n======================================================================");
        System.out.println("Performance Analysis Results (SF1 Dataset)");
        System.out.println("======================================================================\n");

        Statistics originalStats = calculateStatistics(originalTimes);
        Statistics optimizedStats = calculateStatistics(optimizedTimes);

        System.out.println("Original Query Statistics:");
        System.out.println("  Min:     " + originalStats.min + "ms");
        System.out.println("  Max:     " + originalStats.max + "ms");
        System.out.println("  Median:  " + originalStats.median + "ms");
        System.out.println("  Average: " + String.format("%.2f", originalStats.average) + "ms");
        System.out.println("  Std Dev: " + String.format("%.2f", originalStats.stdDev) + "ms");

        System.out.println("\nOptimized Query Statistics:");
        System.out.println("  Min:     " + optimizedStats.min + "ms");
        System.out.println("  Max:     " + optimizedStats.max + "ms");
        System.out.println("  Median:  " + optimizedStats.median + "ms");
        System.out.println("  Average: " + String.format("%.2f", optimizedStats.average) + "ms");
        System.out.println("  Std Dev: " + String.format("%.2f", optimizedStats.stdDev) + "ms");

        // Calculate improvements
        double medianImprovement = ((double)(originalStats.median - optimizedStats.median)
            / originalStats.median) * 100;
        double averageImprovement = ((originalStats.average - optimizedStats.average)
            / originalStats.average) * 100;

        System.out.println("\n--- Performance Improvement ---");
        System.out.println("Based on Median:  " + String.format("%.2f", medianImprovement) + "%");
        System.out.println("Based on Average: " + String.format("%.2f", averageImprovement) + "%");
        System.out.println("Absolute time saved (median): "
            + (originalStats.median - optimizedStats.median) + "ms");

        // Compare with baseline results
        System.out.println("\n--- Comparison with Other Datasets ---");
        System.out.println("Small dataset (15 Person):    2.01% improvement");
        System.out.println("Large dataset (300 Person):   15-30% improvement (predicted)");
        System.out.println("SF1 dataset (9,892 Person):   " + String.format("%.2f", medianImprovement) + "% improvement");
        System.out.println("Scale factor vs baseline:     660x data size");

        // Issue #363 targets
        System.out.println("\n--- Issue #363 Goals ---");
        System.out.println("Target Performance Improvement: 30-50%");
        System.out.println("Current Achievement: " + String.format("%.2f", medianImprovement) + "%");

        if (medianImprovement >= 50.0) {
            System.out.println("\n✅ EXCELLENT: Exceeded 50% target!");
        } else if (medianImprovement >= 30.0) {
            System.out.println("\n✅ SUCCESS: Achieved 30-50% target range");
        } else if (medianImprovement >= 20.0) {
            System.out.println("\n⚠️  PARTIAL: Achieved 20%+, approaching target");
        } else {
            System.out.println("\n⚠️  NEEDS IMPROVEMENT: Below 20% threshold");
        }

        System.out.println("\n======================================================================");

        // Assert optimized is faster
        Assert.assertTrue(optimizedStats.median < originalStats.median,
            "Optimized query should be faster than original on SF1 dataset");
    }

    private long runQuery(String queryPath) throws Exception {
        String schemaPath = "/ldbc/bi_graph_schema_sf1.sql";

        long startTime = System.currentTimeMillis();
        QueryTester.build()
            .withGraphDefine(schemaPath)
            .withQueryPath(queryPath)
            .withConfig(testConfig)
            .execute();
        return System.currentTimeMillis() - startTime;
    }

    private Statistics calculateStatistics(long[] values) {
        Statistics stats = new Statistics();

        long[] sorted = Arrays.copyOf(values, values.length);
        Arrays.sort(sorted);

        stats.min = sorted[0];
        stats.max = sorted[sorted.length - 1];

        int mid = sorted.length / 2;
        if (sorted.length % 2 == 0) {
            stats.median = (sorted[mid - 1] + sorted[mid]) / 2;
        } else {
            stats.median = sorted[mid];
        }

        long sum = 0;
        for (long value : values) {
            sum += value;
        }
        stats.average = (double) sum / values.length;

        double variance = 0;
        for (long value : values) {
            variance += Math.pow(value - stats.average, 2);
        }
        stats.stdDev = Math.sqrt(variance / values.length);

        return stats;
    }

    private static class Statistics {
        long min;
        long max;
        long median;
        double average;
        double stdDev;
    }
}
