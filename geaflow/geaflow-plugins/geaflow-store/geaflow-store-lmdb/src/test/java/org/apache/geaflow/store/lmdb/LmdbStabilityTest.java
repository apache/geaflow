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

package org.apache.geaflow.store.lmdb;

import static org.apache.geaflow.common.config.keys.FrameworkConfigKeys.JOB_MAX_PARALLEL;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.file.FileConfigKeys;
import org.apache.geaflow.state.DataModel;
import org.apache.geaflow.state.serializer.DefaultKVSerializer;
import org.apache.geaflow.store.IStoreBuilder;
import org.apache.geaflow.store.api.key.IKVStatefulStore;
import org.apache.geaflow.store.context.StoreContext;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Stability tests for LMDB storage backend.
 *
 * <p>These tests verify long-running behavior, memory stability, and reliability
 * under various stress conditions.
 */
public class LmdbStabilityTest {

    private Map<String, String> config = new HashMap<>();
    private IStoreBuilder builder;
    private Random random = new Random(54321);

    @BeforeClass
    public void setUp() {
        FileUtils.deleteQuietly(new File("/tmp/LmdbStabilityTest"));
        config.put(ExecutionConfigKeys.JOB_APP_NAME.getKey(), "LmdbStabilityTest");
        config.put(FileConfigKeys.PERSISTENT_TYPE.getKey(), "LOCAL");
        config.put(FileConfigKeys.ROOT.getKey(), "/tmp/LmdbStabilityTest");
        config.put(JOB_MAX_PARALLEL.getKey(), "1");
        config.put(LmdbConfigKeys.LMDB_MAP_SIZE.getKey(), "2147483648"); // 2GB
        builder = new LmdbStoreBuilder();
    }

    @Test
    public void testLongRunningOperations() {
        System.out.println("\n=== Stability Test: Long Running Operations ===");
        Configuration configuration = new Configuration(config);
        IKVStatefulStore<Integer, String> kvStore = (IKVStatefulStore<Integer, String>) builder.getStore(
            DataModel.KV, configuration);
        StoreContext storeContext = new StoreContext("long_running").withConfig(configuration);
        storeContext.withKeySerializer(new DefaultKVSerializer<>(Integer.class, String.class));

        kvStore.init(storeContext);

        int iterations = 1000;
        int operationsPerIteration = 100;
        long startTime = System.currentTimeMillis();

        for (int iter = 0; iter < iterations; iter++) {
            // Mixed workload
            for (int i = 0; i < operationsPerIteration; i++) {
                int key = random.nextInt(10000);
                if (random.nextBoolean()) {
                    kvStore.put(key, "value_" + key);
                } else {
                    kvStore.get(key);
                }
            }
            kvStore.flush();

            if (iter % 100 == 0) {
                System.out.printf("Iteration %d/%d completed%n", iter, iterations);
            }
        }

        long duration = System.currentTimeMillis() - startTime;
        System.out.printf("Total operations: %d%n", iterations * operationsPerIteration);
        System.out.printf("Duration: %d ms%n", duration);
        System.out.println("Long running test completed successfully");

        kvStore.close();
        kvStore.drop();
    }

    @Test
    public void testRepeatedCheckpointRecovery() {
        System.out.println("\n=== Stability Test: Repeated Checkpoint/Recovery ===");
        Configuration configuration = new Configuration(config);
        IKVStatefulStore<Integer, String> kvStore = (IKVStatefulStore<Integer, String>) builder.getStore(
            DataModel.KV, configuration);
        StoreContext storeContext = new StoreContext("checkpoint_recovery").withConfig(configuration);
        storeContext.withKeySerializer(new DefaultKVSerializer<>(Integer.class, String.class));

        kvStore.init(storeContext);

        int cycles = 20;
        int dataSize = 100;

        for (int cycle = 0; cycle < cycles; cycle++) {
            // Write data
            for (int i = 0; i < dataSize; i++) {
                kvStore.put(i, "cycle_" + cycle + "_value_" + i);
            }
            kvStore.flush();

            // Create checkpoint
            kvStore.archive(cycle);

            // Verify data before drop
            for (int i = 0; i < dataSize; i++) {
                Assert.assertEquals(kvStore.get(i), "cycle_" + cycle + "_value_" + i);
            }

            // Drop and recover
            kvStore.drop();
            kvStore.init(storeContext);
            kvStore.recovery(cycle);

            // Verify data after recovery
            for (int i = 0; i < dataSize; i++) {
                Assert.assertEquals(kvStore.get(i), "cycle_" + cycle + "_value_" + i,
                    "Data mismatch at cycle " + cycle);
            }

            if (cycle % 5 == 0) {
                System.out.printf("Cycle %d/%d completed%n", cycle, cycles);
            }
        }

        System.out.println("Repeated checkpoint/recovery test completed successfully");

        kvStore.close();
        kvStore.drop();
    }

    @Test
    public void testMapSizeGrowth() {
        System.out.println("\n=== Stability Test: Map Size Growth ===");
        Configuration configuration = new Configuration(config);
        IKVStatefulStore<Integer, String> kvStore = (IKVStatefulStore<Integer, String>) builder.getStore(
            DataModel.KV, configuration);
        StoreContext storeContext = new StoreContext("map_size_growth").withConfig(configuration);
        storeContext.withKeySerializer(new DefaultKVSerializer<>(Integer.class, String.class));

        kvStore.init(storeContext);

        BaseLmdbStore baseStore = (BaseLmdbStore) kvStore;
        LmdbClient client = baseStore.getLmdbClient();

        // Insert data in batches and monitor size
        int batches = 10;
        int batchSize = 1000;

        for (int batch = 0; batch < batches; batch++) {
            for (int i = 0; i < batchSize; i++) {
                int key = batch * batchSize + i;
                kvStore.put(key, generateLargeValue(200));
            }
            kvStore.flush();

            // Check map size utilization
            client.checkMapSizeUtilization();

            // Get statistics
            Map<String, LmdbClient.DatabaseStats> stats = client.getDatabaseStats();
            LmdbClient.DatabaseStats defaultStats = stats.get(LmdbConfigKeys.DEFAULT_DB);

            System.out.printf("Batch %d: Entries=%d, Size=%d KB%n",
                batch, defaultStats.entries, defaultStats.sizeBytes / 1024);
        }

        System.out.println("Map size growth test completed successfully");

        kvStore.close();
        kvStore.drop();
    }

    @Test
    public void testConcurrentOperations() {
        System.out.println("\n=== Stability Test: Concurrent-like Operations ===");
        Configuration configuration = new Configuration(config);
        IKVStatefulStore<Integer, String> kvStore = (IKVStatefulStore<Integer, String>) builder.getStore(
            DataModel.KV, configuration);
        StoreContext storeContext = new StoreContext("concurrent").withConfig(configuration);
        storeContext.withKeySerializer(new DefaultKVSerializer<>(Integer.class, String.class));

        kvStore.init(storeContext);

        // Insert initial data
        int dataSize = 1000;
        for (int i = 0; i < dataSize; i++) {
            kvStore.put(i, "initial_" + i);
        }
        kvStore.flush();

        // Simulate concurrent-like access patterns
        int rounds = 100;
        for (int round = 0; round < rounds; round++) {
            // Multiple rapid reads
            for (int i = 0; i < 50; i++) {
                kvStore.get(random.nextInt(dataSize));
            }

            // Interspersed writes
            for (int i = 0; i < 10; i++) {
                kvStore.put(random.nextInt(dataSize), "updated_" + round + "_" + i);
            }

            kvStore.flush();
        }

        System.out.println("Concurrent-like operations test completed successfully");

        kvStore.close();
        kvStore.drop();
    }

    @Test
    public void testMemoryStability() {
        System.out.println("\n=== Stability Test: Memory Stability ===");
        Configuration configuration = new Configuration(config);
        IKVStatefulStore<Integer, String> kvStore = (IKVStatefulStore<Integer, String>) builder.getStore(
            DataModel.KV, configuration);
        StoreContext storeContext = new StoreContext("memory_stability").withConfig(configuration);
        storeContext.withKeySerializer(new DefaultKVSerializer<>(Integer.class, String.class));

        kvStore.init(storeContext);

        Runtime runtime = Runtime.getRuntime();
        long initialMemory = runtime.totalMemory() - runtime.freeMemory();

        // Continuous operations with periodic cleanup
        int cycles = 50;
        int operationsPerCycle = 200;

        for (int cycle = 0; cycle < cycles; cycle++) {
            // Write data
            for (int i = 0; i < operationsPerCycle; i++) {
                kvStore.put(i, generateLargeValue(500));
            }
            kvStore.flush();

            // Read data
            for (int i = 0; i < operationsPerCycle; i++) {
                kvStore.get(i);
            }

            // Periodic checkpoint to verify no memory leaks
            if (cycle % 10 == 0) {
                kvStore.archive(cycle);
                long currentMemory = runtime.totalMemory() - runtime.freeMemory();
                long memoryGrowth = (currentMemory - initialMemory) / 1024 / 1024;
                System.out.printf("Cycle %d: Memory growth = %d MB%n", cycle, memoryGrowth);
            }
        }

        System.out.println("Memory stability test completed successfully");

        kvStore.close();
        kvStore.drop();
    }

    @Test
    public void testLargeValueOperations() {
        System.out.println("\n=== Stability Test: Large Value Operations ===");
        Configuration configuration = new Configuration(config);
        IKVStatefulStore<Integer, String> kvStore = (IKVStatefulStore<Integer, String>) builder.getStore(
            DataModel.KV, configuration);
        StoreContext storeContext = new StoreContext("large_values").withConfig(configuration);
        storeContext.withKeySerializer(new DefaultKVSerializer<>(Integer.class, String.class));

        kvStore.init(storeContext);

        // Test with increasingly large values
        int[] valueSizes = {1024, 10240, 102400}; // 1KB, 10KB, 100KB
        int entriesPerSize = 10;

        for (int sizeIdx = 0; sizeIdx < valueSizes.length; sizeIdx++) {
            int valueSize = valueSizes[sizeIdx];
            System.out.printf("Testing with value size: %d bytes%n", valueSize);

            for (int i = 0; i < entriesPerSize; i++) {
                int key = sizeIdx * entriesPerSize + i;
                String value = generateLargeValue(valueSize);
                kvStore.put(key, value);
            }
            kvStore.flush();

            // Verify reads
            for (int i = 0; i < entriesPerSize; i++) {
                int key = sizeIdx * entriesPerSize + i;
                String value = kvStore.get(key);
                Assert.assertNotNull(value);
                Assert.assertEquals(value.length(), valueSize);
            }
        }

        System.out.println("Large value operations test completed successfully");

        kvStore.close();
        kvStore.drop();
    }

    private String generateLargeValue(int length) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append((char) ('a' + (i % 26)));
        }
        return sb.toString();
    }

    @AfterClass
    public void tearDown() {
        FileUtils.deleteQuietly(new File("/tmp/LmdbStabilityTest"));
    }
}
