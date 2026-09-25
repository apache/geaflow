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
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Performance benchmark for LMDB storage backend.
 *
 * <p>This benchmark tests various workload patterns and compares performance
 * characteristics of LMDB operations.
 */
public class LmdbPerformanceBenchmark {

    private Map<String, String> config = new HashMap<>();
    private IStoreBuilder builder;
    private Random random = new Random(12345);

    // Benchmark parameters
    private static final int SMALL_DATASET = 1000;
    private static final int MEDIUM_DATASET = 10000;
    private static final int LARGE_DATASET = 100000;
    private static final int WARMUP_ITERATIONS = 100;

    @BeforeClass
    public void setUp() {
        FileUtils.deleteQuietly(new File("/tmp/LmdbPerformanceBenchmark"));
        config.put(ExecutionConfigKeys.JOB_APP_NAME.getKey(), "LmdbPerformanceBenchmark");
        config.put(FileConfigKeys.PERSISTENT_TYPE.getKey(), "LOCAL");
        config.put(FileConfigKeys.ROOT.getKey(), "/tmp/LmdbPerformanceBenchmark");
        config.put(JOB_MAX_PARALLEL.getKey(), "1");
        config.put(LmdbConfigKeys.LMDB_MAP_SIZE.getKey(), "1073741824"); // 1GB for benchmarks
        config.put(LmdbConfigKeys.LMDB_SYNC_MODE.getKey(), "NO_SYNC"); // Fast mode for benchmarks
        builder = new LmdbStoreBuilder();
    }

    @Test
    public void benchmarkSequentialWrites() {
        System.out.println("\n=== Benchmark: Sequential Writes ===");
        benchmarkWrites(MEDIUM_DATASET, true, "sequential_write");
    }

    @Test
    public void benchmarkRandomWrites() {
        System.out.println("\n=== Benchmark: Random Writes ===");
        benchmarkWrites(MEDIUM_DATASET, false, "random_write");
    }

    @Test
    public void benchmarkSequentialReads() {
        System.out.println("\n=== Benchmark: Sequential Reads ===");
        benchmarkReads(MEDIUM_DATASET, true, "sequential_read");
    }

    @Test
    public void benchmarkRandomReads() {
        System.out.println("\n=== Benchmark: Random Reads ===");
        benchmarkReads(MEDIUM_DATASET, false, "random_read");
    }

    @Test
    public void benchmarkMixedWorkload() {
        System.out.println("\n=== Benchmark: Mixed Workload (70% Read, 30% Write) ===");
        benchmarkMixed(MEDIUM_DATASET, "mixed_workload");
    }

    @Test
    public void benchmarkBatchWrites() {
        System.out.println("\n=== Benchmark: Batch Writes ===");
        benchmarkBatchOperations(MEDIUM_DATASET, "batch_write");
    }

    @Test
    public void benchmarkLargeDataset() {
        System.out.println("\n=== Benchmark: Large Dataset Performance ===");
        benchmarkScalability(LARGE_DATASET, "large_dataset");
    }

    @Test
    public void benchmarkCheckpointPerformance() {
        System.out.println("\n=== Benchmark: Checkpoint Performance ===");
        benchmarkCheckpoint(SMALL_DATASET, "checkpoint");
    }

    private void benchmarkWrites(int count, boolean sequential, String testName) {
        Configuration configuration = new Configuration(config);
        IKVStatefulStore<Integer, String> kvStore = (IKVStatefulStore<Integer, String>) builder.getStore(
            DataModel.KV, configuration);
        StoreContext storeContext = new StoreContext(testName).withConfig(configuration);
        storeContext.withKeySerializer(new DefaultKVSerializer<>(Integer.class, String.class));

        kvStore.init(storeContext);

        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            kvStore.put(i, "warmup_" + i);
        }
        kvStore.flush();

        // Benchmark
        long startTime = System.nanoTime();
        for (int i = 0; i < count; i++) {
            int key = sequential ? i : random.nextInt(count);
            kvStore.put(key, generateValue(100));
        }
        kvStore.flush();
        long endTime = System.nanoTime();

        double durationMs = (endTime - startTime) / 1_000_000.0;
        double throughput = count / (durationMs / 1000.0);
        double avgLatencyUs = (durationMs * 1000.0) / count;

        System.out.printf("Operations: %d%n", count);
        System.out.printf("Duration: %.2f ms%n", durationMs);
        System.out.printf("Throughput: %.2f ops/sec%n", throughput);
        System.out.printf("Avg Latency: %.2f μs%n", avgLatencyUs);

        kvStore.close();
        kvStore.drop();
    }

    private void benchmarkReads(int count, boolean sequential, String testName) {
        Configuration configuration = new Configuration(config);
        IKVStatefulStore<Integer, String> kvStore = (IKVStatefulStore<Integer, String>) builder.getStore(
            DataModel.KV, configuration);
        StoreContext storeContext = new StoreContext(testName).withConfig(configuration);
        storeContext.withKeySerializer(new DefaultKVSerializer<>(Integer.class, String.class));

        kvStore.init(storeContext);

        // Insert data
        for (int i = 0; i < count; i++) {
            kvStore.put(i, generateValue(100));
        }
        kvStore.flush();

        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            kvStore.get(i);
        }

        // Benchmark
        long startTime = System.nanoTime();
        for (int i = 0; i < count; i++) {
            int key = sequential ? i : random.nextInt(count);
            kvStore.get(key);
        }
        long endTime = System.nanoTime();

        double durationMs = (endTime - startTime) / 1_000_000.0;
        double throughput = count / (durationMs / 1000.0);
        double avgLatencyUs = (durationMs * 1000.0) / count;

        System.out.printf("Operations: %d%n", count);
        System.out.printf("Duration: %.2f ms%n", durationMs);
        System.out.printf("Throughput: %.2f ops/sec%n", throughput);
        System.out.printf("Avg Latency: %.2f μs%n", avgLatencyUs);

        kvStore.close();
        kvStore.drop();
    }

    private void benchmarkMixed(int count, String testName) {
        Configuration configuration = new Configuration(config);
        IKVStatefulStore<Integer, String> kvStore = (IKVStatefulStore<Integer, String>) builder.getStore(
            DataModel.KV, configuration);
        StoreContext storeContext = new StoreContext(testName).withConfig(configuration);
        storeContext.withKeySerializer(new DefaultKVSerializer<>(Integer.class, String.class));

        kvStore.init(storeContext);

        // Insert initial data
        for (int i = 0; i < count; i++) {
            kvStore.put(i, generateValue(100));
        }
        kvStore.flush();

        // Mixed workload: 70% reads, 30% writes
        long startTime = System.nanoTime();
        int reads = 0;
        int writes = 0;
        for (int i = 0; i < count; i++) {
            if (random.nextDouble() < 0.7) {
                // Read
                kvStore.get(random.nextInt(count));
                reads++;
            } else {
                // Write
                kvStore.put(random.nextInt(count), generateValue(100));
                writes++;
            }
        }
        kvStore.flush();
        long endTime = System.nanoTime();

        double durationMs = (endTime - startTime) / 1_000_000.0;
        double throughput = count / (durationMs / 1000.0);

        System.out.printf("Total Operations: %d (Reads: %d, Writes: %d)%n", count, reads, writes);
        System.out.printf("Duration: %.2f ms%n", durationMs);
        System.out.printf("Throughput: %.2f ops/sec%n", throughput);

        kvStore.close();
        kvStore.drop();
    }

    private void benchmarkBatchOperations(int count, String testName) {
        Configuration configuration = new Configuration(config);
        IKVStatefulStore<Integer, String> kvStore = (IKVStatefulStore<Integer, String>) builder.getStore(
            DataModel.KV, configuration);
        StoreContext storeContext = new StoreContext(testName).withConfig(configuration);
        storeContext.withKeySerializer(new DefaultKVSerializer<>(Integer.class, String.class));

        kvStore.init(storeContext);

        int batchSize = 1000;
        int batches = count / batchSize;

        long startTime = System.nanoTime();
        for (int batch = 0; batch < batches; batch++) {
            for (int i = 0; i < batchSize; i++) {
                kvStore.put(batch * batchSize + i, generateValue(100));
            }
            kvStore.flush();
        }
        long endTime = System.nanoTime();

        double durationMs = (endTime - startTime) / 1_000_000.0;
        double throughput = count / (durationMs / 1000.0);

        System.out.printf("Operations: %d (Batch size: %d, Batches: %d)%n", count, batchSize, batches);
        System.out.printf("Duration: %.2f ms%n", durationMs);
        System.out.printf("Throughput: %.2f ops/sec%n", throughput);

        kvStore.close();
        kvStore.drop();
    }

    private void benchmarkScalability(int count, String testName) {
        Configuration configuration = new Configuration(config);
        IKVStatefulStore<Integer, String> kvStore = (IKVStatefulStore<Integer, String>) builder.getStore(
            DataModel.KV, configuration);
        StoreContext storeContext = new StoreContext(testName).withConfig(configuration);
        storeContext.withKeySerializer(new DefaultKVSerializer<>(Integer.class, String.class));

        kvStore.init(storeContext);

        System.out.println("Inserting large dataset...");
        long startTime = System.nanoTime();
        for (int i = 0; i < count; i++) {
            kvStore.put(i, generateValue(100));
            if (i % 10000 == 0) {
                kvStore.flush();
            }
        }
        kvStore.flush();
        long endTime = System.nanoTime();

        double durationMs = (endTime - startTime) / 1_000_000.0;
        System.out.printf("Insert Duration: %.2f ms%n", durationMs);
        System.out.printf("Insert Throughput: %.2f ops/sec%n", count / (durationMs / 1000.0));

        // Random reads on large dataset
        System.out.println("Random reads on large dataset...");
        int readCount = 10000;
        startTime = System.nanoTime();
        for (int i = 0; i < readCount; i++) {
            kvStore.get(random.nextInt(count));
        }
        endTime = System.nanoTime();

        durationMs = (endTime - startTime) / 1_000_000.0;
        System.out.printf("Read Duration: %.2f ms%n", durationMs);
        System.out.printf("Read Throughput: %.2f ops/sec%n", readCount / (durationMs / 1000.0));
        System.out.printf("Avg Read Latency: %.2f μs%n", (durationMs * 1000.0) / readCount);

        kvStore.close();
        kvStore.drop();
    }

    private void benchmarkCheckpoint(int count, String testName) {
        Configuration configuration = new Configuration(config);
        IKVStatefulStore<Integer, String> kvStore = (IKVStatefulStore<Integer, String>) builder.getStore(
            DataModel.KV, configuration);
        StoreContext storeContext = new StoreContext(testName).withConfig(configuration);
        storeContext.withKeySerializer(new DefaultKVSerializer<>(Integer.class, String.class));

        kvStore.init(storeContext);

        // Insert data
        for (int i = 0; i < count; i++) {
            kvStore.put(i, generateValue(100));
        }
        kvStore.flush();

        // Benchmark checkpoint creation
        long startTime = System.nanoTime();
        kvStore.archive(1);
        long endTime = System.nanoTime();

        double durationMs = (endTime - startTime) / 1_000_000.0;
        System.out.printf("Checkpoint Creation Duration: %.2f ms%n", durationMs);

        // Benchmark recovery
        kvStore.drop();
        kvStore.init(storeContext);

        startTime = System.nanoTime();
        kvStore.recovery(1);
        endTime = System.nanoTime();

        durationMs = (endTime - startTime) / 1_000_000.0;
        System.out.printf("Recovery Duration: %.2f ms%n", durationMs);

        kvStore.close();
        kvStore.drop();
    }

    private String generateValue(int length) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append((char) ('a' + random.nextInt(26)));
        }
        return sb.toString();
    }

    @AfterClass
    public void tearDown() {
        FileUtils.deleteQuietly(new File("/tmp/LmdbPerformanceBenchmark"));
    }
}
