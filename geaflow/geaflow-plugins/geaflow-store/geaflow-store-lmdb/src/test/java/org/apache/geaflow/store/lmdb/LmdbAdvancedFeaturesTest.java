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
import org.apache.commons.io.FileUtils;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.file.FileConfigKeys;
import org.apache.geaflow.state.DataModel;
import org.apache.geaflow.state.serializer.DefaultKVSerializer;
import org.apache.geaflow.store.IStoreBuilder;
import org.apache.geaflow.store.context.StoreContext;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class LmdbAdvancedFeaturesTest {

    private Map<String, String> config = new HashMap<>();
    private IStoreBuilder builder;

    @BeforeClass
    public void setUp() {
        FileUtils.deleteQuietly(new File("/tmp/LmdbAdvancedFeaturesTest"));
        config.put(ExecutionConfigKeys.JOB_APP_NAME.getKey(), "LmdbAdvancedFeaturesTest");
        config.put(FileConfigKeys.PERSISTENT_TYPE.getKey(), "LOCAL");
        config.put(FileConfigKeys.ROOT.getKey(), "/tmp/LmdbAdvancedFeaturesTest");
        config.put(JOB_MAX_PARALLEL.getKey(), "1");
        config.put(LmdbConfigKeys.LMDB_MAP_SIZE.getKey(), "10485760"); // 10MB for tests
        builder = new LmdbStoreBuilder();
    }

    @Test
    public void testMapSizeMonitoring() {
        Configuration configuration = new Configuration(config);
        KVLmdbStore<String, String> kvStore = (KVLmdbStore<String, String>) builder.getStore(
            DataModel.KV, configuration);
        StoreContext storeContext = new StoreContext("test_map_size").withConfig(configuration);
        storeContext.withKeySerializer(new DefaultKVSerializer<>(String.class, String.class));

        kvStore.init(storeContext);

        // Insert data
        for (int i = 0; i < 100; i++) {
            kvStore.put("key" + i, "value" + i);
        }
        kvStore.flush();

        // Trigger map size check
        BaseLmdbStore baseStore = (BaseLmdbStore) kvStore;
        LmdbClient client = baseStore.getLmdbClient();
        client.checkMapSizeUtilization();

        kvStore.close();
        kvStore.drop();
    }

    @Test
    public void testDatabaseStatistics() {
        Configuration configuration = new Configuration(config);
        KVLmdbStore<String, String> kvStore = (KVLmdbStore<String, String>) builder.getStore(
            DataModel.KV, configuration);
        StoreContext storeContext = new StoreContext("test_stats").withConfig(configuration);
        storeContext.withKeySerializer(new DefaultKVSerializer<>(String.class, String.class));

        kvStore.init(storeContext);

        // Insert data
        for (int i = 0; i < 100; i++) {
            kvStore.put("key" + i, "value_" + i);
        }
        kvStore.flush();

        // Get statistics
        BaseLmdbStore baseStore = (BaseLmdbStore) kvStore;
        LmdbClient client = baseStore.getLmdbClient();
        Map<String, LmdbClient.DatabaseStats> stats = client.getDatabaseStats();

        Assert.assertNotNull(stats);
        Assert.assertFalse(stats.isEmpty());

        // Verify statistics for default database
        LmdbClient.DatabaseStats defaultStats = stats.get(LmdbConfigKeys.DEFAULT_DB);
        Assert.assertNotNull(defaultStats);
        Assert.assertTrue(defaultStats.entries >= 100);
        Assert.assertTrue(defaultStats.depth > 0);

        kvStore.close();
        kvStore.drop();
    }

    @Test
    public void testPeriodicMapSizeCheck() {
        Configuration configuration = new Configuration(config);
        KVLmdbStore<String, String> kvStore = (KVLmdbStore<String, String>) builder.getStore(
            DataModel.KV, configuration);
        StoreContext storeContext = new StoreContext("test_periodic").withConfig(configuration);
        storeContext.withKeySerializer(new DefaultKVSerializer<>(String.class, String.class));

        kvStore.init(storeContext);

        // Perform 110 flushes to trigger periodic check (interval is 100)
        for (int i = 0; i < 110; i++) {
            kvStore.put("key" + i, "value" + i);
            kvStore.flush();
        }

        // Verify map size check was triggered
        BaseLmdbStore baseStore = (BaseLmdbStore) kvStore;
        LmdbClient client = baseStore.getLmdbClient();
        client.checkMapSizeUtilization();

        kvStore.close();
        kvStore.drop();
    }

    @Test
    public void testTransactionManagement() {
        Configuration configuration = new Configuration(config);
        KVLmdbStore<String, String> kvStore = (KVLmdbStore<String, String>) builder.getStore(
            DataModel.KV, configuration);
        StoreContext storeContext = new StoreContext("test_txn").withConfig(configuration);
        storeContext.withKeySerializer(new DefaultKVSerializer<>(String.class, String.class));

        kvStore.init(storeContext);

        // Test write transaction
        kvStore.put("key1", "value1");
        kvStore.put("key2", "value2");
        kvStore.flush();

        Assert.assertEquals(kvStore.get("key1"), "value1");
        Assert.assertEquals(kvStore.get("key2"), "value2");

        // Test read after write
        kvStore.put("key3", "value3");
        kvStore.flush();

        Assert.assertEquals(kvStore.get("key3"), "value3");

        kvStore.close();
        kvStore.drop();
    }

    @Test
    public void testConcurrentReads() {
        Configuration configuration = new Configuration(config);
        KVLmdbStore<String, String> kvStore = (KVLmdbStore<String, String>) builder.getStore(
            DataModel.KV, configuration);
        StoreContext storeContext = new StoreContext("test_concurrent").withConfig(configuration);
        storeContext.withKeySerializer(new DefaultKVSerializer<>(String.class, String.class));

        kvStore.init(storeContext);

        // Insert data
        for (int i = 0; i < 10; i++) {
            kvStore.put("key" + i, "value" + i);
        }
        kvStore.flush();

        // Perform multiple reads (simulating concurrent access)
        for (int i = 0; i < 10; i++) {
            String value = kvStore.get("key" + i);
            Assert.assertEquals(value, "value" + i);
        }

        kvStore.close();
        kvStore.drop();
    }

    @AfterMethod
    public void tearDown() {
        FileUtils.deleteQuietly(new File("/tmp/LmdbAdvancedFeaturesTest"));
    }
}
