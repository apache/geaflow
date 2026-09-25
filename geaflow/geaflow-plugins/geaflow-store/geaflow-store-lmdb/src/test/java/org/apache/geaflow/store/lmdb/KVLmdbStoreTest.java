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
import org.apache.geaflow.store.api.key.IKVStatefulStore;
import org.apache.geaflow.store.context.StoreContext;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class KVLmdbStoreTest {

    private Map<String, String> config = new HashMap<>();
    private IStoreBuilder builder;

    @BeforeClass
    public void setUp() {
        FileUtils.deleteQuietly(new File("/tmp/KVLmdbStoreTest"));
        config.put(ExecutionConfigKeys.JOB_APP_NAME.getKey(), "KVLmdbStoreTest");
        config.put(FileConfigKeys.PERSISTENT_TYPE.getKey(), "LOCAL");
        config.put(FileConfigKeys.ROOT.getKey(), "/tmp/KVLmdbStoreTest");
        config.put(JOB_MAX_PARALLEL.getKey(), "1");
        config.put(LmdbConfigKeys.LMDB_MAP_SIZE.getKey(), "10485760"); // 10MB for tests
        builder = new LmdbStoreBuilder();
    }

    @Test
    public void testBasicOperations() {
        Configuration configuration = new Configuration(config);
        IKVStatefulStore<String, String> kvStore = (IKVStatefulStore<String, String>) builder.getStore(
            DataModel.KV, configuration);
        StoreContext storeContext = new StoreContext("test_basic").withConfig(configuration);
        storeContext.withKeySerializer(new DefaultKVSerializer<>(String.class, String.class));

        kvStore.init(storeContext);

        // Test put and get
        kvStore.put("key1", "value1");
        kvStore.put("key2", "value2");
        kvStore.flush();

        Assert.assertEquals(kvStore.get("key1"), "value1");
        Assert.assertEquals(kvStore.get("key2"), "value2");
        Assert.assertNull(kvStore.get("nonexistent"));

        // Test update
        kvStore.put("key1", "updated_value1");
        kvStore.flush();
        Assert.assertEquals(kvStore.get("key1"), "updated_value1");

        kvStore.close();
        kvStore.drop();
    }

    @Test
    public void testDelete() {
        Configuration configuration = new Configuration(config);
        IKVStatefulStore<String, String> kvStore = (IKVStatefulStore<String, String>) builder.getStore(
            DataModel.KV, configuration);
        StoreContext storeContext = new StoreContext("test_delete").withConfig(configuration);
        storeContext.withKeySerializer(new DefaultKVSerializer<>(String.class, String.class));

        kvStore.init(storeContext);

        kvStore.put("key1", "value1");
        kvStore.put("key2", "value2");
        kvStore.flush();

        Assert.assertEquals(kvStore.get("key1"), "value1");

        kvStore.remove("key1");
        kvStore.flush();

        Assert.assertNull(kvStore.get("key1"));
        Assert.assertEquals(kvStore.get("key2"), "value2");

        kvStore.close();
        kvStore.drop();
    }

    @Test
    public void testCheckpointAndRecovery() {
        Configuration configuration = new Configuration(config);
        IKVStatefulStore<String, String> kvStore = (IKVStatefulStore<String, String>) builder.getStore(
            DataModel.KV, configuration);
        StoreContext storeContext = new StoreContext("test_checkpoint").withConfig(configuration);
        storeContext.withKeySerializer(new DefaultKVSerializer<>(String.class, String.class));

        kvStore.init(storeContext);

        // Write data
        kvStore.put("checkpoint_key1", "checkpoint_value1");
        kvStore.put("checkpoint_key2", "checkpoint_value2");
        kvStore.flush();

        // Create checkpoint
        kvStore.archive(1L);

        // Verify data still accessible
        Assert.assertEquals(kvStore.get("checkpoint_key1"), "checkpoint_value1");

        // Write more data
        kvStore.put("checkpoint_key3", "checkpoint_value3");
        kvStore.flush();

        // Drop and recover
        kvStore.drop();

        IKVStatefulStore<String, String> recoveredStore = (IKVStatefulStore<String, String>) builder.getStore(
            DataModel.KV, configuration);
        recoveredStore.init(storeContext);
        recoveredStore.recovery(1L);

        // Verify checkpoint data recovered
        Assert.assertEquals(recoveredStore.get("checkpoint_key1"), "checkpoint_value1");
        Assert.assertEquals(recoveredStore.get("checkpoint_key2"), "checkpoint_value2");
        // Data after checkpoint should not exist
        Assert.assertNull(recoveredStore.get("checkpoint_key3"));

        recoveredStore.close();
        recoveredStore.drop();
    }

    @Test
    public void testMultipleCheckpoints() {
        Configuration configuration = new Configuration(config);
        KVLmdbStore<String, String> kvStore = (KVLmdbStore<String, String>) builder.getStore(
            DataModel.KV, configuration);
        StoreContext storeContext = new StoreContext("test_multi_checkpoint").withConfig(configuration);
        storeContext.withKeySerializer(new DefaultKVSerializer<>(String.class, String.class));

        kvStore.init(storeContext);

        // Create multiple checkpoints
        for (int i = 1; i <= 5; i++) {
            kvStore.put("key" + i, "value" + i);
            kvStore.flush();
            kvStore.archive(i);
        }

        kvStore.drop();

        // Recover from checkpoint 3
        kvStore = (KVLmdbStore<String, String>) builder.getStore(DataModel.KV, configuration);
        kvStore.init(storeContext);
        kvStore.recovery(3);

        Assert.assertEquals(kvStore.get("key1"), "value1");
        Assert.assertEquals(kvStore.get("key2"), "value2");
        Assert.assertEquals(kvStore.get("key3"), "value3");

        kvStore.close();
        kvStore.drop();
    }

    @Test
    public void testLargeDataSet() {
        Configuration configuration = new Configuration(config);
        IKVStatefulStore<Integer, String> kvStore = (IKVStatefulStore<Integer, String>) builder.getStore(
            DataModel.KV, configuration);
        StoreContext storeContext = new StoreContext("test_large").withConfig(configuration);
        storeContext.withKeySerializer(new DefaultKVSerializer<>(Integer.class, String.class));

        kvStore.init(storeContext);

        // Write 1000 entries
        int count = 1000;
        for (int i = 0; i < count; i++) {
            kvStore.put(i, "value_" + i);
        }
        kvStore.flush();

        // Verify random entries
        Assert.assertEquals(kvStore.get(0), "value_0");
        Assert.assertEquals(kvStore.get(500), "value_500");
        Assert.assertEquals(kvStore.get(999), "value_999");

        kvStore.close();
        kvStore.drop();
    }

    @AfterMethod
    public void tearDown() {
        FileUtils.deleteQuietly(new File("/tmp/KVLmdbStoreTest"));
    }
}
