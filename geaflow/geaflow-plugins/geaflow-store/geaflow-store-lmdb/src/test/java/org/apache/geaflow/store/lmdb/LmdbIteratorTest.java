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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.common.iterator.CloseableIterator;
import org.apache.geaflow.common.tuple.Tuple;
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

public class LmdbIteratorTest {

    private Map<String, String> config = new HashMap<>();
    private IStoreBuilder builder;

    @BeforeClass
    public void setUp() {
        FileUtils.deleteQuietly(new File("/tmp/LmdbIteratorTest"));
        config.put(ExecutionConfigKeys.JOB_APP_NAME.getKey(), "LmdbIteratorTest");
        config.put(FileConfigKeys.PERSISTENT_TYPE.getKey(), "LOCAL");
        config.put(FileConfigKeys.ROOT.getKey(), "/tmp/LmdbIteratorTest");
        config.put(JOB_MAX_PARALLEL.getKey(), "1");
        config.put(LmdbConfigKeys.LMDB_MAP_SIZE.getKey(), "10485760"); // 10MB for tests
        builder = new LmdbStoreBuilder();
    }

    @Test
    public void testBasicIteration() {
        Configuration configuration = new Configuration(config);
        KVLmdbStore<String, String> kvStore = (KVLmdbStore<String, String>) builder.getStore(
            DataModel.KV, configuration);
        StoreContext storeContext = new StoreContext("test_iter_basic").withConfig(configuration);
        storeContext.withKeySerializer(new DefaultKVSerializer<>(String.class, String.class));

        kvStore.init(storeContext);

        // Insert test data
        for (int i = 0; i < 10; i++) {
            kvStore.put("key" + i, "value" + i);
        }
        kvStore.flush();

        // Test iteration
        CloseableIterator<Tuple<String, String>> iterator = kvStore.getKeyValueIterator();
        List<Tuple<String, String>> results = new ArrayList<>();
        while (iterator.hasNext()) {
            results.add(iterator.next());
        }
        iterator.close();

        Assert.assertEquals(results.size(), 10);

        kvStore.close();
        kvStore.drop();
    }

    @Test
    public void testPrefixIteration() {
        Configuration configuration = new Configuration(config);
        KVLmdbStore<String, String> kvStore = (KVLmdbStore<String, String>) builder.getStore(
            DataModel.KV, configuration);
        StoreContext storeContext = new StoreContext("test_iter_prefix").withConfig(configuration);
        storeContext.withKeySerializer(new DefaultKVSerializer<>(String.class, String.class));

        kvStore.init(storeContext);

        // Insert test data with different prefixes
        for (int i = 0; i < 5; i++) {
            kvStore.put("prefix1_key" + i, "value" + i);
        }
        for (int i = 0; i < 5; i++) {
            kvStore.put("prefix2_key" + i, "value" + i);
        }
        kvStore.flush();

        // Test prefix iteration
        CloseableIterator<Tuple<String, String>> iterator = kvStore.getKeyValueIterator();
        List<Tuple<String, String>> results = new ArrayList<>();
        while (iterator.hasNext()) {
            Tuple<String, String> entry = iterator.next();
            if (entry.f0.startsWith("prefix1_")) {
                results.add(entry);
            }
        }
        iterator.close();

        Assert.assertEquals(results.size(), 5);

        kvStore.close();
        kvStore.drop();
    }

    @Test
    public void testEmptyIteration() {
        Configuration configuration = new Configuration(config);
        KVLmdbStore<String, String> kvStore = (KVLmdbStore<String, String>) builder.getStore(
            DataModel.KV, configuration);
        StoreContext storeContext = new StoreContext("test_iter_empty").withConfig(configuration);
        storeContext.withKeySerializer(new DefaultKVSerializer<>(String.class, String.class));

        kvStore.init(storeContext);
        kvStore.flush();

        // Test empty iteration
        CloseableIterator<Tuple<String, String>> iterator = kvStore.getKeyValueIterator();
        Assert.assertFalse(iterator.hasNext());
        iterator.close();

        kvStore.close();
        kvStore.drop();
    }

    @Test
    public void testLargeIteration() {
        Configuration configuration = new Configuration(config);
        KVLmdbStore<Integer, String> kvStore = (KVLmdbStore<Integer, String>) builder.getStore(
            DataModel.KV, configuration);
        StoreContext storeContext = new StoreContext("test_iter_large").withConfig(configuration);
        storeContext.withKeySerializer(new DefaultKVSerializer<>(Integer.class, String.class));

        kvStore.init(storeContext);

        // Insert 1000 entries
        int count = 1000;
        for (int i = 0; i < count; i++) {
            kvStore.put(i, "value_" + i);
        }
        kvStore.flush();

        // Iterate and count
        CloseableIterator<Tuple<Integer, String>> iterator = kvStore.getKeyValueIterator();
        int iteratedCount = 0;
        while (iterator.hasNext()) {
            iterator.next();
            iteratedCount++;
        }
        iterator.close();

        Assert.assertEquals(iteratedCount, count);

        kvStore.close();
        kvStore.drop();
    }

    @Test
    public void testIteratorClose() {
        Configuration configuration = new Configuration(config);
        KVLmdbStore<String, String> kvStore = (KVLmdbStore<String, String>) builder.getStore(
            DataModel.KV, configuration);
        StoreContext storeContext = new StoreContext("test_iter_close").withConfig(configuration);
        storeContext.withKeySerializer(new DefaultKVSerializer<>(String.class, String.class));

        kvStore.init(storeContext);

        // Insert test data
        for (int i = 0; i < 10; i++) {
            kvStore.put("key" + i, "value" + i);
        }
        kvStore.flush();

        // Test early close
        CloseableIterator<Tuple<String, String>> iterator = kvStore.getKeyValueIterator();
        Assert.assertTrue(iterator.hasNext());
        iterator.next();
        iterator.close();

        // Verify multiple close is safe
        iterator.close();

        kvStore.close();
        kvStore.drop();
    }

    @AfterMethod
    public void tearDown() {
        FileUtils.deleteQuietly(new File("/tmp/LmdbIteratorTest"));
    }
}
