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
import org.apache.geaflow.state.StoreType;
import org.apache.geaflow.state.serializer.DefaultKVSerializer;
import org.apache.geaflow.store.IStoreBuilder;
import org.apache.geaflow.store.api.StoreBuilderFactory;
import org.apache.geaflow.store.api.key.IKVStatefulStore;
import org.apache.geaflow.store.context.StoreContext;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class LmdbStoreBuilderTest {

    Map<String, String> config = new HashMap<>();

    @BeforeClass
    public void setUp() {
        FileUtils.deleteQuietly(new File("/tmp/LmdbStoreBuilderTest"));
        config.put(ExecutionConfigKeys.JOB_APP_NAME.getKey(), "LmdbStoreBuilderTest");
        config.put(FileConfigKeys.PERSISTENT_TYPE.getKey(), "LOCAL");
        config.put(FileConfigKeys.ROOT.getKey(), "/tmp/LmdbStoreBuilderTest");
        config.put(JOB_MAX_PARALLEL.getKey(), "1");
        // LMDB specific configuration
        config.put(LmdbConfigKeys.LMDB_MAP_SIZE.getKey(), "10485760"); // 10MB for tests
    }

    @Test
    public void testKV() {
        IStoreBuilder builder = StoreBuilderFactory.build(StoreType.LMDB.name());
        Configuration configuration = new Configuration(config);
        IKVStatefulStore<String, String> kvStore = (IKVStatefulStore<String, String>) builder.getStore(
            DataModel.KV, configuration);
        StoreContext storeContext = new StoreContext("lmdb_kv").withConfig(configuration);
        storeContext.withKeySerializer(new DefaultKVSerializer<>(String.class, String.class));

        kvStore.init(storeContext);
        kvStore.put("hello", "world");
        kvStore.put("foo", "bar");
        kvStore.flush();

        Assert.assertEquals(kvStore.get("hello"), "world");
        Assert.assertEquals(kvStore.get("foo"), "bar");

        kvStore.archive(1);
        kvStore.drop();

        kvStore = (IKVStatefulStore<String, String>) builder.getStore(DataModel.KV, configuration);
        kvStore.init(storeContext);
        kvStore.recovery(1);

        Assert.assertEquals(kvStore.get("hello"), "world");
        Assert.assertEquals(kvStore.get("foo"), "bar");
    }

    @Test
    public void testFO() {
        IStoreBuilder builder = StoreBuilderFactory.build(StoreType.LMDB.name());
        Configuration configuration = new Configuration(config);
        KVLmdbStore<String, String> kvStore =
            (KVLmdbStore<String, String>) builder.getStore(
                DataModel.KV, configuration);
        StoreContext storeContext = new StoreContext("lmdb_kv").withConfig(configuration);
        storeContext.withKeySerializer(new DefaultKVSerializer<>(String.class, String.class));
        kvStore.init(storeContext);
        Assert.assertEquals(kvStore.recoveryLatest(), -1);
        for (int i = 1; i < 10; i++) {
            kvStore.put("hello", "world" + i);
            kvStore.put("foo", "bar" + i);
            kvStore.flush();
            kvStore.archive(i);
        }
        kvStore.close();
        kvStore.drop();
        kvStore = (KVLmdbStore<String, String>) builder.getStore(DataModel.KV,
            configuration);
        kvStore.init(storeContext);
        kvStore.recoveryLatest();
        Assert.assertEquals(kvStore.get("hello"), "world" + 9);
        Assert.assertEquals(kvStore.get("foo"), "bar" + 9);
        kvStore.close();
        kvStore.drop();
        FileUtils.deleteQuietly(new File("/tmp/LmdbStoreBuilderTest/LmdbStoreBuilderTest"
            + "/lmdb_kv/0/meta.9/_commit"));
        kvStore = (KVLmdbStore<String, String>) builder.getStore(DataModel.KV,
            configuration);
        kvStore.init(storeContext);
        kvStore.recoveryLatest();
        Assert.assertEquals(kvStore.get("hello"), "world" + 8);
        Assert.assertEquals(kvStore.get("foo"), "bar" + 8);
        kvStore.close();
        kvStore.drop();
    }

    @AfterMethod
    public void tearUp() {
        FileUtils.deleteQuietly(new File("/tmp/LmdbStoreBuilderTest"));
    }
}
