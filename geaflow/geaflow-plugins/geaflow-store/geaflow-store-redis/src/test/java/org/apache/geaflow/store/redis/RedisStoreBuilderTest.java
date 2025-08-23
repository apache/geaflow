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

package org.apache.geaflow.store.redis;

import com.github.fppt.jedismock.RedisServer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.state.DataModel;
import org.apache.geaflow.state.StoreType;
import org.apache.geaflow.state.serializer.DefaultKMapSerializer;
import org.apache.geaflow.state.serializer.DefaultKVSerializer;
import org.apache.geaflow.state.serializer.IKVSerializer;
import org.apache.geaflow.store.IStoreBuilder;
import org.apache.geaflow.store.api.StoreBuilderFactory;
import org.apache.geaflow.store.api.key.IKListStore;
import org.apache.geaflow.store.api.key.IKMapStore;
import org.apache.geaflow.store.api.key.IKVStore;
import org.apache.geaflow.store.context.StoreContext;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class RedisStoreBuilderTest {

    private RedisServer redisServer;

    @BeforeClass
    public void prepare() throws IOException {
        redisServer = RedisServer.newRedisServer().start();
    }

    @AfterClass
    public void tearUp() throws IOException {
        redisServer.stop();
    }

    @Test
    public void testMultiThread() throws ExecutionException, InterruptedException {
        IStoreBuilder builder = StoreBuilderFactory.build(StoreType.REDIS.name());
        IKVStore<String, String> kvStore = (IKVStore<String, String>) builder.getStore(DataModel.KV,
            new Configuration());
        Configuration configuration = new Configuration();
        configuration.put(RedisConfigKeys.REDIS_HOST, redisServer.getHost());
        configuration.put(RedisConfigKeys.REDIS_PORT, String.valueOf(redisServer.getBindPort()));
        StoreContext storeContext = new StoreContext("redis").withConfig(configuration);
        storeContext.withKeySerializer(new IKVSerializer<String, String>() {
            @Override
            public byte[] serializeKey(String key) {
                return key.getBytes();
            }

            @Override
            public String deserializeKey(byte[] array) {
                return new String(array);
            }

            @Override
            public byte[] serializeValue(String value) {
                return value.getBytes();
            }

            @Override
            public String deserializeValue(byte[] valueArray) {
                return new String(valueArray);
            }
        });
        kvStore.init(storeContext);

        ExecutorService executors = Executors.newFixedThreadPool(10);

        List<Future> futureList = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            final int index = i;
            Future future = executors.submit(() -> {
                for (int j = 0; j < 1000; j++) {
                    kvStore.put(index + "hello" + j, index + "world" + j);
                    Assert.assertEquals(kvStore.get(index + "hello" + j), index + "world" + j);
                }
            });
            futureList.add(future);
        }
        for (Future f : futureList) {
            f.get();
        }
    }

    @Test
    public void testKV() {
        IStoreBuilder builder = StoreBuilderFactory.build(StoreType.REDIS.name());
        IKVStore<String, String> kvStore = (IKVStore<String, String>) builder.getStore(DataModel.KV,
            new Configuration());

        Configuration configuration = new Configuration();
        configuration.put(RedisConfigKeys.REDIS_HOST, redisServer.getHost());
        configuration.put(RedisConfigKeys.REDIS_PORT, String.valueOf(redisServer.getBindPort()));
        StoreContext storeContext = new StoreContext("redis").withConfig(configuration);
        storeContext.withKeySerializer(new IKVSerializer<String, String>() {
            @Override
            public byte[] serializeKey(String key) {
                return key.getBytes();
            }

            @Override
            public String deserializeKey(byte[] array) {
                return new String(array);
            }

            @Override
            public byte[] serializeValue(String value) {
                return value.getBytes();
            }

            @Override
            public String deserializeValue(byte[] valueArray) {
                return new String(valueArray);
            }
        });

        kvStore.init(storeContext);
        kvStore.put("hello", "world");
        kvStore.put("foo", "bar");

        Assert.assertEquals(kvStore.get("hello"), "world");
        Assert.assertEquals(kvStore.get("foo"), "bar");

        kvStore.remove("foo");
        Assert.assertNull(kvStore.get("foo"));
    }

    @Test
    public void testKMap() {
        IStoreBuilder builder = StoreBuilderFactory.build(StoreType.REDIS.name());
        IKMapStore<String, String, String> kMapStore =
            (IKMapStore<String, String, String>) builder.getStore(
                DataModel.KMap, new Configuration());

        Configuration configuration = new Configuration();
        configuration.put(RedisConfigKeys.REDIS_HOST, redisServer.getHost());
        configuration.put(RedisConfigKeys.REDIS_PORT, String.valueOf(redisServer.getBindPort()));
        StoreContext storeContext = new StoreContext("redis").withConfig(configuration);
        storeContext.withKeySerializer(
            new DefaultKMapSerializer<>(String.class, String.class, String.class));
        kMapStore.init(storeContext);

        Map<String, String> map = new HashMap<>();
        map.put("hello", "world");
        map.put("hello1", "world1");

        kMapStore.add("hw", map);

        map.clear();
        map.put("foo", "bar");
        kMapStore.add("hw", map);
        kMapStore.add("hw", "bar", "foo");

        Assert.assertEquals(kMapStore.get("hw").size(), 4);
        Assert.assertEquals(kMapStore.get("hw", "foo", "bar"), Arrays.asList("bar", "foo"));

        kMapStore.remove("hw", "bar");
        Assert.assertEquals(kMapStore.get("hw").size(), 3);

        kMapStore.remove("hw");
        Assert.assertEquals(kMapStore.get("hw").size(), 0);
    }

    @Test
    public void testKList() {
        IStoreBuilder builder = StoreBuilderFactory.build(StoreType.REDIS.name());
        IKListStore<String, String> kListStore = (IKListStore<String, String>) builder.getStore(
            DataModel.KList, new Configuration());

        Configuration configuration = new Configuration();
        configuration.put(RedisConfigKeys.REDIS_HOST, redisServer.getHost());
        configuration.put(RedisConfigKeys.REDIS_PORT, String.valueOf(redisServer.getBindPort()));
        StoreContext storeContext = new StoreContext("redis").withConfig(configuration);
        storeContext.withKeySerializer(new DefaultKVSerializer<>(String.class, String.class));
        kListStore.init(storeContext);

        kListStore.add("hw", "foo", "bar");
        kListStore.add("hw", "hello");

        Assert.assertEquals(kListStore.get("hw").size(), 3);
        kListStore.remove("hw");
        Assert.assertEquals(kListStore.get("hw").size(), 0);
    }
}
