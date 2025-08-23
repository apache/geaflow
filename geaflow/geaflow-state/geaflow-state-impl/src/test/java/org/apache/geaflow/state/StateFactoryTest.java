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

package org.apache.geaflow.state;

import com.google.common.collect.Iterators;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.io.FileUtils;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.common.thread.Executors;
import org.apache.geaflow.common.type.Types;
import org.apache.geaflow.common.utils.GsonUtil;
import org.apache.geaflow.file.FileConfigKeys;
import org.apache.geaflow.model.graph.edge.impl.ValueEdge;
import org.apache.geaflow.model.graph.meta.GraphMeta;
import org.apache.geaflow.model.graph.meta.GraphMetaType;
import org.apache.geaflow.model.graph.vertex.impl.ValueVertex;
import org.apache.geaflow.state.data.OneDegreeGraph;
import org.apache.geaflow.state.descriptor.GraphStateDescriptor;
import org.apache.geaflow.state.descriptor.KeyListStateDescriptor;
import org.apache.geaflow.state.descriptor.KeyMapStateDescriptor;
import org.apache.geaflow.state.descriptor.KeyValueStateDescriptor;
import org.apache.geaflow.state.manage.LoadOption;
import org.apache.geaflow.utils.keygroup.DefaultKeyGroupAssigner;
import org.apache.geaflow.utils.keygroup.KeyGroup;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class StateFactoryTest {

    private long ts;
    private String testName;

    @BeforeMethod
    public void setUp() {
        this.ts = System.currentTimeMillis();
        this.testName = "factory-test-" + ts;
    }

    @AfterMethod
    public void tearDown() throws IOException {
        FileUtils.deleteQuietly(new File("/tmp/" + testName));
    }

    @Test
    public void testSingleton() throws ExecutionException, InterruptedException {
        Map<String, String> config = new HashMap<>();
        Map<String, String> persistConfig = new HashMap<>();
        config.put(ExecutionConfigKeys.JOB_APP_NAME.getKey(), testName);
        config.put(FileConfigKeys.PERSISTENT_TYPE.getKey(), "LOCAL");
        config.put(FileConfigKeys.ROOT.getKey(), "/tmp/geaflow/chk/");
        config.put(FileConfigKeys.JSON_CONFIG.getKey(), GsonUtil.toJson(persistConfig));

        GraphMetaType tag = new GraphMetaType(Types.STRING, ValueVertex.class,
            String.class, ValueEdge.class, String.class);
        GraphStateDescriptor desc = GraphStateDescriptor.build(testName, StoreType.ROCKSDB.name());
        desc.withGraphMeta(new GraphMeta(tag)).withKeyGroup(new KeyGroup(0, 1))
            .withKeyGroupAssigner(new DefaultKeyGroupAssigner(2));
        desc.withSingleton();
        ExecutorService executors = Executors.getExecutorService(8, "testSingleton");
        List<Future<GraphState>> list = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            Future<GraphState> future = executors.submit(
                () -> StateFactory.buildGraphState(desc, new Configuration(config)));
            list.add(future);
        }
        GraphState graphState = list.get(0).get();
        for (int i = 1; i < 8; i++) {
            Assert.assertTrue(graphState == list.get(i).get());
        }

        graphState.manage().operate().setCheckpointId(1);
        for (int i = 0; i < 10000; i++) {
            graphState.staticGraph().V().add(new ValueVertex<>("hello" + i, "hello"));
            graphState.staticGraph().E().add(new ValueEdge<>("hello" + i, "world" + i, "hello" + i));
        }
        graphState.manage().operate().finish();
        graphState.manage().operate().archive();
        graphState.manage().operate().drop();

        final GraphState graphState2 = StateFactory.buildGraphState(desc, new Configuration(config));
        List<Future> list2 = new ArrayList<>();
        AtomicInteger count = new AtomicInteger(0);
        for (int i = 0; i < 8; i++) {
            final int keyGroupId = i % 2;
            list2.add(executors.submit(() -> {
                graphState2.manage().operate().load(LoadOption.of().withKeyGroup(new KeyGroup(keyGroupId, keyGroupId)).withCheckpointId(1L));
                int size = Iterators.size(graphState2.staticGraph().V().query(new KeyGroup(keyGroupId, keyGroupId)).idIterator());
                count.addAndGet(size);
            }));
        }
        for (Future f : list2) {
            f.get();
        }
        Assert.assertEquals(count.get(), 40000);
        graphState2.manage().operate().drop();
        executors.shutdown();

        FileUtils.deleteQuietly(new File("/tmp/geaflow/chk"));
    }

    @Test
    public void testGraphState() throws Exception {
        Map<String, String> config = new HashMap<>();

        GraphMetaType tag = new GraphMetaType(Types.STRING, ValueVertex.class,
            String.class, ValueEdge.class, String.class);
        GraphStateDescriptor desc = GraphStateDescriptor.build(testName, StoreType.MEMORY.name());
        desc.withGraphMeta(new GraphMeta(tag)).withKeyGroup(new KeyGroup(0, 0))
            .withKeyGroupAssigner(new DefaultKeyGroupAssigner(1));
        GraphState<String, String, String> graphState = StateFactory.buildGraphState(desc,
            new Configuration(config));

        graphState.manage().operate().setCheckpointId(1);
        for (int i = 0; i < 100; i++) {
            graphState.staticGraph().V().add(new ValueVertex<>("hello" + ts, "hello"));
            graphState.staticGraph().E().add(new ValueEdge<>("hello" + ts, "world" + i,
                "hello" + i));
        }
        graphState.staticGraph().V().add(new ValueVertex<>("world" + ts, "world"));

        Iterator<OneDegreeGraph<String, String, String>> it = graphState.staticGraph().VE().query(
            "hello" + ts, "world" + ts).iterator();
        Map<String, OneDegreeGraph<String, String, String>> res = new HashMap<>();
        while (it.hasNext()) {
            OneDegreeGraph<String, String, String> oneDegreeGraph = it.next();
            res.put(oneDegreeGraph.getKey(), oneDegreeGraph);
        }

        Assert.assertEquals(res.size(), 2);
        OneDegreeGraph<String, String, String> oneDegreeGraph = res.get("hello" + ts);
        Assert.assertEquals(oneDegreeGraph.getVertex().getValue(), "hello");
        Assert.assertEquals(Iterators.size(oneDegreeGraph.getEdgeIterator()), 100);
        oneDegreeGraph = res.get("world" + ts);
        Assert.assertEquals(oneDegreeGraph.getVertex().getValue(), "world");
        Assert.assertEquals(Iterators.size(oneDegreeGraph.getEdgeIterator()), 0);
    }

    @Test
    public void testKeyValueState() {
        Map<String, String> config = new HashMap<>();
        KeyValueStateDescriptor<String, String> desc =
            KeyValueStateDescriptor.build(testName, StoreType.MEMORY.name());
        desc.withDefaultValue(() -> "foobar").withKeyGroup(new KeyGroup(0, 0))
            .withKeyGroupAssigner(new DefaultKeyGroupAssigner(1));
        KeyValueState<String, String> valueState = StateFactory.buildKeyValueState(desc,
            new Configuration(config));

        valueState.put("hello", "world");
        Assert.assertEquals(valueState.get("hello"), "world");
        Assert.assertEquals(valueState.get("foo"), "foobar");
    }

    @Test
    public void testKeyListState() {
        Map<String, String> config = new HashMap<>();
        KeyListStateDescriptor<String, String> desc =
            KeyListStateDescriptor.build(testName, StoreType.MEMORY.name());
        desc.withKeyGroup(new KeyGroup(0, 0))
            .withKeyGroupAssigner(new DefaultKeyGroupAssigner(1));
        ;
        KeyListState<String, String> listState = StateFactory.buildKeyListState(desc,
            new Configuration(config));

        listState.add("hello", "world");
        listState.put("foo", Arrays.asList("bar1", "bar2"));
        Assert.assertEquals(listState.get("hello"), Arrays.asList("world"));
        Assert.assertEquals(listState.get("foo"), Arrays.asList("bar1", "bar2"));
        listState.remove("foo");
        Assert.assertEquals(listState.get("foo").size(), 0);
    }

    @Test
    public void testKeyMapState() {
        Map<String, String> config = new HashMap<>();
        KeyMapStateDescriptor<String, String, String> desc =
            KeyMapStateDescriptor.build(testName, StoreType.MEMORY.name());
        desc.withKeyGroup(new KeyGroup(0, 0))
            .withKeyGroupAssigner(new DefaultKeyGroupAssigner(1));
        ;
        KeyMapState<String, String, String> mapState = StateFactory.buildKeyMapState(desc,
            new Configuration(config));

        mapState.put("hello", config);
        mapState.add("foo", "bar1", "bar2");
        Assert.assertEquals(mapState.get("hello").size(), config.size());
        Assert.assertEquals(mapState.get("foo").get("bar1"), "bar2");
        mapState.remove("hello");
        Assert.assertEquals(mapState.get("hello").size(), 0);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testUnsupportedStore_0() {
        KeyValueStateDescriptor<String, String> desc =
            KeyValueStateDescriptor.build(testName, StoreType.REDIS.name());
        StateFactory.buildKeyValueState(desc, new Configuration());
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testUnsupportedStore_1() {
        KeyListStateDescriptor<String, String> desc =
            KeyListStateDescriptor.build(testName, StoreType.REDIS.name());
        StateFactory.buildKeyListState(desc, new Configuration());
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testUnsupportedStore_2() {
        KeyMapStateDescriptor<String, String, String> desc =
            KeyMapStateDescriptor.build(testName, StoreType.REDIS.name());
        StateFactory.buildKeyMapState(desc, new Configuration());
    }

    @Test(expectedExceptions = Exception.class)
    public void testUnsupportedStore_3() {
        GraphStateDescriptor desc = GraphStateDescriptor.build(testName, StoreType.REDIS.name());
        StateFactory.buildGraphState(desc, new Configuration());
    }
}
