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

package com.antgroup.geaflow.state;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys;
import com.antgroup.geaflow.common.config.keys.StateConfigKeys;
import com.antgroup.geaflow.common.type.Types;
import com.antgroup.geaflow.model.graph.edge.EdgeDirection;
import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.model.graph.edge.impl.ValueEdge;
import com.antgroup.geaflow.model.graph.edge.impl.ValueLabelEdge;
import com.antgroup.geaflow.model.graph.edge.impl.ValueLabelTimeEdge;
import com.antgroup.geaflow.model.graph.edge.impl.ValueTimeEdge;
import com.antgroup.geaflow.model.graph.meta.GraphMeta;
import com.antgroup.geaflow.model.graph.meta.GraphMetaType;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.model.graph.vertex.impl.ValueVertex;
import com.antgroup.geaflow.state.data.TimeRange;
import com.antgroup.geaflow.state.descriptor.GraphStateDescriptor;
import com.antgroup.geaflow.state.graph.encoder.EdgeAtom;
import com.antgroup.geaflow.state.pushdown.filter.EdgeLabelFilter;
import com.antgroup.geaflow.state.pushdown.filter.EdgeTsFilter;
import com.antgroup.geaflow.state.pushdown.filter.IEdgeFilter;
import com.antgroup.geaflow.state.pushdown.filter.IFilter;
import com.antgroup.geaflow.state.pushdown.filter.InEdgeFilter;
import com.antgroup.geaflow.state.pushdown.filter.OutEdgeFilter;
import com.antgroup.geaflow.state.pushdown.project.DstIdProjector;
import com.antgroup.geaflow.state.pushdown.project.TimeProjector;
import com.antgroup.geaflow.store.memory.MemoryConfigKeys;
import com.antgroup.geaflow.utils.keygroup.DefaultKeyGroupAssigner;
import com.antgroup.geaflow.utils.keygroup.KeyGroup;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.primitives.Longs;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

public class StaticGraphStateTest {

    private final Map<String, String> additionalConfig;
    private final StoreType storeType;

    @AfterMethod
    public void tearDown() {
        FileUtils.deleteQuietly(new File("/tmp/StaticGraphStateTest"));
    }

    public StaticGraphStateTest(StoreType storeType, Map<String, String> config) {
        this.storeType = storeType;
        this.additionalConfig = config;
    }

    public static class GraphStateTestFactory {

        @Factory
        public Object[] factoryMethod() {
            return new Object[]{new StaticGraphStateTest(StoreType.MEMORY, new HashMap<>()),
                new StaticGraphStateTest(StoreType.MEMORY,
                    ImmutableMap.of(MemoryConfigKeys.CSR_MEMORY_ENABLE.getKey(), "true")),
                new StaticGraphStateTest(StoreType.ROCKSDB,
                    ImmutableMap.of(ExecutionConfigKeys.JOB_APP_NAME.getKey(),
                        "StaticGraphStateTest"))};
        }
    }

    @Test
    public void testNormalGet() {
        GraphStateDescriptor<String, String, String> desc = GraphStateDescriptor.build("test1",
            storeType.name());
        desc.withKeyGroup(new KeyGroup(0, 1)).withKeyGroupAssigner(new DefaultKeyGroupAssigner(2));
        desc.withGraphMeta(new GraphMeta(
            new GraphMetaType<>(Types.STRING, ValueVertex.class, String.class, ValueEdge.class,
                String.class)));
        Map<String, String> config = new HashMap<>(additionalConfig);

        GraphState<String, String, String> graphState = StateFactory.buildGraphState(desc,
            new Configuration(config));

        graphState.manage().operate().setCheckpointId(1);

        graphState.staticGraph().E().add(new ValueEdge<>("1", "2", "hello", EdgeDirection.IN));
        graphState.staticGraph().E().add(new ValueEdge<>("1", "3", "hello", EdgeDirection.OUT));
        graphState.staticGraph().E().add(new ValueEdge<>("2", "2", "world", EdgeDirection.IN));
        graphState.staticGraph().E().add(new ValueEdge<>("2", "3", "world", EdgeDirection.OUT));
        graphState.staticGraph().V().add(new ValueVertex<>("1", "3"));
        graphState.staticGraph().V().add(new ValueVertex<>("2", "4"));
        graphState.manage().operate().finish();

        List<IEdge<String, String>> list = graphState.staticGraph().E().query("1").asList();
        Assert.assertEquals(list.size(), 2);

        Iterator<IVertex<String, String>> iterator = graphState.staticGraph().V().iterator();
        Assert.assertEquals(Iterators.size(iterator), 2);

        IVertex<String, String> vertex = graphState.staticGraph().V().query("1").get();
        Assert.assertEquals(vertex.getValue(), "3");

        // keyGroup get
        iterator = graphState.staticGraph().V().query(new KeyGroup(0, 0)).iterator();
        Assert.assertEquals(Iterators.size(iterator), 1);

        Iterator<String> idIterator = graphState.staticGraph().V().query(new KeyGroup(0, 0))
            .idIterator();
        Assert.assertEquals(Iterators.size(idIterator), 1);

        list = graphState.staticGraph().E().query(new KeyGroup(0, 0)).asList();
        Assert.assertEquals(list.size(), 2);

        graphState.manage().operate().close();
        graphState.manage().operate().drop();
    }

    @Test
    public void testFilter() {
        GraphStateDescriptor<String, String, String> desc = GraphStateDescriptor.build("testFilter",
            storeType.name());
        desc.withKeyGroup(new KeyGroup(0, 1)).withKeyGroupAssigner(new DefaultKeyGroupAssigner(2));
        desc.withGraphMeta(new GraphMeta(
            new GraphMetaType<>(Types.STRING, ValueVertex.class, String.class,
                ValueLabelTimeEdge.class, String.class)));
        Map<String, String> config = new HashMap<>(additionalConfig);

        GraphState<String, String, String> graphState = StateFactory.buildGraphState(desc,
            new Configuration(config));

        graphState.manage().operate().setCheckpointId(1);

        graphState.staticGraph().E().add(new ValueLabelTimeEdge<>("1", "2", "hello", "foo", 1000));
        graphState.staticGraph().E().add(new ValueLabelTimeEdge<>("1", "3", "hello", "bar", 100));
        graphState.staticGraph().E().add(new ValueLabelTimeEdge<>("2", "2", "world", "foo", 1000));
        graphState.staticGraph().E().add(new ValueLabelTimeEdge<>("2", "3", "world", "bar", 100));
        graphState.staticGraph().V().add(new ValueVertex<>("1", "3"));
        graphState.staticGraph().V().add(new ValueVertex<>("2", "4"));

        graphState.manage().operate().finish();

        List<IEdge<String, String>> list = graphState.staticGraph().E().query("1", "2")
            .by(new EdgeTsFilter<>(TimeRange.of(0, 500))).asList();
        Assert.assertEquals(list.size(), 2);

        list = graphState.staticGraph().E().query("1", "2")
            .by(new EdgeTsFilter<>(TimeRange.of(0, 500)).or(
                new EdgeTsFilter<>(TimeRange.of(800, 1100)))).asList();
        Assert.assertEquals(list.size(), 4);

        list = graphState.staticGraph().E().query("1", "2")
            .by(new IFilter[]{new EdgeTsFilter<>(TimeRange.of(0, 500)),
                new EdgeTsFilter<>(TimeRange.of(800, 1100))}).asList();
        Assert.assertEquals(list.size(), 2);

        graphState.manage().operate().close();
        graphState.manage().operate().drop();
    }

    @Test
    public void testLimitAndSort() {
        GraphStateDescriptor<String, String, String> desc = GraphStateDescriptor.build(
            "testLimitAndSort", storeType.name());
        desc.withKeyGroup(new KeyGroup(0, 1)).withKeyGroupAssigner(new DefaultKeyGroupAssigner(2));
        desc.withGraphMeta(new GraphMeta(
            new GraphMetaType<>(Types.STRING, ValueVertex.class, String.class, ValueTimeEdge.class,
                String.class)));
        Map<String, String> config = new HashMap<>(additionalConfig);
        config.put(StateConfigKeys.STATE_KV_ENCODER_EDGE_ORDER.getKey(),
            "SRC_ID, DESC_TIME, DIRECTION, DST_ID");

        GraphState<String, String, String> graphState = StateFactory.buildGraphState(desc,
            new Configuration(config));

        graphState.manage().operate().setCheckpointId(1);

        for (int i = 0; i < 100; i++) {
            String src = Integer.toString(i);
            for (int j = 1; j < 100; j++) {
                String dst = Integer.toString(j);
                graphState.staticGraph().E().add(new ValueTimeEdge<>(src, dst, "hello" + src + dst,
                    EdgeDirection.values()[j % 2], i >= 10 ? j : (i + 1) * 100 + j));
            }
            graphState.staticGraph().V().add(new ValueVertex<>(src, "world" + src));
        }
        graphState.manage().operate().finish();

        // key limit
        List<IEdge<String, String>> list = graphState.staticGraph().E().query("1", "2", "3")
            .limit(1L, 1L).asList();
        Assert.assertEquals(list.size(), 6);

        list = graphState.staticGraph().E().query("1", "2", "3").by(InEdgeFilter.getInstance())
            .limit(1L, 1L).asList();
        Assert.assertEquals(list.size(), 3);

        list = graphState.staticGraph().E().query("1").limit(1L, 1L).asList();
        Assert.assertEquals(list.size(), 2);

        list = graphState.staticGraph().E().query("11", "12", "13")
            .by(EdgeTsFilter.getInstance(10, 20).or(EdgeTsFilter.getInstance(50, 60)).singleLimit())
            .limit(2L, 1L).asList();
        Assert.assertEquals(list.size(), 18);

        list = graphState.staticGraph().E().query("11", "12", "13")
            .by(EdgeTsFilter.getInstance(10, 20).or(EdgeTsFilter.getInstance(50, 60))).limit(2L, 1L)
            .asList();
        Assert.assertEquals(list.size(), 9);

        // full limit
        list = graphState.staticGraph().E().query().by(InEdgeFilter.getInstance()).limit(1L, 1L)
            .asList();
        Assert.assertEquals(list.size(), 100);

        list = graphState.staticGraph().E().query().by(InEdgeFilter.getInstance()).limit(1L, 2L)
            .asList();
        Assert.assertEquals(list.size(), 200);

        list = graphState.staticGraph().E().query()
            .by(EdgeTsFilter.getInstance(10, 20).or(EdgeTsFilter.getInstance(50, 60)).singleLimit())
            .limit(2L, 1L).asList();
        Assert.assertEquals(list.size(), 540);

        list = graphState.staticGraph().E().query()
            .by(EdgeTsFilter.getInstance(10, 20).or(EdgeTsFilter.getInstance(50, 60))).limit(2L, 1L)
            .asList();
        Assert.assertEquals(list.size(), 270);

        // sort keys
        long[] times = Longs.toArray(graphState.staticGraph().E().query("1", "2", "3").limit(3L, 3L)
            .orderBy(EdgeAtom.DESC_TIME).select(new TimeProjector<>()).asList());
        Assert.assertEquals(times.length, 18);
        Assert.assertEquals(Longs.max(times), 4 * 100 + 99L);
        Assert.assertEquals(Longs.min(times), 294L);

        // sort all
        times = Longs.toArray(
            graphState.staticGraph().E().query().limit(0L, 1L).orderBy(EdgeAtom.DESC_TIME)
                .select(new TimeProjector<>()).asList());
        Assert.assertEquals(times.length, 100);
        Assert.assertEquals(Longs.max(times), 10 * 100 + 98L);
        Assert.assertEquals(Longs.min(times), 98L);

        graphState.manage().operate().close();
        graphState.manage().operate().drop();
    }

    @Test
    public void testProjectAndAgg() {
        GraphStateDescriptor<String, String, String> desc = GraphStateDescriptor.build(
            "testProjectAndAgg", storeType.name());
        desc.withKeyGroup(new KeyGroup(0, 1)).withKeyGroupAssigner(new DefaultKeyGroupAssigner(2));
        desc.withGraphMeta(new GraphMeta(
            new GraphMetaType<>(Types.STRING, ValueVertex.class, String.class, ValueLabelEdge.class,
                String.class)));
        Map<String, String> config = new HashMap<>(additionalConfig);

        GraphState<String, String, String> graphState = StateFactory.buildGraphState(desc,
            new Configuration(config));

        graphState.manage().operate().setCheckpointId(1);
        String[] labels = new String[]{"teacher", "student", "president"};

        for (int i = 0; i < 10; i++) {
            String src = Integer.toString(i);
            for (int j = 1; j < 10; j++) {
                String dst = Integer.toString(j);
                graphState.staticGraph().E().add(new ValueLabelEdge<>(src, dst, "hello" + src + dst,
                    EdgeDirection.values()[j % 2], labels[j % 3]));
            }
            graphState.staticGraph().V().add(new ValueVertex<>(src, "world" + src));
        }
        graphState.manage().operate().finish();

        // project test
        List<String> targetIds = graphState.staticGraph().E().query().by(InEdgeFilter.getInstance())
            .select(new DstIdProjector<>()).limit(1L, 2L).asList();

        Assert.assertEquals(targetIds.size(), 20);

        targetIds = graphState.staticGraph().E().query()
            .by(InEdgeFilter.getInstance().or(OutEdgeFilter.getInstance()).singleLimit())
            .select(new DstIdProjector<>()).limit(1L, 2L).asList();
        Assert.assertEquals(targetIds.size(), 30);

        // full agg test
        Map<String, Long> res = graphState.staticGraph().E().query()
            .by(InEdgeFilter.getInstance().and(new EdgeLabelFilter("teacher"))).aggregate();
        Assert.assertEquals(res.size(), 10);
        Assert.assertTrue(res.get("2") == 1L);

        res = graphState.staticGraph().E().query()
            .by(OutEdgeFilter.getInstance().and(new EdgeLabelFilter("student"))).aggregate();
        Assert.assertEquals(res.size(), 10);
        Assert.assertTrue(res.get("2") == 2L);

        // key agg test
        res = graphState.staticGraph().E().query("2", "5")
            .by(InEdgeFilter.getInstance().and(new EdgeLabelFilter("teacher"))).aggregate();

        Assert.assertEquals(res.size(), 2);
        Assert.assertTrue(res.get("2") == 1L);

        res = graphState.staticGraph().E().query("2", "5")
            .by(InEdgeFilter.getInstance().and(new EdgeLabelFilter("teacher")),
                OutEdgeFilter.getInstance().and(new EdgeLabelFilter("student"))).aggregate();
        Assert.assertEquals(res.size(), 2);
        Assert.assertTrue(res.get("2") == 1L);
        Assert.assertTrue(res.get("5") == 2L);

        graphState.manage().operate().close();
        graphState.manage().operate().drop();
    }
}
