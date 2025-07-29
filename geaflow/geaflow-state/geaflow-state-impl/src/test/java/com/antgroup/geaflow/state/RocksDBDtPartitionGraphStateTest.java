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
import com.antgroup.geaflow.common.type.IType;
import com.antgroup.geaflow.common.type.primitive.StringType;
import com.antgroup.geaflow.common.utils.GsonUtil;
import com.antgroup.geaflow.file.FileConfigKeys;
import com.antgroup.geaflow.file.IPersistentIO;
import com.antgroup.geaflow.file.PersistentIOBuilder;
import com.antgroup.geaflow.model.graph.edge.EdgeDirection;
import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.model.graph.edge.impl.IDLabelTimeEdge;
import com.antgroup.geaflow.model.graph.edge.impl.ValueLabelTimeEdge;
import com.antgroup.geaflow.model.graph.edge.impl.ValueTimeEdge;
import com.antgroup.geaflow.model.graph.meta.GraphElementMetas;
import com.antgroup.geaflow.model.graph.meta.GraphMeta;
import com.antgroup.geaflow.model.graph.meta.GraphMetaType;
import com.antgroup.geaflow.model.graph.property.EmptyProperty;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.model.graph.vertex.impl.ValueLabelTimeVertex;
import com.antgroup.geaflow.model.graph.vertex.impl.ValueTimeVertex;
import com.antgroup.geaflow.state.data.OneDegreeGraph;
import com.antgroup.geaflow.state.data.TimeRange;
import com.antgroup.geaflow.state.descriptor.GraphStateDescriptor;
import com.antgroup.geaflow.state.pushdown.filter.EdgeLabelFilter;
import com.antgroup.geaflow.state.pushdown.filter.EdgeTsFilter;
import com.antgroup.geaflow.state.pushdown.filter.InEdgeFilter;
import com.antgroup.geaflow.state.pushdown.filter.OutEdgeFilter;
import com.antgroup.geaflow.state.pushdown.filter.VertexTsFilter;
import com.antgroup.geaflow.state.pushdown.project.DstIdProjector;
import com.antgroup.geaflow.store.config.StoreConfigKeys;
import com.antgroup.geaflow.store.rocksdb.RocksdbConfigKeys;
import com.antgroup.geaflow.utils.keygroup.DefaultKeyGroupAssigner;
import com.antgroup.geaflow.utils.keygroup.KeyGroup;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class RocksDBDtPartitionGraphStateTest {

    Map<String, String> config = new HashMap<>();

    @BeforeClass
    public void setUp() {
        FileUtils.deleteQuietly(new File("/tmp/geaflow/chk/"));
        FileUtils.deleteQuietly(new File("/tmp/RocksDBDtPartitionGraphStateTest"));
        Map<String, String> persistConfig = new HashMap<>();
        config.put(ExecutionConfigKeys.JOB_APP_NAME.getKey(),
            "RocksDBDtPartitionGraphStateTest" + System.currentTimeMillis());
        config.put(FileConfigKeys.PERSISTENT_TYPE.getKey(), "LOCAL");
        config.put(FileConfigKeys.ROOT.getKey(), "/tmp/geaflow/chk/");
        config.put(FileConfigKeys.JSON_CONFIG.getKey(), GsonUtil.toJson(persistConfig));
        config.put(RocksdbConfigKeys.ROCKSDB_GRAPH_STORE_PARTITION_TYPE.getKey(), "dt");
        // 2025-01-01 00:00:00
        config.put(RocksdbConfigKeys.ROCKSDB_GRAPH_STORE_DT_START.getKey(), "1735660800");
        // 7 days
        config.put(RocksdbConfigKeys.ROCKSDB_GRAPH_STORE_DT_CYCLE.getKey(), "604800");
        config.put(StoreConfigKeys.STORE_FILTER_CODEGEN_ENABLE.getKey(), "false");
    }

    private <T> GraphState<T, T, T> getGraphState(IType<T> type, String name,
                                                  Map<String, String> conf) {
        return getGraphState(type, name, conf, new KeyGroup(0, 1), 2);
    }

    private <T> GraphState<T, T, T> getGraphState(IType<T> type, String name,
                                                  Map<String, String> conf, KeyGroup keyGroup,
                                                  int maxPara) {
        GraphElementMetas.clearCache();
        GraphMetaType tag = new GraphMetaType(type, ValueTimeVertex.class, type.getTypeClass(),
            ValueTimeEdge.class, type.getTypeClass());

        GraphStateDescriptor desc = GraphStateDescriptor.build(name, StoreType.ROCKSDB.name());
        desc.withKeyGroup(keyGroup).withKeyGroupAssigner(new DefaultKeyGroupAssigner(maxPara));
        desc.withGraphMeta(new GraphMeta(tag));
        GraphState<T, T, T> graphState = StateFactory.buildGraphState(desc,
            new Configuration(conf));
        return graphState;
    }

    @Test(invocationCount = 10)
    public void testWrite() {
        Map<String, String> conf = Maps.newHashMap(config);
        GraphState<String, String, String> graphState = getGraphState(StringType.INSTANCE,
            "testWrite", conf);
        graphState.manage().operate().setCheckpointId(1);

        for (int i = 0; i < 390; i++) {
            String id = Integer.toString(i);
            graphState.staticGraph().E().add(new ValueTimeEdge<>("0", id, "hello", 1735660800 + i));
        }

        for (int i = 0; i < 360; i++) {
            String id = Integer.toString(i);
            graphState.staticGraph().E()
                .add(new ValueTimeEdge<>("0", id, "world", 1736265600/*1735660800 + 604800*/ + i));
        }

        for (int i = 0; i < 390; i++) {
            String id = Integer.toString(i);
            graphState.staticGraph().E().add(new ValueTimeEdge<>("1", id, "val", 1735660800 + i));
        }

        graphState.manage().operate().finish();
        List<IEdge<String, String>> edgeList1 = graphState.staticGraph().E().query("0")
            .by(OutEdgeFilter.getInstance().and(EdgeTsFilter.getInstance(1735660800, 1735662000)))
            .asList();
        Assert.assertEquals(edgeList1.size(), 390);
        graphState.manage().operate().close();

        graphState = getGraphState(StringType.INSTANCE, "testWrite", conf);
        graphState.manage().operate().setCheckpointId(1);

        List<IEdge<String, String>> edgeList = graphState.staticGraph().E().query("0").asList();
        Assert.assertEquals(edgeList.size(), 750);

        edgeList = graphState.staticGraph().E().query("0").by(OutEdgeFilter.getInstance()).asList();
        Assert.assertEquals(edgeList.size(), 750);
        edgeList = graphState.staticGraph().E().query("0").by(InEdgeFilter.getInstance()).asList();
        Assert.assertEquals(edgeList.size(), 0);
        edgeList = graphState.staticGraph().E().query()
            .by(OutEdgeFilter.getInstance().and(EdgeTsFilter.getInstance(1735660800, 1735662000)))
            .asList();
        Assert.assertEquals(edgeList.size(), 780);
        edgeList = graphState.staticGraph().E().query()
            .by(OutEdgeFilter.getInstance().and(EdgeTsFilter.getInstance(1736265600, 1736265699)))
            .asList();
        Assert.assertEquals(edgeList.size(), 99);

        graphState.manage().operate().close();
        graphState.manage().operate().drop();
    }

    @Test
    public void testRead() {
        Map<String, String> conf = new HashMap<>(config);

        GraphState<String, String, String> graphState = getGraphState(StringType.INSTANCE,
            "testRead", conf);
        graphState.manage().operate().setCheckpointId(1);

        for (int i = 0; i < 10000; i++) {
            String id = Integer.toString(i);
            graphState.staticGraph().E().add(new ValueTimeEdge<>("2", id, "hello", 1735660800 + i));
            graphState.staticGraph().V().add(new ValueTimeVertex<>(id, "hello", 1735660800 + i));
        }
        for (int i = 0; i < 10000; i++) {
            String id = Integer.toString(i);
            graphState.staticGraph().E()
                .add(new ValueTimeEdge<>("2", id, "world", 1736265600/*1735660800 + 604800*/ + i));
            graphState.staticGraph().V().add(new ValueTimeVertex<>(id, "world", 1736870400 + i
                /*1735660800 + 2*604800*/));
        }

        List<IEdge<String, String>> edges1 = graphState.staticGraph().E().query("2")
            .by(EdgeTsFilter.getInstance(1735660800, 1735662800)).asList();
        List<IEdge<String, String>> edges2 = graphState.staticGraph().E().query("2")
            .by(EdgeTsFilter.getInstance(1736265600, 1736365600)).asList();
        List<IEdge<String, String>> edges3 = graphState.staticGraph().E().query("2")
            .by(EdgeTsFilter.getInstance(1835660800, 1835760800)).asList();
        IVertex<String, String> vertex1 = graphState.staticGraph().V().query("9999")
            .by(VertexTsFilter.getInstance(1736870400, 1736970400)).get();
        IVertex<String, String> vertex2 = graphState.staticGraph().V().query("9999")
            .by(VertexTsFilter.getInstance(1735660800, 1735760800)).get();
        IVertex<String, String> vertex3 = graphState.staticGraph().V().query("9999")
            .by(VertexTsFilter.getInstance(1835660800, 1835760800)).get();

        Assert.assertEquals(edges1.size(), 2000);
        Assert.assertEquals(edges2.size(), 10000);
        Assert.assertEquals(edges3.size(), 0);
        Assert.assertEquals(edges1.get(1).getValue(), "hello");
        Assert.assertEquals(edges2.get(1).getValue(), "world");
        Assert.assertEquals(vertex1, new ValueTimeVertex<>("9999", "world", 1736880399
            /*1736870400 + 9999*/));
        Assert.assertEquals(vertex2, new ValueTimeVertex<>("9999", "hello", 1735670799
            /*1735660800 + 9999*/));
        Assert.assertNull(vertex3);

        graphState.manage().operate().finish();
        graphState.manage().operate().drop();
    }

    @Test
    public void testIterator() {
        Map<String, String> conf = new HashMap<>(config);

        GraphState<String, String, String> graphState = getGraphState(StringType.INSTANCE,
            "testIterator", conf);
        graphState.manage().operate().setCheckpointId(1);

        for (int i = 0; i < 200; i++) {
            String id = Integer.toString(i);
            if (i % 4 <= 1) {
                if (i % 4 == 0) {
                    graphState.staticGraph().E()
                        .add(new ValueTimeEdge<>(id, id, "hello", 1735660800 + i));
                } else {
                    graphState.staticGraph().E().add(new ValueTimeEdge<>(id, id, "world",
                        1736265600/*1735660800 + 604800*/ + i));
                }
            }

            if (i % 4 >= 1) {
                if (i % 4 == 1) {
                    graphState.staticGraph().V()
                        .add(new ValueTimeVertex<>(id, "tom", 1735660800 + i));
                } else {
                    graphState.staticGraph().V()
                        .add(new ValueTimeVertex<>(id, "company", 1736870400 + i
                            /*1735660800 + 2*604800*/));
                }
            }
        }
        graphState.manage().operate().finish();

        Iterator<IVertex<String, String>> it = graphState.staticGraph().V().query()
            .by(VertexTsFilter.getInstance(1736870400, 1736970400)).iterator();
        List<IVertex<String, String>> vertices = Lists.newArrayList(it);
        Assert.assertEquals(vertices.size(), 100);

        it = graphState.staticGraph().V().query()
            .by(VertexTsFilter.getInstance(1836870400, 1836970400)).iterator();
        vertices = Lists.newArrayList(it);
        Assert.assertEquals(vertices.size(), 0);

        it = graphState.staticGraph().V().query("122", "151").iterator();
        vertices = Lists.newArrayList(it);
        Assert.assertEquals(vertices.size(), 2);

        it = graphState.staticGraph().V().query().iterator();
        Assert.assertEquals(Iterators.size(it), 150);
        Iterator<String> idIt = graphState.staticGraph().V().query().idIterator();
        Assert.assertEquals(Iterators.size(idIt), 150);

        Iterator<OneDegreeGraph<String, String, String>> it2 = graphState.staticGraph().VE().query()
            .by(VertexTsFilter.getInstance(1735660800, 1736265600)
                .and(EdgeTsFilter.getInstance(1735660800, 1736265600))).iterator();
        List<OneDegreeGraph> res = Lists.newArrayList(it2);
        Assert.assertEquals(res.size(), 100);

        it2 = graphState.staticGraph().VE().query()
            .by(VertexTsFilter.getInstance(1836870400, 1836970400)
                .and(EdgeTsFilter.getInstance(1836870400, 1836970400))).iterator();
        res = Lists.newArrayList(it2);
        Assert.assertEquals(res.size(), 0);

        it2 = graphState.staticGraph().VE().query("109", "115").iterator();
        res = Lists.newArrayList(it2);
        Assert.assertEquals(res.size(), 2);

        graphState.manage().operate().close();
        graphState.manage().operate().drop();
    }

    @Test
    public void testFilter() {
        Map<String, String> conf = new HashMap<>(config);
        GraphMetaType tag = new GraphMetaType(StringType.INSTANCE, ValueLabelTimeVertex.class,
            ValueLabelTimeVertex::new, EmptyProperty.class, IDLabelTimeEdge.class,
            IDLabelTimeEdge::new, EmptyProperty.class);

        GraphStateDescriptor desc = GraphStateDescriptor.build("filter", StoreType.ROCKSDB.name());
        desc.withKeyGroup(new KeyGroup(0, 0)).withKeyGroupAssigner(new DefaultKeyGroupAssigner(1));
        desc.withGraphMeta(new GraphMeta(tag));
        GraphState<String, Object, Object> graphState = StateFactory.buildGraphState(desc,
            new Configuration(conf));
        graphState.manage().operate().setCheckpointId(1);

        for (int i = 0; i < 10000; i++) {
            String id = Integer.toString(i);
            IEdge<String, Object> edge1 = new IDLabelTimeEdge<>("2", id, "hello", 1735660800 + i);
            edge1.setDirect(EdgeDirection.OUT);
            graphState.staticGraph().E().add(edge1);

            IEdge<String, Object> edge2 = new IDLabelTimeEdge<>("2", id, "hello", 1735660800 + i);
            edge2.setDirect(EdgeDirection.IN);
            graphState.staticGraph().E().add(edge2);

            IEdge<String, Object> edge3 = new IDLabelTimeEdge<>("2", id, "world", 1735660800 + i);
            edge3.setDirect(EdgeDirection.IN);
            graphState.staticGraph().E().add(edge3);
        }

        graphState.manage().operate().finish();
        List<IEdge<String, Object>> edges = graphState.staticGraph().E().query("2")
            .by(new EdgeTsFilter(TimeRange.of(1735660800, 1735665800))).asList();

        Assert.assertEquals(edges.size(), 15000);
        long maxTime = edges.stream().mapToLong(e -> ((IDLabelTimeEdge) e).getTime()).max()
            .getAsLong();
        Assert.assertEquals(maxTime, 1735665799);

        long num = edges.stream().filter(e -> e.getDirect() == EdgeDirection.OUT).count();
        Assert.assertEquals(num, 5000);

        edges = graphState.staticGraph().E().query("2").by(OutEdgeFilter.getInstance()
            .and(new EdgeTsFilter(TimeRange.of(1735660800, 1735661800)))).asList();
        Assert.assertEquals(edges.size(), 1000);

        maxTime = edges.stream().mapToLong(e -> ((IDLabelTimeEdge) e).getTime()).max().getAsLong();
        Assert.assertEquals(maxTime, 1735661799);

        num = edges.stream().filter(e -> e.getDirect() == EdgeDirection.OUT).count();
        Assert.assertEquals(num, 1000);

        num = edges.stream().filter(e -> e.getDirect() == EdgeDirection.IN).count();
        Assert.assertEquals(num, 0);

        edges = graphState.staticGraph().E().query("2")
            .by(new EdgeTsFilter(TimeRange.of(1735667800, 1735670800)).and(
                EdgeLabelFilter.getInstance("world"))).asList();
        Assert.assertEquals(edges.size(), 3000);
        long minTime = edges.stream().mapToLong(e -> ((IDLabelTimeEdge) e).getTime()).min()
            .getAsLong();
        Assert.assertEquals(minTime, 1735667800);

        graphState.manage().operate().close();
        graphState.manage().operate().drop();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testEdgeOrderError() {
        Map<String, String> conf = Maps.newHashMap(config);
        conf.put(StateConfigKeys.STATE_KV_ENCODER_EDGE_ORDER.getKey(),
            "SRC_ID, DESC_TIME, LABEL, DIRECTION, DST_ID");
        getGraphState(StringType.INSTANCE, "testEdgeOrderError", conf);
    }

    @Test
    public void testEdgeSort() {
        Map<String, String> conf = Maps.newHashMap(config);
        conf.put(StateConfigKeys.STATE_KV_ENCODER_EDGE_ORDER.getKey(),
            "SRC_ID, DESC_TIME, LABEL, DIRECTION, DST_ID");

        GraphMetaType tag = new GraphMetaType(StringType.INSTANCE, ValueLabelTimeVertex.class,
            ValueLabelTimeVertex::new, Object.class, ValueLabelTimeEdge.class,
            ValueLabelTimeEdge::new, Object.class);

        GraphStateDescriptor desc = GraphStateDescriptor.build("testEdgeSort",
            StoreType.ROCKSDB.name());
        desc.withKeyGroup(new KeyGroup(0, 0)).withKeyGroupAssigner(new DefaultKeyGroupAssigner(1));
        desc.withGraphMeta(new GraphMeta(tag));
        GraphState<String, String, String> graphState = StateFactory.buildGraphState(desc,
            new Configuration(conf));
        graphState.manage().operate().setCheckpointId(1);

        for (int i = 0; i < 10000; i++) {
            String id = Integer.toString(i);
            IEdge<String, String> edge = new ValueLabelTimeEdge<>("2", id, null, "hello",
                1735660800 + i);
            graphState.staticGraph().E().add(edge.withValue("world"));
        }
        graphState.manage().operate().finish();

        List<IEdge<String, String>> list = graphState.staticGraph().E().asList();
        Assert.assertEquals(((ValueLabelTimeEdge) list.get(0)).getTime(), 1735670799);
        Assert.assertEquals(((ValueLabelTimeEdge) list.get(9999)).getTime(), 1735660800);
    }

    @Test
    public void testLimit() {
        Map<String, String> conf = new HashMap<>(config);
        String name = "testLimit";
        conf.put(ExecutionConfigKeys.JOB_APP_NAME.getKey(),
            "RocksDBDtPartitionGraphStateTest" + System.currentTimeMillis());
        GraphState<String, String, String> graphState = getGraphState(StringType.INSTANCE, name,
            conf);
        graphState.manage().operate().setCheckpointId(1);

        for (int i = 0; i < 10; i++) {
            String src = Integer.toString(i);
            for (int j = 1; j < 10; j++) {
                String dst = Integer.toString(j);
                graphState.staticGraph().E().add(new ValueTimeEdge<>(src, dst, "hello" + src + dst,
                    EdgeDirection.values()[j % 2], 1735660800 + i * j));
            }
            graphState.staticGraph().V()
                .add(new ValueTimeVertex<>(src, "world" + src, 1735660800 + i));
        }
        graphState.manage().operate().finish();

        List<IEdge<String, String>> list = graphState.staticGraph().E().query("1", "2", "3")
            .limit(1L, 1L).asList();
        Assert.assertEquals(list.size(), 6);

        list = graphState.staticGraph().E().query().by(InEdgeFilter.getInstance()).limit(1L, 1L)
            .asList();
        Assert.assertEquals(list.size(), 10);

        list = graphState.staticGraph().E().query().by(InEdgeFilter.getInstance()).limit(1L, 2L)
            .asList();
        Assert.assertEquals(list.size(), 20);

        List<String> targetIds = graphState.staticGraph().E().query().by(InEdgeFilter.getInstance())
            .select(new DstIdProjector<>()).limit(1L, 2L).asList();

        Assert.assertEquals(targetIds.size(), 20);
        graphState.manage().operate().close();
        graphState.manage().operate().drop();
    }

    @Test
    public void testFO() throws IOException {
        Map<String, String> conf = new HashMap<>(config);
        String name = "testFO";
        GraphState<String, String, String> graphState = getGraphState(StringType.INSTANCE, name,
            conf);
        graphState.manage().operate().setCheckpointId(1);

        graphState.manage().operate().finish();
        graphState.manage().operate().archive();

        graphState.manage().operate().drop();
        graphState = getGraphState(StringType.INSTANCE, name, conf);

        graphState.manage().operate().setCheckpointId(1);
        graphState.manage().operate().recover();
        graphState.manage().operate().setCheckpointId(2);

        for (int i = 0; i < 100; i++) {
            String id = Integer.toString(i);
            graphState.staticGraph().E().add(new ValueTimeEdge<>("1", id, "hello", 1735660800 + i));
            graphState.staticGraph().V().add(new ValueTimeVertex<>("1", "hello", 1735660800 + i));
        }

        List<IEdge<String, String>> edges = graphState.staticGraph().E().query("1").asList();
        Assert.assertEquals(edges.size(), 100);
        List<IVertex<String, String>> vertices = graphState.staticGraph().V().asList();
        Assert.assertEquals(vertices.size(), 1);

        graphState.manage().operate().archive();
        graphState.manage().operate().finish();
        graphState.manage().operate().close();
        graphState.manage().operate().drop();

        graphState = getGraphState(StringType.INSTANCE, name, conf);
        graphState.manage().operate().setCheckpointId(2);
        graphState.manage().operate().recover();
        graphState.manage().operate().setCheckpointId(3);

        edges = graphState.staticGraph().E().asList();
        Assert.assertEquals(edges.size(), 100);
        vertices = graphState.staticGraph().V().asList();
        Assert.assertEquals(vertices.size(), 1);

        for (int i = 0; i < 80; i++) {
            String id = Integer.toString(i);
            graphState.staticGraph().E()
                .add(new ValueTimeEdge<>("2", id, "hello", 1736265600/*1735660800 + 604800*/ + i));
            graphState.staticGraph().V()
                .add(new ValueTimeVertex<>("2", "hello", 1736265600/*1735660800 + 604800*/ + i));
        }
        graphState.manage().operate().finish();
        graphState.manage().operate().archive();
        edges = graphState.staticGraph().E().asList();
        Assert.assertEquals(edges.size(), 180);
        edges = graphState.staticGraph().E().query()
            .by(EdgeTsFilter.getInstance(1736265600, 1737265600)).asList();
        Assert.assertEquals(edges.size(), 80);
        edges = graphState.staticGraph().E().query()
            .by(EdgeTsFilter.getInstance(1735660800, 1735661800)).asList();
        Assert.assertEquals(edges.size(), 100);
        vertices = graphState.staticGraph().V().asList();
        Assert.assertEquals(vertices.size(), 2);
        graphState.manage().operate().close();
        graphState.manage().operate().drop();

        graphState = getGraphState(StringType.INSTANCE, name, conf);
        graphState.manage().operate().setCheckpointId(3);
        graphState.manage().operate().recover();
        graphState.staticGraph().V().add(new ValueTimeVertex<>("2", "world", 1737265600));
        Assert.assertEquals(graphState.staticGraph().V().query("2")
            .by(VertexTsFilter.getInstance(1737265600, 1737265601)).get().getValue(), "world");
        graphState.manage().operate().finish();
        Assert.assertEquals(graphState.staticGraph().V().query("2")
            .by(VertexTsFilter.getInstance(1737265600, 1737265601)).get().getValue(), "world");

        graphState.manage().operate().close();
        graphState.manage().operate().drop();
        IPersistentIO persistentIO = PersistentIOBuilder.build(new Configuration(conf));
        persistentIO.delete(new Path(Configuration.getString(FileConfigKeys.ROOT, conf),
            Configuration.getString(ExecutionConfigKeys.JOB_APP_NAME, conf)), true);
    }


    @Test
    public void testArchive() throws IOException {
        Map<String, String> conf = new HashMap<>(config);
        IPersistentIO persistentIO = PersistentIOBuilder.build(new Configuration(conf));
        persistentIO.delete(new Path(Configuration.getString(FileConfigKeys.ROOT, conf),
            Configuration.getString(ExecutionConfigKeys.JOB_APP_NAME, conf)), true);

        GraphState<String, String, String> graphState = null;

        for (int v = 1; v < 10; v++) {
            graphState = getGraphState(StringType.INSTANCE, "archive", conf);
            if (v > 1) {
                graphState.manage().operate().setCheckpointId(v - 1);
                graphState.manage().operate().recover();
            }
            graphState.manage().operate().setCheckpointId(v);
            for (int i = 0; i < 10; i++) {
                String id = Integer.toString(i);
                graphState.staticGraph().E().add(new ValueTimeEdge<>(id, id, id, 1735660800 + i));
            }
            graphState.manage().operate().finish();
            graphState.manage().operate().archive();
            graphState.manage().operate().close();
        }

        graphState.manage().operate().drop();
        persistentIO.delete(new Path(Configuration.getString(FileConfigKeys.ROOT, conf),
            Configuration.getString(ExecutionConfigKeys.JOB_APP_NAME, conf)), true);
    }
}
