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
import com.antgroup.geaflow.common.type.primitive.IntegerType;
import com.antgroup.geaflow.common.type.primitive.StringType;
import com.antgroup.geaflow.common.utils.GsonUtil;
import com.antgroup.geaflow.file.FileConfigKeys;
import com.antgroup.geaflow.file.IPersistentIO;
import com.antgroup.geaflow.file.PersistentIOBuilder;
import com.antgroup.geaflow.model.graph.edge.EdgeDirection;
import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.model.graph.edge.impl.IDLabelTimeEdge;
import com.antgroup.geaflow.model.graph.edge.impl.ValueLabelEdge;
import com.antgroup.geaflow.model.graph.edge.impl.ValueLabelTimeEdge;
import com.antgroup.geaflow.model.graph.meta.GraphElementMetas;
import com.antgroup.geaflow.model.graph.meta.GraphMeta;
import com.antgroup.geaflow.model.graph.meta.GraphMetaType;
import com.antgroup.geaflow.model.graph.property.EmptyProperty;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.model.graph.vertex.impl.ValueLabelTimeVertex;
import com.antgroup.geaflow.model.graph.vertex.impl.ValueLabelVertex;
import com.antgroup.geaflow.state.data.OneDegreeGraph;
import com.antgroup.geaflow.state.data.TimeRange;
import com.antgroup.geaflow.state.descriptor.GraphStateDescriptor;
import com.antgroup.geaflow.state.pushdown.filter.EdgeLabelFilter;
import com.antgroup.geaflow.state.pushdown.filter.EdgeTsFilter;
import com.antgroup.geaflow.state.pushdown.filter.InEdgeFilter;
import com.antgroup.geaflow.state.pushdown.filter.OutEdgeFilter;
import com.antgroup.geaflow.state.pushdown.filter.VertexLabelFilter;
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

public class RocksDBLabelPartitionGraphStateTest {

    Map<String, String> config = new HashMap<>();

    @BeforeClass
    public void setUp() {
        FileUtils.deleteQuietly(new File("/tmp/geaflow/chk/"));
        FileUtils.deleteQuietly(new File("/tmp/RocksDBLabelPartitionGraphStateTest"));
        Map<String, String> persistConfig = new HashMap<>();
        config.put(ExecutionConfigKeys.JOB_APP_NAME.getKey(),
            "RocksDBLabelPartitionGraphStateTest" + System.currentTimeMillis());
        config.put(FileConfigKeys.PERSISTENT_TYPE.getKey(), "LOCAL");
        config.put(FileConfigKeys.ROOT.getKey(), "/tmp/geaflow/chk/");
        config.put(FileConfigKeys.JSON_CONFIG.getKey(), GsonUtil.toJson(persistConfig));
        config.put(RocksdbConfigKeys.ROCKSDB_GRAPH_STORE_PARTITION_TYPE.getKey(), "label");
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
        GraphMetaType tag = new GraphMetaType(type, ValueLabelVertex.class, type.getTypeClass(),
            ValueLabelEdge.class, type.getTypeClass());

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
            graphState.staticGraph().E().add(new ValueLabelEdge<>("0", id, "hello", "person"));
        }

        for (int i = 0; i < 360; i++) {
            String id = Integer.toString(i);
            graphState.staticGraph().E().add(new ValueLabelEdge<>("0", id, "world", "trade"));
        }

        for (int i = 0; i < 390; i++) {
            String id = Integer.toString(i);
            graphState.staticGraph().E().add(new ValueLabelEdge<>("1", id, "val", "person"));
        }

        graphState.manage().operate().finish();
        List<IEdge<String, String>> edgeList1 = graphState.staticGraph().E().query("0")
            .by(OutEdgeFilter.getInstance().and(EdgeLabelFilter.getInstance("person"))).asList();
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
        edgeList = graphState.staticGraph().E().query("0")
            .by(OutEdgeFilter.getInstance().and(EdgeLabelFilter.getInstance("person"))).asList();
        Assert.assertEquals(edgeList.size(), 390);

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
            graphState.staticGraph().E().add(new ValueLabelEdge<>("2", id, "hello", "person"));
            graphState.staticGraph().V().add(new ValueLabelVertex<>(id, "hello", "person"));
        }
        for (int i = 0; i < 10000; i++) {
            String id = Integer.toString(i);
            graphState.staticGraph().E().add(new ValueLabelEdge<>("2", id, "world", "trade"));
            graphState.staticGraph().V().add(new ValueLabelVertex<>(id, "world", "relation"));
        }

        List<IEdge<String, String>> edges1 = graphState.staticGraph().E().query("2")
            .by(EdgeLabelFilter.getInstance("person")).asList();
        List<IEdge<String, String>> edges2 = graphState.staticGraph().E().query("2")
            .by(EdgeLabelFilter.getInstance("trade")).asList();
        List<IEdge<String, String>> edges3 = graphState.staticGraph().E().query("2")
            .by(EdgeLabelFilter.getInstance("illegal")).asList();
        IVertex<String, String> vertex1 = graphState.staticGraph().V().query("9999")
            .by(VertexLabelFilter.getInstance("relation")).get();
        IVertex<String, String> vertex2 = graphState.staticGraph().V().query("9999")
            .by(VertexLabelFilter.getInstance("person")).get();
        IVertex<String, String> vertex3 = graphState.staticGraph().V().query("9999")
            .by(VertexLabelFilter.getInstance("illegal")).get();

        Assert.assertEquals(edges1.size(), 10000);
        Assert.assertEquals(edges2.size(), 10000);
        Assert.assertEquals(edges3.size(), 0);
        Assert.assertEquals(edges1.get(1).getValue(), "hello");
        Assert.assertEquals(edges2.get(1).getValue(), "world");
        Assert.assertEquals(vertex1, new ValueLabelVertex<>("9999", "world", "relation"));
        Assert.assertEquals(vertex2, new ValueLabelVertex<>("9999", "hello", "person"));
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
                        .add(new ValueLabelEdge<>(id, id, "hello", "person"));
                } else {
                    graphState.staticGraph().E()
                        .add(new ValueLabelEdge<>(id, id, "world", "trade"));
                }
            }

            if (i % 4 >= 1) {
                if (i % 4 == 1) {
                    graphState.staticGraph().V().add(new ValueLabelVertex<>(id, "tom", "person"));
                } else {
                    graphState.staticGraph().V()
                        .add(new ValueLabelVertex<>(id, "company", "entity"));
                }
            }
        }
        graphState.manage().operate().finish();

        Iterator<IVertex<String, String>> it = graphState.staticGraph().V().query()
            .by(VertexLabelFilter.getInstance("entity")).iterator();
        List<IVertex<String, String>> vertices = Lists.newArrayList(it);
        Assert.assertEquals(vertices.size(), 100);

        it = graphState.staticGraph().V().query().by(VertexLabelFilter.getInstance("illegal"))
            .iterator();
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
            .by(VertexLabelFilter.getInstance("person")).iterator();
        List<OneDegreeGraph> res = Lists.newArrayList(it2);
        Assert.assertEquals(res.size(), 100);

        it2 = graphState.staticGraph().VE().query().by(VertexLabelFilter.getInstance("entity"))
            .iterator();
        res = Lists.newArrayList(it2);
        Assert.assertEquals(res.size(), 200);

        it2 = graphState.staticGraph().VE().query("109", "115").iterator();
        res = Lists.newArrayList(it2);
        Assert.assertEquals(res.size(), 2);

        graphState.manage().operate().close();
        graphState.manage().operate().drop();
    }

    @Test
    public void testOtherVE() {
        Map<String, String> conf = new HashMap<>(config);

        GraphMetaType tag = new GraphMetaType(IntegerType.INSTANCE, ValueLabelVertex.class,
            StringType.class, ValueLabelTimeEdge.class, StringType.class);
        GraphStateDescriptor desc = GraphStateDescriptor.build("OtherVE", StoreType.ROCKSDB.name());
        desc.withKeyGroup(new KeyGroup(0, 0)).withKeyGroupAssigner(new DefaultKeyGroupAssigner(1));
        desc.withGraphMeta(new GraphMeta(tag));
        GraphState<Integer, Object, Object> graphState = StateFactory.buildGraphState(desc,
            new Configuration(conf));
        graphState.manage().operate().setCheckpointId(1);

        for (int i = 0; i < 1000; i++) {
            if (i % 3 == 0) {
                // 334
                graphState.staticGraph().V().add(new ValueLabelVertex<>(i, "hello", "default"));
                IEdge<Integer, Object> edge = new ValueLabelTimeEdge<>(i, i + 1, "hello", "foo", i);
                edge.setDirect(EdgeDirection.IN);
                graphState.staticGraph().E().add(edge.withValue("bar"));
            } else {
                // 666
                graphState.staticGraph().V().add(new ValueLabelVertex<>(i, "hello", "default"));
                IEdge<Integer, Object> edge = new ValueLabelTimeEdge<>(i, i + 1, "hello", "person", i);
                edge.setDirect(EdgeDirection.OUT);
                graphState.staticGraph().E().add(edge.withValue("male"));
            }
        }
        graphState.manage().operate().finish();

        List<OneDegreeGraph<Integer, Object, Object>> list = graphState.staticGraph().VE().query()
            .by(EdgeLabelFilter.getInstance("foo")).asList();
        Assert.assertEquals(list.size(), 1000);
        int key = list.get(0).getKey();
        Assert.assertEquals(list.get(0).getVertex(),
            new ValueLabelVertex<>(key, "hello", "default"));
        IEdge<Integer, Object> edge = new ValueLabelTimeEdge<>(key, key + 1, "hello", "foo", key);
        edge.setDirect(EdgeDirection.IN);
        Assert.assertEquals(list.get(0).getEdgeIterator().next(), edge.withValue("bar"));
        Assert.assertFalse(list.get(1).getEdgeIterator().hasNext());
        graphState.manage().operate().close();
        graphState.manage().operate().drop();

        tag = new GraphMetaType(IntegerType.INSTANCE, ValueLabelTimeVertex.class, Object.class,
            ValueLabelEdge.class, Object.class);
        desc.withGraphMeta(new GraphMeta(tag));
        graphState = StateFactory.buildGraphState(desc, new Configuration(conf));
        graphState.manage().operate().setCheckpointId(1);

        for (int i = 0; i < 1000; i++) {
            graphState.staticGraph().V().add(new ValueLabelTimeVertex<>(i, "bar", "foo", i));
            IEdge<Integer, Object> idEdge = new ValueLabelEdge<>(i, i + 1, "3.0", "default");
            idEdge.setDirect(EdgeDirection.IN);
            graphState.staticGraph().E().add(idEdge);
        }
        graphState.manage().operate().finish();
        list = graphState.staticGraph().VE().query().asList();
        Assert.assertEquals(list.size(), 1000);
        key = list.get(0).getKey();
        Assert.assertEquals(list.get(0).getVertex(),
            new ValueLabelTimeVertex<>(key, "bar", "foo", key));

        edge = new ValueLabelEdge<>(key, key + 1, "3.0", "default");
        edge.setDirect(EdgeDirection.IN);
        Assert.assertEquals(list.get(0).getEdgeIterator().next(), edge);

        graphState.manage().operate().close();
        graphState.manage().operate().drop();
    }

    @Test
    public void testFilter() {
        Map<String, String> conf = new HashMap<>(config);
        GraphMetaType tag = new GraphMetaType(StringType.INSTANCE, ValueLabelVertex.class,
            ValueLabelVertex::new, EmptyProperty.class, IDLabelTimeEdge.class, IDLabelTimeEdge::new,
            EmptyProperty.class);

        GraphStateDescriptor desc = GraphStateDescriptor.build("filter", StoreType.ROCKSDB.name());
        desc.withKeyGroup(new KeyGroup(0, 0)).withKeyGroupAssigner(new DefaultKeyGroupAssigner(1));
        desc.withGraphMeta(new GraphMeta(tag));
        GraphState<String, Object, Object> graphState = StateFactory.buildGraphState(desc,
            new Configuration(conf));
        graphState.manage().operate().setCheckpointId(1);

        for (int i = 0; i < 10000; i++) {
            String id = Integer.toString(i);
            IEdge<String, Object> edge1 = new IDLabelTimeEdge<>("2", id, "hello", i);
            edge1.setDirect(EdgeDirection.OUT);
            graphState.staticGraph().E().add(edge1);

            IEdge<String, Object> edge2 = new IDLabelTimeEdge<>("2", id, "hello", i);
            edge2.setDirect(EdgeDirection.IN);
            graphState.staticGraph().E().add(edge2);

            IEdge<String, Object> edge3 = new IDLabelTimeEdge<>("2", id, "world", i);
            edge3.setDirect(EdgeDirection.IN);
            graphState.staticGraph().E().add(edge3);
        }

        graphState.manage().operate().finish();
        List<IEdge<String, Object>> edges = graphState.staticGraph().E().query("2")
            .by(new EdgeTsFilter(TimeRange.of(0, 5000))).asList();

        Assert.assertEquals(edges.size(), 15000);
        long maxTime = edges.stream().mapToLong(e -> ((IDLabelTimeEdge) e).getTime()).max()
            .getAsLong();
        Assert.assertEquals(maxTime, 4999);

        long num = edges.stream().filter(e -> e.getDirect() == EdgeDirection.OUT).count();
        Assert.assertEquals(num, 5000);

        edges = graphState.staticGraph().E().query("2")
            .by(OutEdgeFilter.getInstance().and(new EdgeTsFilter(TimeRange.of(0, 1000)))).asList();
        Assert.assertEquals(edges.size(), 1000);

        maxTime = edges.stream().mapToLong(e -> ((IDLabelTimeEdge) e).getTime()).max().getAsLong();
        Assert.assertEquals(maxTime, 999);

        num = edges.stream().filter(e -> e.getDirect() == EdgeDirection.OUT).count();
        Assert.assertEquals(num, 1000);

        num = edges.stream().filter(e -> e.getDirect() == EdgeDirection.IN).count();
        Assert.assertEquals(num, 0);

        edges = graphState.staticGraph().E().query("2")
            .by(new EdgeTsFilter(TimeRange.of(7000, 10000)).and(EdgeLabelFilter.getInstance("world")))
            .asList();
        Assert.assertEquals(edges.size(), 3000);
        long minTime = edges.stream().mapToLong(e -> ((IDLabelTimeEdge) e).getTime()).min()
            .getAsLong();
        Assert.assertEquals(minTime, 7000);

        graphState.manage().operate().close();
        graphState.manage().operate().drop();
        // TODO: MAX VERSION FILTER SUPPORT
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

        GraphMetaType tag = new GraphMetaType(StringType.INSTANCE, ValueLabelVertex.class,
            ValueLabelVertex::new, Object.class, ValueLabelTimeEdge.class, ValueLabelTimeEdge::new,
            Object.class);

        GraphStateDescriptor desc = GraphStateDescriptor.build("testEdgeSort",
            StoreType.ROCKSDB.name());
        desc.withKeyGroup(new KeyGroup(0, 0)).withKeyGroupAssigner(new DefaultKeyGroupAssigner(1));
        desc.withGraphMeta(new GraphMeta(tag));
        GraphState<String, String, String> graphState = StateFactory.buildGraphState(desc,
            new Configuration(conf));
        graphState.manage().operate().setCheckpointId(1);

        for (int i = 0; i < 10000; i++) {
            String id = Integer.toString(i);
            IEdge<String, String> edge = new ValueLabelTimeEdge<>("2", id, null, "hello", i);
            graphState.staticGraph().E().add(edge.withValue("world"));
        }
        graphState.manage().operate().finish();

        List<IEdge<String, String>> list = graphState.staticGraph().E().asList();
        Assert.assertEquals(((ValueLabelTimeEdge) list.get(0)).getTime(), 9999);
        Assert.assertEquals(((ValueLabelTimeEdge) list.get(9999)).getTime(), 0);
    }

    @Test
    public void testLimit() {
        Map<String, String> conf = new HashMap<>(config);
        String name = "testLimit";
        conf.put(ExecutionConfigKeys.JOB_APP_NAME.getKey(),
            "RocksDBLabelPartitionGraphStateTest" + System.currentTimeMillis());
        GraphState<String, String, String> graphState = getGraphState(StringType.INSTANCE, name,
            conf);
        graphState.manage().operate().setCheckpointId(1);

        for (int i = 0; i < 10; i++) {
            String src = Integer.toString(i);
            for (int j = 1; j < 10; j++) {
                String dst = Integer.toString(j);
                graphState.staticGraph().E().add(new ValueLabelEdge<>(src, dst, "hello" + src + dst,
                    EdgeDirection.values()[j % 2], "foo"));
            }
            graphState.staticGraph().V().add(new ValueLabelVertex<>(src, "world" + src, "foo"));
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
            graphState.staticGraph().E().add(new ValueLabelEdge<>("1", id, "hello", "foo"));
            graphState.staticGraph().V().add(new ValueLabelVertex<>("1", "hello", "foo"));
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
            graphState.staticGraph().E().add(new ValueLabelEdge<>("2", id, "hello", "person"));
            graphState.staticGraph().V().add(new ValueLabelVertex<>("2", "hello", "person"));
        }
        graphState.manage().operate().finish();
        graphState.manage().operate().archive();
        edges = graphState.staticGraph().E().asList();
        Assert.assertEquals(edges.size(), 180);
        edges = graphState.staticGraph().E().query().by(EdgeLabelFilter.getInstance("person"))
            .asList();
        Assert.assertEquals(edges.size(), 80);
        edges = graphState.staticGraph().E().query().by(EdgeLabelFilter.getInstance("foo")).asList();
        Assert.assertEquals(edges.size(), 100);
        vertices = graphState.staticGraph().V().asList();
        Assert.assertEquals(vertices.size(), 2);
        graphState.manage().operate().close();
        graphState.manage().operate().drop();

        graphState = getGraphState(StringType.INSTANCE, name, conf);
        graphState.manage().operate().setCheckpointId(3);
        graphState.manage().operate().recover();
        graphState.staticGraph().V().add(new ValueLabelVertex<>("2", "world", "person"));
        Assert.assertEquals(graphState.staticGraph().V().query("2").get().getValue(), "world");
        graphState.manage().operate().finish();
        Assert.assertEquals(graphState.staticGraph().V().query("2").get().getValue(), "world");

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
                graphState.staticGraph().E().add(new ValueLabelEdge<>(id, id, id, "foo"));
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
