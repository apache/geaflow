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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.common.config.keys.StateConfigKeys;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.common.type.primitive.IntegerType;
import org.apache.geaflow.common.type.primitive.StringType;
import org.apache.geaflow.common.utils.GsonUtil;
import org.apache.geaflow.file.FileConfigKeys;
import org.apache.geaflow.file.IPersistentIO;
import org.apache.geaflow.file.PersistentIOBuilder;
import org.apache.geaflow.model.graph.edge.EdgeDirection;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.edge.impl.IDEdge;
import org.apache.geaflow.model.graph.edge.impl.IDLabelTimeEdge;
import org.apache.geaflow.model.graph.edge.impl.ValueEdge;
import org.apache.geaflow.model.graph.edge.impl.ValueLabelTimeEdge;
import org.apache.geaflow.model.graph.meta.GraphElementMetas;
import org.apache.geaflow.model.graph.meta.GraphMeta;
import org.apache.geaflow.model.graph.meta.GraphMetaType;
import org.apache.geaflow.model.graph.property.EmptyProperty;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.model.graph.vertex.impl.IDVertex;
import org.apache.geaflow.model.graph.vertex.impl.ValueLabelTimeVertex;
import org.apache.geaflow.model.graph.vertex.impl.ValueVertex;
import org.apache.geaflow.state.data.OneDegreeGraph;
import org.apache.geaflow.state.data.TimeRange;
import org.apache.geaflow.state.descriptor.GraphStateDescriptor;
import org.apache.geaflow.state.pushdown.filter.EdgeTsFilter;
import org.apache.geaflow.state.pushdown.filter.InEdgeFilter;
import org.apache.geaflow.state.pushdown.filter.OutEdgeFilter;
import org.apache.geaflow.state.pushdown.project.DstIdProjector;
import org.apache.geaflow.utils.keygroup.DefaultKeyGroupAssigner;
import org.apache.geaflow.utils.keygroup.KeyGroup;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class RocksDBGraphStateTest {

    Map<String, String> config = new HashMap<>();

    @BeforeClass
    public void setUp() {
        FileUtils.deleteQuietly(new File("/tmp/geaflow/chk/"));
        FileUtils.deleteQuietly(new File("/tmp/RocksDBGraphStateTest"));
        Map<String, String> persistConfig = new HashMap<>();
        config.put(ExecutionConfigKeys.JOB_APP_NAME.getKey(),
            "RocksDBGraphStateTest" + System.currentTimeMillis());
        config.put(FileConfigKeys.PERSISTENT_TYPE.getKey(), "LOCAL");
        config.put(FileConfigKeys.ROOT.getKey(), "/tmp/geaflow/chk/");
        config.put(FileConfigKeys.JSON_CONFIG.getKey(), GsonUtil.toJson(persistConfig));
    }

    private <T> GraphState<T, T, T> getGraphState(IType<T> type, String name,
                                                  Map<String, String> conf) {
        return getGraphState(type, name, conf, new KeyGroup(0, 1), 2);
    }

    private <T> GraphState<T, T, T> getGraphState(IType<T> type, String name,
                                                  Map<String, String> conf, KeyGroup keyGroup,
                                                  int maxPara) {
        GraphElementMetas.clearCache();
        GraphMetaType tag = new GraphMetaType(type, ValueVertex.class, ValueVertex::new,
            type.getTypeClass(), ValueEdge.class, ValueEdge::new, type.getTypeClass());

        GraphStateDescriptor desc = GraphStateDescriptor.build(name, StoreType.ROCKSDB.name());
        desc.withKeyGroup(keyGroup).withKeyGroupAssigner(new DefaultKeyGroupAssigner(maxPara));
        desc.withGraphMeta(new GraphMeta(tag));
        GraphState<T, T, T> graphState = StateFactory.buildGraphState(desc,
            new Configuration(conf));
        return graphState;
    }

    public void testWrite(boolean async) {
        Map<String, String> conf = Maps.newHashMap(config);
        conf.put(StateConfigKeys.STATE_WRITE_ASYNC_ENABLE.getKey(), Boolean.toString(async));

        GraphState<String, String, String> graphState = getGraphState(StringType.INSTANCE,
            "writeTest", conf);
        graphState.manage().operate().setCheckpointId(1);

        for (int i = 0; i < 390; i++) {
            String id = Integer.toString(i);
            graphState.staticGraph().E().add(new ValueEdge<>("0", id, "hello"));
        }

        for (int i = 0; i < 390; i++) {
            String id = Integer.toString(i);
            graphState.staticGraph().E().add(new ValueEdge<>("1", id, "hello"));
        }
        graphState.manage().operate().finish();
        graphState.manage().operate().close();

        graphState = getGraphState(StringType.INSTANCE, "writeTest", conf);
        graphState.manage().operate().setCheckpointId(1);

        List<IEdge<String, String>> edgeList = graphState.staticGraph().E().query("0").asList();
        Assert.assertEquals(edgeList.size(), 390);

        edgeList = graphState.staticGraph().E().query("0").by(OutEdgeFilter.getInstance()).asList();
        Assert.assertEquals(edgeList.size(), 390);
        edgeList = graphState.staticGraph().E().query("0").by(InEdgeFilter.getInstance()).asList();
        Assert.assertEquals(edgeList.size(), 0);

        graphState.manage().operate().close();
        graphState.manage().operate().drop();
    }

    @Test(invocationCount = 10)
    public void testBothWriteMode() {
        testWrite(true);
        testWrite(false);
    }

    @Test
    public void testAsyncRead() {
        Map<String, String> conf = new HashMap<>(config);

        GraphState<String, String, String> graphState = getGraphState(StringType.INSTANCE, "async",
            conf);
        graphState.manage().operate().setCheckpointId(1);

        for (int i = 0; i < 10000; i++) {
            String id = Integer.toString(i);
            graphState.staticGraph().E().add(new ValueEdge<>("2", id, "hello"));
            graphState.staticGraph().V().add(new ValueVertex<>(id, "hello"));
        }
        for (int i = 0; i < 10000; i++) {
            String id = Integer.toString(i);
            graphState.staticGraph().E().add(new ValueEdge<>("3", id, "world"));
            graphState.staticGraph().V().add(new ValueVertex<>(id, "world"));
        }

        List<IEdge<String, String>> edges1 = graphState.staticGraph().E().query("2").asList();
        List<IEdge<String, String>> edges2 = graphState.staticGraph().E().query("3").asList();
        IVertex<String, String> vertex = graphState.staticGraph().V().query("9999").get();

        Assert.assertEquals(edges1.size(), 10000);
        Assert.assertEquals(edges2.size(), 10000);
        Assert.assertEquals(edges1.get(1).getValue(), "hello");
        Assert.assertEquals(edges2.get(1).getValue(), "world");
        Assert.assertEquals(vertex, new ValueVertex<>("9999", "world"));

        graphState.manage().operate().finish();
        graphState.manage().operate().drop();
    }

    @Test
    public void testIterator() {
        Map<String, String> conf = new HashMap<>(config);

        GraphState<String, String, String> graphState = getGraphState(StringType.INSTANCE,
            "iterator", conf);
        graphState.manage().operate().setCheckpointId(1);

        for (int i = 0; i < 200; i++) {
            String id = Integer.toString(i);
            if (i % 3 <= 1) {
                graphState.staticGraph().E().add(new ValueEdge<>(id, id, "hello"));
            }
            if (i % 3 >= 1) {
                graphState.staticGraph().V().add(new ValueVertex<>(id, "world"));
            }
        }
        graphState.manage().operate().finish();

        Iterator<IVertex<String, String>> it = graphState.staticGraph().V().iterator();

        List<IVertex<String, String>> vertices = Lists.newArrayList(it);
        Assert.assertEquals(vertices.size(), 133);

        it = graphState.staticGraph().V().query("122", "151").iterator();
        vertices = Lists.newArrayList(it);
        Assert.assertEquals(vertices.size(), 2);

        it = graphState.staticGraph().V().query(new KeyGroup(1, 1)).iterator();
        Assert.assertEquals(Iterators.size(it), 73);

        Iterator<String> idIt = graphState.staticGraph().V().query(new KeyGroup(1, 1)).idIterator();
        Assert.assertEquals(Iterators.size(idIt), 73);

        Iterator<OneDegreeGraph<String, String, String>> it2 = graphState.staticGraph().VE().query()
            .iterator();

        List<OneDegreeGraph> res = Lists.newArrayList(it2);
        Assert.assertEquals(res.size(), 200);

        it2 = graphState.staticGraph().VE().query("111", "115").iterator();
        res = Lists.newArrayList(it2);
        Assert.assertEquals(res.size(), 2);

        graphState.manage().operate().close();
        graphState.manage().operate().drop();

        GraphState<Integer, Integer, Integer> graphState2 = getGraphState(IntegerType.INSTANCE,
            "iterator", conf);
        graphState2.manage().operate().setCheckpointId(1);
        for (int i = 0; i < 200; i++) {
            if (i % 3 <= 1) {
                graphState2.staticGraph().E().add(new ValueEdge<>(i, i, i));
            }
            if (i % 3 >= 1) {
                graphState2.staticGraph().V().add(new ValueVertex<>(i, i));
            }
        }
        graphState2.manage().operate().finish();
        Iterator<OneDegreeGraph<Integer, Integer, Integer>> it3 = graphState2.staticGraph().VE()
            .query().iterator();

        res = Lists.newArrayList(it3);
        Assert.assertEquals(res.size(), 200);

        Iterator<Integer> idIterator = graphState2.staticGraph().V().idIterator();
        List<Integer> idList = Lists.newArrayList(idIterator);
        Assert.assertEquals(idList.size(), 133);

        graphState2.manage().operate().close();
        graphState2.manage().operate().drop();
    }

    @Test
    public void testOtherVE() {
        Map<String, String> conf = new HashMap<>(config);

        GraphMetaType tag = new GraphMetaType(IntegerType.INSTANCE, IDVertex.class, IDVertex::new,
            EmptyProperty.class, ValueLabelTimeEdge.class, ValueLabelTimeEdge::new, Object.class);

        GraphStateDescriptor desc = GraphStateDescriptor.build("OtherVE", StoreType.ROCKSDB.name());
        desc.withKeyGroup(new KeyGroup(0, 0)).withKeyGroupAssigner(new DefaultKeyGroupAssigner(1));
        desc.withGraphMeta(new GraphMeta(tag));
        GraphState<Integer, Object, Object> graphState = StateFactory.buildGraphState(desc,
            new Configuration(conf));
        graphState.manage().operate().setCheckpointId(1);

        for (int i = 0; i < 1000; i++) {
            graphState.staticGraph().V().add(new IDVertex<>(i));
            IEdge<Integer, Object> edge = new ValueLabelTimeEdge<>(i, i + 1, null, "foo", i);
            edge.setDirect(EdgeDirection.IN);
            graphState.staticGraph().E().add(edge.withValue("bar"));
        }
        graphState.manage().operate().finish();

        List<OneDegreeGraph<Integer, Object, Object>> list = graphState.staticGraph().VE().query()
            .asList();
        Assert.assertEquals(list.size(), 1000);
        int key = list.get(0).getKey();
        Assert.assertEquals(list.get(0).getVertex(), new IDVertex<>(key));
        IEdge<Integer, Object> edge = new ValueLabelTimeEdge<>(key, key + 1, null, "foo", key);
        edge.setDirect(EdgeDirection.IN);
        Assert.assertEquals(list.get(0).getEdgeIterator().next(), edge.withValue("bar"));
        graphState.manage().operate().close();
        graphState.manage().operate().drop();

        tag = new GraphMetaType(IntegerType.INSTANCE, ValueLabelTimeVertex.class, Object.class,
            IDEdge.class, EmptyProperty.class);
        desc.withGraphMeta(new GraphMeta(tag));
        graphState = StateFactory.buildGraphState(desc, new Configuration(conf));
        graphState.manage().operate().setCheckpointId(1);

        for (int i = 0; i < 1000; i++) {
            graphState.staticGraph().V().add(new ValueLabelTimeVertex<>(i, "bar", "foo", i));
            IEdge<Integer, Object> idEdge = new IDEdge<>(i, i + 1);
            idEdge.setDirect(EdgeDirection.IN);
            graphState.staticGraph().E().add(idEdge);
        }
        graphState.manage().operate().finish();
        list = graphState.staticGraph().VE().query().asList();
        Assert.assertEquals(list.size(), 1000);
        key = list.get(0).getKey();
        Assert.assertEquals(list.get(0).getVertex(),
            new ValueLabelTimeVertex<>(key, "bar", "foo", key));

        IEdge<Integer, Object> idEdge = new IDEdge<>(key, key + 1);
        idEdge.setDirect(EdgeDirection.IN);
        Assert.assertEquals(list.get(0).getEdgeIterator().next(), idEdge);

        graphState.manage().operate().close();
        graphState.manage().operate().drop();
    }

    @Test
    public void testFilter() {
        Map<String, String> conf = new HashMap<>(config);
        GraphMetaType tag = new GraphMetaType(StringType.INSTANCE, IDVertex.class, IDVertex::new,
            EmptyProperty.class, IDLabelTimeEdge.class, IDLabelTimeEdge::new, EmptyProperty.class);

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
        }

        graphState.manage().operate().finish();
        List<IEdge<String, Object>> edges = graphState.staticGraph().E().query("2")
            .by(new EdgeTsFilter(TimeRange.of(0, 5000))).asList();

        Assert.assertEquals(edges.size(), 10000);
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

        GraphMetaType tag = new GraphMetaType(StringType.INSTANCE, ValueVertex.class,
            ValueVertex::new, Object.class, ValueLabelTimeEdge.class, ValueLabelTimeEdge::new,
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
            "RocksDBGraphStateTest" + System.currentTimeMillis());
        GraphState<String, String, String> graphState = getGraphState(StringType.INSTANCE, name,
            conf);
        graphState.manage().operate().setCheckpointId(1);

        for (int i = 0; i < 10; i++) {
            String src = Integer.toString(i);
            for (int j = 1; j < 10; j++) {
                String dst = Integer.toString(j);
                graphState.staticGraph().E().add(
                    new ValueEdge<>(src, dst, "hello" + src + dst, EdgeDirection.values()[j % 2]));
            }
            graphState.staticGraph().V().add(new ValueVertex<>(src, "world" + src));
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
        String name = "fo";
        conf.put(ExecutionConfigKeys.JOB_APP_NAME.getKey(),
            "RocksDBGraphStateTest" + System.currentTimeMillis());
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
            graphState.staticGraph().E().add(new ValueEdge<>("1", id, "hello"));
            graphState.staticGraph().V().add(new ValueVertex<>("1", "hello"));
        }
        graphState.manage().operate().finish();
        graphState.manage().operate().archive();

        graphState.manage().operate().close();
        graphState.manage().operate().drop();

        graphState = getGraphState(StringType.INSTANCE, name, conf);
        graphState.manage().operate().setCheckpointId(2);
        graphState.manage().operate().recover();
        graphState.manage().operate().setCheckpointId(3);

        List<IEdge<String, String>> edges = graphState.staticGraph().E().asList();
        Assert.assertEquals(edges.size(), 100);
        List<IVertex<String, String>> vertices = graphState.staticGraph().V().asList();
        Assert.assertEquals(vertices.size(), 1);

        for (int i = 0; i < 100; i++) {
            String id = Integer.toString(i);
            graphState.staticGraph().E().add(new ValueEdge<>("2", id, "hello"));
            graphState.staticGraph().V().add(new ValueVertex<>("2", "hello"));
        }
        graphState.manage().operate().finish();
        graphState.manage().operate().archive();
        edges = graphState.staticGraph().E().asList();
        Assert.assertEquals(edges.size(), 200);
        vertices = graphState.staticGraph().V().asList();
        Assert.assertEquals(vertices.size(), 2);
        graphState.manage().operate().close();
        graphState.manage().operate().drop();

        graphState = getGraphState(StringType.INSTANCE, name, conf);
        graphState.manage().operate().setCheckpointId(3);
        graphState.manage().operate().recover();
        graphState.staticGraph().V().add(new ValueVertex<>("2", "world"));
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
                graphState.staticGraph().E().add(new ValueEdge<>(id, id, id));
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
