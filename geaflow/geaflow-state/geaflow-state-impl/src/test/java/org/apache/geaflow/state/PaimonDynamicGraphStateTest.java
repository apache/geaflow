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
import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.common.config.keys.StateConfigKeys;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.common.type.primitive.StringType;
import org.apache.geaflow.common.utils.GsonUtil;
import org.apache.geaflow.file.FileConfigKeys;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.edge.impl.ValueEdge;
import org.apache.geaflow.model.graph.meta.GraphMeta;
import org.apache.geaflow.model.graph.meta.GraphMetaType;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.model.graph.vertex.impl.ValueVertex;
import org.apache.geaflow.state.data.OneDegreeGraph;
import org.apache.geaflow.state.descriptor.GraphStateDescriptor;
import org.apache.geaflow.state.pushdown.filter.IEdgeFilter;
import org.apache.geaflow.state.pushdown.filter.IVertexFilter;
import org.apache.geaflow.store.paimon.config.PaimonConfigKeys;
import org.apache.geaflow.utils.keygroup.DefaultKeyGroupAssigner;
import org.apache.geaflow.utils.keygroup.KeyGroup;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class PaimonDynamicGraphStateTest {

    static Map<String, String> config = new HashMap<>();

    @BeforeClass
    public static void setUp() {
        FileUtils.deleteQuietly(new File("/tmp/geaflow/chk/"));
        FileUtils.deleteQuietly(new File("/tmp/PaimonDynamicGraphStateTest"));
        Map<String, String> persistConfig = new HashMap<>();
        config.put(ExecutionConfigKeys.JOB_APP_NAME.getKey(),
            "PaimonDynamicGraphStateTest" + System.currentTimeMillis());
        config.put(FileConfigKeys.PERSISTENT_TYPE.getKey(), "LOCAL");
        config.put(FileConfigKeys.ROOT.getKey(), "/tmp/geaflow/chk/");
        config.put(FileConfigKeys.JSON_CONFIG.getKey(), GsonUtil.toJson(persistConfig));
        config.put(PaimonConfigKeys.PAIMON_STORE_WAREHOUSE.getKey(),
            "file:///tmp/PaimonDynamicGraphStateTest/");
        config.put(PaimonConfigKeys.PAIMON_STORE_TABLE_AUTO_CREATE_ENABLE.getKey(), "true");
    }

    @AfterClass
    public static void tearUp() {
        FileUtils.deleteQuietly(new File("/tmp/PaimonDynamicGraphStateTest"));
    }

    private <T> GraphState<T, T, T> getGraphState(IType<T> type, String name,
                                                  Map<String, String> conf) {
        return getGraphState(type, name, conf, new KeyGroup(0, 1), 2);
    }

    private <T> GraphState<T, T, T> getGraphState(IType<T> type, String name,
                                                  Map<String, String> conf, KeyGroup group,
                                                  int maxPara) {
        GraphMetaType tag = new GraphMetaType(type, ValueVertex.class, type.getTypeClass(),
            ValueEdge.class, type.getTypeClass());

        GraphStateDescriptor desc = GraphStateDescriptor.build(name, StoreType.PAIMON.name());
        desc.withKeyGroup(group).withDataModel(DataModel.DYNAMIC_GRAPH)
            .withKeyGroupAssigner(new DefaultKeyGroupAssigner(maxPara));
        desc.withGraphMeta(new GraphMeta(tag));
        GraphState<T, T, T> graphState = StateFactory.buildGraphState(desc,
            new Configuration(conf));
        return graphState;
    }

    @Test
    public void testWriteRead() {
        Map<String, String> conf = new HashMap<>(config);
        conf.put(StateConfigKeys.STATE_WRITE_ASYNC_ENABLE.getKey(), "false");
        conf.put(PaimonConfigKeys.PAIMON_STORE_DISTRIBUTED_MODE_ENABLE.getKey(), "false");
        testApi(conf);
    }

    @Test
    public void testWriteRead2() {
        Map<String, String> conf = new HashMap<>(config);
        conf.put(StateConfigKeys.STATE_WRITE_ASYNC_ENABLE.getKey(), "false");
        conf.put(PaimonConfigKeys.PAIMON_STORE_DISTRIBUTED_MODE_ENABLE.getKey(), "false");
        conf.put(PaimonConfigKeys.PAIMON_STORE_DATABASE.getKey(), "graph");
        testApi(conf);
    }

    private void testApi(Map<String, String> conf) {
        conf.put(StateConfigKeys.STATE_WRITE_BUFFER_SIZE.getKey(), "100");
        GraphState<String, String, String> graphState = getGraphState(StringType.INSTANCE, "testApi", conf);

        graphState.manage().operate().setCheckpointId(1);

        for (int i = 0; i < 1000; i++) {
            graphState.dynamicGraph().E().add(1L, new ValueEdge<>("1", "2", "hello"));
            graphState.dynamicGraph().E().add(1L, new ValueEdge<>("1", "3", "hello"));
            graphState.dynamicGraph().E().add(2L, new ValueEdge<>("2", "2", "world"));
            graphState.dynamicGraph().E().add(2L, new ValueEdge<>("2", "3", "world"));
            graphState.dynamicGraph().V().add(1L, new ValueVertex<>("1", "3"));
            graphState.dynamicGraph().V().add(2L, new ValueVertex<>("2", "4"));
            graphState.dynamicGraph().V().add(2L, new ValueVertex<>("1", "5"));
            graphState.dynamicGraph().V().add(3L, new ValueVertex<>("1", "6"));
        }

        graphState.dynamicGraph().V().add(4L, new ValueVertex<>("1", "6"));
        graphState.dynamicGraph().V().add(4L, new ValueVertex<>("3", "6"));
        graphState.dynamicGraph().E().add(4L, new ValueEdge<>("1", "1", "6"));
        graphState.dynamicGraph().E().add(4L, new ValueEdge<>("1", "2", "6"));
        graphState.manage().operate().finish();
        graphState.manage().operate().archive();

        List<IEdge<String, String>> list = graphState.dynamicGraph().E().query(1L, "1").asList();
        Assert.assertEquals(list.size(), 2);

        list = graphState.dynamicGraph().E().query(1L, "1").by(
            (IEdgeFilter<String, String>) value -> !value.getTargetId().equals("2")).asList();
        Assert.assertEquals(list.size(), 1);
        Assert.assertEquals(list.get(0).getTargetId(), "3");

        Iterator<IVertex<String, String>> iterator = graphState.dynamicGraph().V().query(2L).iterator();
        Assert.assertEquals(Iterators.size(iterator), 2);

        IVertex<String, String> vertex = graphState.dynamicGraph().V().query(1L, "1").get();
        Assert.assertEquals(vertex.getValue(), "3");

        Assert.assertEquals(graphState.dynamicGraph().V().getLatestVersion("2"), 2L);
        Assert.assertEquals(graphState.dynamicGraph().V().getAllVersions("1").size(), 4);
        Assert.assertEquals(graphState.dynamicGraph().V().getLatestVersion("1"), 4);

        Map<Long, IVertex<String, String>> map = graphState.dynamicGraph().V().query("1").asMap();
        Assert.assertEquals(map.size(), 4);

        map = graphState.dynamicGraph().V().query("1", Arrays.asList(2L, 3L)).asMap();
        Assert.assertEquals(map.size(), 2);

        map = graphState.dynamicGraph().V().query("1", Arrays.asList(2L, 3L, 4L)).by(
            (IVertexFilter<String, String>) value -> !value.getValue().equals("5")).asMap();
        Assert.assertEquals(map.size(), 2);

        map = graphState.dynamicGraph().V().query("1").by(
            (IVertexFilter<String, String>) value -> !value.getValue().equals("5")).asMap();
        Assert.assertEquals(map.size(), 3);

        map = graphState.dynamicGraph().V().query("1", Arrays.asList(2L, 3L, 4L, 5L)).asMap();
        Assert.assertEquals(map.size(), 3);

        List<OneDegreeGraph<String, String, String>> res =
            graphState.dynamicGraph().VE().query(2L, "2").asList();
        Assert.assertEquals(res.size(), 1);

        res = graphState.dynamicGraph().VE().query(3L, "1").asList();
        Assert.assertEquals(res.size(), 1);

        Iterator<String> idIterator = graphState.dynamicGraph().V().idIterator();
        List<String> idList = Lists.newArrayList(idIterator);
        Assert.assertEquals(idList.size(), 3);

        res =
            graphState.dynamicGraph().VE().query(4L, "1").asList();
        Assert.assertEquals(res.size(), 1);
        Assert.assertEquals(Iterators.size(res.get(0).getEdgeIterator()), 2);

        res =
            graphState.dynamicGraph().VE().query(4L, "1")
                .by((IEdgeFilter<String, String>) value -> !value.getTargetId().equals("1")).asList();
        Assert.assertEquals(res.size(), 1);
        Assert.assertEquals(Iterators.size(res.get(0).getEdgeIterator()), 1);

        graphState.manage().operate().close();
        graphState.manage().operate().drop();
    }

    @Test
    public void testKeyGroup() {
        Map<String, String> conf = new HashMap<>(config);
        conf.put(PaimonConfigKeys.PAIMON_STORE_DISTRIBUTED_MODE_ENABLE.getKey(), "false");
        conf.put(PaimonConfigKeys.PAIMON_STORE_TABLE_AUTO_CREATE_ENABLE.getKey(), "true");
        GraphState<String, String, String> graphState = getGraphState(StringType.INSTANCE, "testKeyGroup", conf);

        graphState.manage().operate().setCheckpointId(1);

        for (int i = 0; i < 10; i++) {
            graphState.dynamicGraph().E().add(i, new ValueEdge<>("1", "2", "hello" + i));
            graphState.dynamicGraph().E().add(i, new ValueEdge<>("1", "3", "hello" + i));
            graphState.dynamicGraph().E().add(i, new ValueEdge<>("2", "2", "world" + i));
            graphState.dynamicGraph().E().add(i, new ValueEdge<>("2", "3", "world" + i));
            graphState.dynamicGraph().V().add(i, new ValueVertex<>("1", "3" + i));
            graphState.dynamicGraph().V().add(i, new ValueVertex<>("2", "4" + i));
            graphState.dynamicGraph().V().add(i, new ValueVertex<>("1", "5" + i));
            graphState.dynamicGraph().V().add(i, new ValueVertex<>("1", "6" + i));
        }

        graphState.manage().operate().finish();
        graphState.manage().operate().archive();

        Iterator<String> idIterator = graphState.dynamicGraph().V().idIterator();
        Assert.assertEquals(Iterators.size(idIterator), 2);

        idIterator = graphState.dynamicGraph().V().query(1L, new KeyGroup(0, 0)).idIterator();
        Assert.assertEquals(Iterators.size(idIterator), 1);

        List<IEdge<String, String>> list = graphState.dynamicGraph().E().query(1L,
            new KeyGroup(0, 0)).by((IEdgeFilter<String, String>) value -> !value.getTargetId().equals("2")).asList();
        Assert.assertEquals(list.size(), 1);
        Assert.assertEquals(list.get(0).getTargetId(), "3");

        graphState.manage().operate().close();
        graphState.manage().operate().drop();
    }

}
