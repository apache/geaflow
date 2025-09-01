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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.common.config.keys.StateConfigKeys;
import org.apache.geaflow.common.type.Types;
import org.apache.geaflow.common.utils.GsonUtil;
import org.apache.geaflow.file.FileConfigKeys;
import org.apache.geaflow.model.graph.edge.EdgeDirection;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.edge.impl.ValueEdge;
import org.apache.geaflow.model.graph.meta.GraphMeta;
import org.apache.geaflow.model.graph.meta.GraphMetaType;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.model.graph.vertex.impl.ValueVertex;
import org.apache.geaflow.state.descriptor.GraphStateDescriptor;
import org.apache.geaflow.state.graph.StateMode;
import org.apache.geaflow.state.manage.LoadOption;
import org.apache.geaflow.state.pushdown.filter.IEdgeFilter;
import org.apache.geaflow.utils.keygroup.DefaultKeyGroupAssigner;
import org.apache.geaflow.utils.keygroup.KeyGroup;
import org.apache.geaflow.view.meta.ViewMetaBookKeeper;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class GraphReadOnlyStateTest {

    private Map<String, String> config;

    @BeforeMethod
    public void setUp() throws IOException {
        FileUtils.deleteQuietly(new File("/tmp/geaflow/chk/"));
        FileUtils.deleteQuietly(new File("/tmp/RocksDBGraphStateTest"));
        Map<String, String> persistConfig = new HashMap<>();
        config = new HashMap<>();
        config.put(ExecutionConfigKeys.JOB_APP_NAME.getKey(),
            "RocksDBGraphStateTest" + System.currentTimeMillis());
        config.put(FileConfigKeys.PERSISTENT_TYPE.getKey(), "LOCAL");
        config.put(FileConfigKeys.ROOT.getKey(), "/tmp/geaflow/chk/");
        config.put(FileConfigKeys.JSON_CONFIG.getKey(), GsonUtil.toJson(persistConfig));
    }

    @Test
    public void testStaticReadOnlyState() {
        GraphStateDescriptor<String, String, String> desc = GraphStateDescriptor.build("test1", StoreType.ROCKSDB.name());
        desc.withKeyGroup(new KeyGroup(0, 1)).withKeyGroupAssigner(new DefaultKeyGroupAssigner(2));
        desc.withGraphMeta(new GraphMeta(new GraphMetaType<>(Types.STRING, ValueVertex.class,
            String.class, ValueEdge.class, String.class)));

        GraphState<String, String, String> graphState = StateFactory.buildGraphState(desc, new Configuration(config));

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

        list = graphState.staticGraph().E().query("1").by(
            (IEdgeFilter<String, String>) value -> !value.getTargetId().equals("2")).asList();
        Assert.assertEquals(list.size(), 1);
        Assert.assertEquals(list.get(0).getTargetId(), "3");

        Iterator<IVertex<String, String>> iterator = graphState.staticGraph().V().iterator();
        Assert.assertEquals(Iterators.size(iterator), 2);

        IVertex<String, String> vertex = graphState.staticGraph().V().query("1").get();
        Assert.assertEquals(vertex.getValue(), "3");

        graphState.manage().operate().archive();
        graphState.manage().operate().close();
        graphState.manage().operate().drop();


        desc = GraphStateDescriptor.build("test1", StoreType.ROCKSDB.name());
        desc.withKeyGroup(new KeyGroup(0, 1)).withKeyGroupAssigner(new DefaultKeyGroupAssigner(2));
        desc.withGraphMeta(new GraphMeta(new GraphMetaType<>(Types.STRING, ValueVertex.class,
            String.class, ValueEdge.class, String.class)));
        desc.withStateMode(StateMode.RDONLY);

        graphState = StateFactory.buildGraphState(desc, new Configuration(config));
        graphState.manage().operate().setCheckpointId(1);
        graphState.manage().operate().recover();

        list = graphState.staticGraph().E().query("1").asList();
        Assert.assertEquals(list.size(), 2);

        list = graphState.staticGraph().E().query("1").by(
            (IEdgeFilter<String, String>) value -> !value.getTargetId().equals("2")).asList();
        Assert.assertEquals(list.size(), 1);
        Assert.assertEquals(list.get(0).getTargetId(), "3");

        iterator = graphState.staticGraph().V().iterator();
        Assert.assertEquals(Iterators.size(iterator), 2);

        vertex = graphState.staticGraph().V().query("1").get();
        Assert.assertEquals(vertex.getValue(), "3");

        graphState.manage().operate().drop();
    }

    @Test
    public void testDynamicReadOnlyState() {
        GraphStateDescriptor<String, String, String> desc = GraphStateDescriptor.build("test2", StoreType.ROCKSDB.name());
        desc.withKeyGroup(new KeyGroup(0, 1)).withKeyGroupAssigner(new DefaultKeyGroupAssigner(2));
        desc.withGraphMeta(new GraphMeta(new GraphMetaType<>(Types.STRING, ValueVertex.class,
            String.class, ValueEdge.class, String.class)));
        desc.withDataModel(DataModel.DYNAMIC_GRAPH);

        GraphState<String, String, String> graphState = StateFactory.buildGraphState(desc, new Configuration(config));

        graphState.manage().operate().setCheckpointId(1);

        graphState.dynamicGraph().E().add(1, new ValueEdge<>("1", "2", "hello", EdgeDirection.IN));
        graphState.dynamicGraph().E().add(2, new ValueEdge<>("1", "3", "hello", EdgeDirection.OUT));
        graphState.dynamicGraph().E().add(1, new ValueEdge<>("2", "2", "world", EdgeDirection.IN));
        graphState.dynamicGraph().E().add(2, new ValueEdge<>("2", "3", "world", EdgeDirection.OUT));
        graphState.dynamicGraph().V().add(1, new ValueVertex<>("1", "3"));
        graphState.dynamicGraph().V().add(1, new ValueVertex<>("2", "4"));
        graphState.dynamicGraph().V().add(2, new ValueVertex<>("1", "5"));
        graphState.dynamicGraph().V().add(2, new ValueVertex<>("2", "6"));
        graphState.manage().operate().finish();

        List<IEdge<String, String>> list =
            graphState.dynamicGraph().E().query(1L, Arrays.asList("1", "2")).asList();
        Assert.assertEquals(list.size(), 2);

        long version = graphState.dynamicGraph().V().getLatestVersion("2");
        Assert.assertEquals(version, 2);

        IVertex<String, String> vertex = graphState.dynamicGraph().V().query(2, "1").get();
        Assert.assertEquals(vertex.getValue(), "5");

        graphState.manage().operate().archive();
        graphState.manage().operate().close();
        graphState.manage().operate().drop();


        desc = GraphStateDescriptor.build("test2", StoreType.ROCKSDB.name());
        desc.withKeyGroup(new KeyGroup(0, 1)).withKeyGroupAssigner(new DefaultKeyGroupAssigner(2));
        desc.withGraphMeta(new GraphMeta(new GraphMetaType<>(Types.STRING, ValueVertex.class,
            String.class, ValueEdge.class, String.class)));
        desc.withDataModel(DataModel.DYNAMIC_GRAPH).withStateMode(StateMode.RDONLY);

        graphState = StateFactory.buildGraphState(desc, new Configuration(config));
        graphState.manage().operate().setCheckpointId(1);
        graphState.manage().operate().recover();

        list = graphState.dynamicGraph().E().query(1L, Arrays.asList("1", "2")).asList();
        Assert.assertEquals(list.size(), 2);

        version = graphState.dynamicGraph().V().getLatestVersion("2");
        Assert.assertEquals(version, 2);

        vertex = graphState.dynamicGraph().V().query(2, "1").get();
        Assert.assertEquals(vertex.getValue(), "5");

        graphState.manage().operate().archive();
        graphState.manage().operate().close();
        graphState.manage().operate().drop();
    }

    @Test(invocationCount = 5)
    public void testReadOnlyStateWarmup() throws IOException {
        GraphStateDescriptor<String, String, String> desc = GraphStateDescriptor.build("test1", StoreType.ROCKSDB.name());
        desc.withKeyGroup(new KeyGroup(0, 1)).withKeyGroupAssigner(new DefaultKeyGroupAssigner(2));
        desc.withGraphMeta(new GraphMeta(new GraphMetaType<>(Types.STRING, ValueVertex.class,
            String.class, ValueEdge.class, String.class)));

        Configuration configuration = new Configuration(config);
        ViewMetaBookKeeper viewMetaBookKeeper = new ViewMetaBookKeeper("test1", configuration);
        GraphState<String, String, String> graphState = StateFactory.buildGraphState(desc, configuration);

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

        list = graphState.staticGraph().E().query("1").by(
            (IEdgeFilter<String, String>) value -> !value.getTargetId().equals("2")).asList();
        Assert.assertEquals(list.size(), 1);
        Assert.assertEquals(list.get(0).getTargetId(), "3");

        Iterator<IVertex<String, String>> iterator = graphState.staticGraph().V().iterator();
        Assert.assertEquals(Iterators.size(iterator), 2);

        IVertex<String, String> vertex = graphState.staticGraph().V().query("1").get();
        Assert.assertEquals(vertex.getValue(), "3");

        graphState.manage().operate().archive();
        graphState.manage().operate().close();
        graphState.manage().operate().drop();

        viewMetaBookKeeper.saveViewVersion(1);
        viewMetaBookKeeper.archive();


        desc = GraphStateDescriptor.build("test1", StoreType.ROCKSDB.name());
        desc.withKeyGroup(new KeyGroup(0, 1)).withKeyGroupAssigner(new DefaultKeyGroupAssigner(2));
        desc.withGraphMeta(new GraphMeta(new GraphMetaType<>(Types.STRING, ValueVertex.class,
            String.class, ValueEdge.class, String.class)));
        desc.withStateMode(StateMode.RDONLY);

        config.put(StateConfigKeys.STATE_BACKGROUND_SYNC_ENABLE.getKey(), "true");
        config.put(StateConfigKeys.STATE_RECOVER_LATEST_VERSION_ENABLE.getKey(), "true");

        graphState = StateFactory.buildGraphState(desc, new Configuration(config));
        graphState.manage().operate().load(LoadOption.of());

        list = graphState.staticGraph().E().query("1").asList();
        Assert.assertEquals(list.size(), 2);

        list = graphState.staticGraph().E().query("1").by(
            (IEdgeFilter<String, String>) value -> !value.getTargetId().equals("2")).asList();
        Assert.assertEquals(list.size(), 1);
        Assert.assertEquals(list.get(0).getTargetId(), "3");

        iterator = graphState.staticGraph().V().iterator();
        Assert.assertEquals(Iterators.size(iterator), 2);

        vertex = graphState.staticGraph().V().query("1").get();
        Assert.assertEquals(vertex.getValue(), "3");

        graphState.manage().operate().drop();
        viewMetaBookKeeper.archive();
    }
}