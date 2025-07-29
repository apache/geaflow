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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.type.Types;
import org.apache.geaflow.model.graph.edge.EdgeDirection;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.edge.impl.ValueEdge;
import org.apache.geaflow.model.graph.edge.impl.ValueLabelTimeEdge;
import org.apache.geaflow.model.graph.meta.GraphMeta;
import org.apache.geaflow.model.graph.meta.GraphMetaType;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.model.graph.vertex.impl.ValueVertex;
import org.apache.geaflow.state.data.TimeRange;
import org.apache.geaflow.state.descriptor.GraphStateDescriptor;
import org.apache.geaflow.state.pushdown.filter.EdgeTsFilter;
import org.apache.geaflow.state.pushdown.filter.IEdgeFilter;
import org.apache.geaflow.state.pushdown.filter.InEdgeFilter;
import org.apache.geaflow.state.pushdown.project.DstIdProjector;
import org.apache.geaflow.store.memory.MemoryConfigKeys;
import org.apache.geaflow.utils.keygroup.DefaultKeyGroupAssigner;
import org.apache.geaflow.utils.keygroup.KeyGroup;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

public class MemoryGraphStateTest {

    private final Map<String, String> additionalConfig;
    private long ts;
    private String testName;

    @BeforeMethod
    public void setUp() {
        this.ts = System.currentTimeMillis();
        this.testName = "graph-state-test-" + ts;
    }

    @AfterMethod
    public void tearDown() throws IOException {
        FileUtils.deleteQuietly(new File("/tmp/" + testName));
    }

    public MemoryGraphStateTest(Map<String, String> config) {
        this.additionalConfig = config;
    }

    public static class GraphMemoryStoreTestFactory {

        @Factory
        public Object[] factoryMethod() {
            return new Object[]{
                new MemoryGraphStateTest(new HashMap<>()),
                new MemoryGraphStateTest(
                    ImmutableMap.of(MemoryConfigKeys.CSR_MEMORY_ENABLE.getKey(), "true")),
            };
        }
    }

    @Test
    public void test() {
        GraphStateDescriptor<String, String, String> desc = GraphStateDescriptor.build("test1", StoreType.MEMORY.name());
        desc.withKeyGroup(new KeyGroup(0, 0)).withKeyGroupAssigner(new DefaultKeyGroupAssigner(1));
        desc.withGraphMeta(new GraphMeta(new GraphMetaType<>(Types.STRING, ValueVertex.class,
            String.class, ValueEdge.class, String.class)));
        Map<String, String> config = new HashMap<>(additionalConfig);

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


        Iterator<String> idIterator = graphState.staticGraph().V().idIterator();
        List<String> idList = Lists.newArrayList(idIterator);
        Assert.assertEquals(idList.size(), 2);

        graphState.manage().operate().close();
        graphState.manage().operate().drop();
    }

    @Test
    public void testFilter() {
        GraphStateDescriptor<String, String, String> desc = GraphStateDescriptor.build("test1", StoreType.MEMORY.name());
        desc.withKeyGroup(new KeyGroup(0, 0)).withKeyGroupAssigner(new DefaultKeyGroupAssigner(1));
        desc.withGraphMeta(new GraphMeta(new GraphMetaType<>(Types.STRING, ValueVertex.class,
            String.class, ValueLabelTimeEdge.class, String.class)));
        Map<String, String> config = new HashMap<>(additionalConfig);

        GraphState<String, String, String> graphState = StateFactory.buildGraphState(desc, new Configuration(config));

        graphState.manage().operate().setCheckpointId(1);

        graphState.staticGraph().E().add(new ValueLabelTimeEdge<>("1", "2", "hello", "foo", 1000));
        graphState.staticGraph().E().add(new ValueLabelTimeEdge<>("1", "3", "hello", "bar", 100));
        graphState.staticGraph().E().add(new ValueLabelTimeEdge<>("2", "2", "world", "foo", 1000));
        graphState.staticGraph().E().add(new ValueLabelTimeEdge<>("2", "3", "world", "bar", 100));
        graphState.staticGraph().V().add(new ValueVertex<>("1", "3"));
        graphState.staticGraph().V().add(new ValueVertex<>("2", "4"));

        graphState.manage().operate().finish();

        List<IEdge<String, String>> list = graphState.staticGraph().E().query("1", "2").by(
            new EdgeTsFilter<>(TimeRange.of(0, 500))).asList();
        Assert.assertEquals(list.size(), 2);

        list = graphState.staticGraph().E().query("1", "2").by(
            new EdgeTsFilter<>(TimeRange.of(0, 500)).or(new EdgeTsFilter<>(TimeRange.of(800,
                1100)))).asList();
        Assert.assertEquals(list.size(), 4);

        graphState.manage().operate().close();
        graphState.manage().operate().drop();
    }

    @Test
    public void testLimit() {
        GraphStateDescriptor<String, String, String> desc = GraphStateDescriptor.build("test1", StoreType.MEMORY.name());
        desc.withKeyGroup(new KeyGroup(0, 0)).withKeyGroupAssigner(new DefaultKeyGroupAssigner(1));
        desc.withGraphMeta(new GraphMeta(new GraphMetaType<>(Types.STRING, ValueVertex.class,
            String.class, ValueEdge.class, String.class)));
        Map<String, String> config = new HashMap<>(additionalConfig);

        GraphState<String, String, String> graphState = StateFactory.buildGraphState(desc, new Configuration(config));

        graphState.manage().operate().setCheckpointId(1);
        for (int i = 0; i < 10; i++) {
            String src = Integer.toString(i);
            for (int j = 1; j < 10; j++) {
                String dst = Integer.toString(j);
                graphState.staticGraph().E().add(new ValueEdge<>(src, dst, "hello" + src + dst,
                    EdgeDirection.values()[j % 2]));
            }
            graphState.staticGraph().V().add(new ValueVertex<>(src, "world" + src));
        }
        graphState.manage().operate().finish();

        List<IEdge<String, String>> list =
            graphState.staticGraph().E().query("1", "2", "3")
                .limit(1L, 1L).asList();
        System.out.println(list);
        Assert.assertEquals(list.size(), 6);

        list =
            graphState.staticGraph().E().query().by(InEdgeFilter.getInstance())
                .limit(1L, 1L).asList();
        Assert.assertEquals(list.size(), 10);

        list = graphState.staticGraph().E().query().by(InEdgeFilter.getInstance())
            .limit(1L, 2L).asList();
        Assert.assertEquals(list.size(), 20);

        List<String> targetIds =
            graphState.staticGraph().E().query().by(InEdgeFilter.getInstance())
                .select(new DstIdProjector<>()).limit(1L, 2L).asList();

        Assert.assertEquals(targetIds.size(), 20);
        graphState.manage().operate().close();
        graphState.manage().operate().drop();
    }
}
