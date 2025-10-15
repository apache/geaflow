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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.edge.impl.ValueEdge;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.model.graph.vertex.impl.ValueVertex;
import org.apache.geaflow.state.data.OneDegreeGraph;
import org.apache.geaflow.state.descriptor.GraphStateDescriptor;
import org.apache.geaflow.state.pushdown.filter.IEdgeFilter;
import org.apache.geaflow.state.pushdown.filter.IVertexFilter;
import org.apache.geaflow.utils.keygroup.DefaultKeyGroupAssigner;
import org.apache.geaflow.utils.keygroup.KeyGroup;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class MemoryDynamicGraphStateTest {

    private long ts;
    private String testName;
    private GraphState<String, String, String> graphState;

    @BeforeMethod
    public void setUp() {
        this.ts = System.currentTimeMillis();
        this.testName = "graph-state-test-" + ts;
        graphState = prepareData();
    }

    @Test
    public void test() {
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
        Assert.assertEquals(graphState.dynamicGraph().V().getAllVersions("1").size(), 3);

        Map<Long, IVertex<String, String>> map =
            graphState.dynamicGraph().V().query("1").asMap();
        Assert.assertEquals(map.size(), 3);

        map = graphState.dynamicGraph().V().query("1", Arrays.asList(2L, 3L)).asMap();
        Assert.assertEquals(map.size(), 2);

        map = graphState.dynamicGraph().V().query("1", Arrays.asList(2L, 3L)).by(
            (IVertexFilter<String, String>) value -> !value.getValue().equals("5")).asMap();
        Assert.assertEquals(map.size(), 1);

        map = graphState.dynamicGraph().V().query("1").by(
            (IVertexFilter<String, String>) value -> !value.getValue().equals("5")).asMap();
        Assert.assertEquals(map.size(), 2);

        map = graphState.dynamicGraph().V().query("1", Arrays.asList(2L, 3L, 4L)).asMap();
        Assert.assertEquals(map.size(), 2);

        List<OneDegreeGraph<String, String, String>> res =
            graphState.dynamicGraph().VE().query(2L, "2").asList();
        Assert.assertEquals(res.size(), 1);

        res = graphState.dynamicGraph().VE().query(3L, "1").asList();
        Assert.assertEquals(res.size(), 1);

        Iterator<String> idIterator = graphState.dynamicGraph().V().idIterator();
        List<String> idList = Lists.newArrayList(idIterator);
        Assert.assertEquals(idList.size(), 2);

        idIterator = graphState.dynamicGraph().V().query(2L, new KeyGroup(1, 1)).idIterator();
        Assert.assertEquals(Iterators.size(idIterator), 1);

        graphState.manage().operate().close();
        graphState.manage().operate().drop();
    }

    private GraphState<String, String, String> prepareData() {
        Map<String, String> config = new HashMap<>();

        GraphStateDescriptor desc = GraphStateDescriptor.build(testName, StoreType.MEMORY.name());
        desc.withDataModel(DataModel.DYNAMIC_GRAPH).withKeyGroup(new KeyGroup(0, 1))
            .withKeyGroupAssigner(new DefaultKeyGroupAssigner(2));
        GraphState<String, String, String> graphState = StateFactory.buildGraphState(desc,
            new Configuration(config));

        graphState.manage().operate().setCheckpointId(1);

        graphState.dynamicGraph().E().add(1L, new ValueEdge<>("1", "2", "hello"));
        graphState.dynamicGraph().E().add(1L, new ValueEdge<>("1", "3", "hello"));
        graphState.dynamicGraph().E().add(2L, new ValueEdge<>("2", "2", "world"));
        graphState.dynamicGraph().E().add(2L, new ValueEdge<>("2", "3", "world"));
        graphState.dynamicGraph().V().add(1L, new ValueVertex<>("1", "3"));
        graphState.dynamicGraph().V().add(2L, new ValueVertex<>("2", "4"));
        graphState.dynamicGraph().V().add(2L, new ValueVertex<>("1", "5"));
        graphState.dynamicGraph().V().add(3L, new ValueVertex<>("1", "6"));
        return graphState;
    }
}
