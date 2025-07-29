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

package org.apache.geaflow.operator.impl.graph.algo.vc.context.statical;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.geaflow.api.graph.function.vc.base.VertexCentricFunction.EdgeQuery;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.type.primitive.IntegerType;
import org.apache.geaflow.model.graph.edge.EdgeDirection;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.edge.impl.ValueEdge;
import org.apache.geaflow.model.graph.meta.GraphMeta;
import org.apache.geaflow.model.graph.meta.GraphMetaType;
import org.apache.geaflow.model.graph.vertex.impl.ValueVertex;
import org.apache.geaflow.state.GraphState;
import org.apache.geaflow.state.StateFactory;
import org.apache.geaflow.state.StoreType;
import org.apache.geaflow.state.descriptor.GraphStateDescriptor;
import org.apache.geaflow.state.pushdown.filter.InEdgeFilter;
import org.apache.geaflow.state.pushdown.filter.OutEdgeFilter;
import org.apache.geaflow.utils.keygroup.DefaultKeyGroupAssigner;
import org.apache.geaflow.utils.keygroup.KeyGroup;
import org.apache.geaflow.view.GraphViewBuilder;
import org.apache.geaflow.view.IViewDesc.BackendType;
import org.apache.geaflow.view.graph.GraphViewDesc;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class StaticEdgeQueryImplTest {

    private EdgeQuery<Integer, Integer> edgeQuery;
    private List<IEdge<Integer, Integer>> edges;

    @BeforeClass
    public void setup() {
        GraphStateDescriptor<Integer, Integer, Integer> desc =
            GraphStateDescriptor.build("test", StoreType.MEMORY.name());

        GraphMetaType graphMetaType = new GraphMetaType(IntegerType.INSTANCE, ValueVertex.class,
            Integer.class, ValueEdge.class, Integer.class);

        GraphViewDesc graphViewDesc = GraphViewBuilder.createGraphView("test").withBackend(
            BackendType.RocksDB).withSchema(graphMetaType).withShardNum(1).build();
        desc.withGraphMeta(new GraphMeta(graphViewDesc.getGraphMetaType()));
        desc.withKeyGroup(new KeyGroup(0, 1)).withKeyGroupAssigner(new DefaultKeyGroupAssigner(2));

        GraphState<Integer, Integer, Integer> graphState = StateFactory.buildGraphState(desc,
            new Configuration());

        graphState.staticGraph().V().add(new ValueVertex<>(0, 0));
        edges = new ArrayList<>();
        edges.add(new ValueEdge<>(0, 1, 1));
        edges.add(new ValueEdge<>(0, 2, 2));
        edges.add(new ValueEdge<>(0, 3, 3));
        edges.add(new ValueEdge<>(0, 4, 4));
        edges.add(new ValueEdge<>(0, 5, 5));
        edges.add(new ValueEdge<>(0, 6, 6, EdgeDirection.IN));
        edges.add(new ValueEdge<>(0, 7, 7, EdgeDirection.IN));
        edges.add(new ValueEdge<>(0, 8, 8, EdgeDirection.IN));
        edges.add(new ValueEdge<>(0, 9, 9, EdgeDirection.IN));
        edges.add(new ValueEdge<>(0, 10, 10, EdgeDirection.IN));

        for (IEdge<Integer, Integer> edge : edges) {
            graphState.staticGraph().E().add(edge);
        }

        edgeQuery = new StaticEdgeQueryImpl<>(0, graphState);
    }


    @Test
    public void testGetEdges() {
        List<IEdge<Integer, Integer>> result = edgeQuery.getEdges();
        Assert.assertEquals(result, edges);
    }

    @Test
    public void testGetOutEdges() {
        List<IEdge<Integer, Integer>> result = edgeQuery.getOutEdges();
        Assert.assertEquals(result,
            edges.stream().filter(x -> x.getDirect() == EdgeDirection.OUT).collect(
                Collectors.toList()));
    }

    @Test
    public void testGetInEdges() {
        List<IEdge<Integer, Integer>> result = edgeQuery.getInEdges();
        Assert.assertEquals(result,
            edges.stream().filter(x -> x.getDirect() == EdgeDirection.IN).collect(
                Collectors.toList()));
    }

    @Test
    public void testTestGetEdges() {
        CloseableIterator<IEdge<Integer, Integer>> outEdges = edgeQuery.getEdges(OutEdgeFilter.getInstance());
        List<IEdge<Integer, Integer>> outEdgesList = new ArrayList<>();
        outEdges.forEachRemaining(outEdgesList::add);
        Assert.assertEquals(outEdgesList,
            edges.stream().filter(x -> x.getDirect() == EdgeDirection.OUT).collect(
                Collectors.toList()));

        CloseableIterator<IEdge<Integer, Integer>> inEdges = edgeQuery.getEdges(InEdgeFilter.getInstance());
        List<IEdge<Integer, Integer>> inEdgesList = new ArrayList<>();
        inEdges.forEachRemaining(inEdgesList::add);
        Assert.assertEquals(inEdgesList,
            edges.stream().filter(x -> x.getDirect() == EdgeDirection.IN).collect(
                Collectors.toList()));
    }
}
