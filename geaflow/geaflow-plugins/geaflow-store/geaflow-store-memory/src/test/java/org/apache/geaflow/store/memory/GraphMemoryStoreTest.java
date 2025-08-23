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

package org.apache.geaflow.store.memory;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Supplier;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.type.primitive.IntegerType;
import org.apache.geaflow.common.type.primitive.StringType;
import org.apache.geaflow.model.graph.edge.EdgeDirection;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.edge.impl.IDEdge;
import org.apache.geaflow.model.graph.edge.impl.IDLabelEdge;
import org.apache.geaflow.model.graph.edge.impl.IDLabelTimeEdge;
import org.apache.geaflow.model.graph.edge.impl.IDTimeEdge;
import org.apache.geaflow.model.graph.edge.impl.ValueEdge;
import org.apache.geaflow.model.graph.edge.impl.ValueLabelEdge;
import org.apache.geaflow.model.graph.edge.impl.ValueLabelTimeEdge;
import org.apache.geaflow.model.graph.edge.impl.ValueTimeEdge;
import org.apache.geaflow.model.graph.meta.GraphElementMetas.GraphElementFlag;
import org.apache.geaflow.model.graph.meta.GraphMeta;
import org.apache.geaflow.model.graph.meta.GraphMetaType;
import org.apache.geaflow.model.graph.property.EmptyProperty;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.model.graph.vertex.impl.IDLabelTimeVertex;
import org.apache.geaflow.model.graph.vertex.impl.IDLabelVertex;
import org.apache.geaflow.model.graph.vertex.impl.IDTimeVertex;
import org.apache.geaflow.model.graph.vertex.impl.IDVertex;
import org.apache.geaflow.model.graph.vertex.impl.ValueLabelTimeVertex;
import org.apache.geaflow.model.graph.vertex.impl.ValueLabelVertex;
import org.apache.geaflow.model.graph.vertex.impl.ValueTimeVertex;
import org.apache.geaflow.model.graph.vertex.impl.ValueVertex;
import org.apache.geaflow.state.DataModel;
import org.apache.geaflow.state.data.OneDegreeGraph;
import org.apache.geaflow.state.pushdown.StatePushDown;
import org.apache.geaflow.state.pushdown.filter.IVertexFilter;
import org.apache.geaflow.state.pushdown.filter.VertexMustContainFilter;
import org.apache.geaflow.state.pushdown.filter.inner.GraphFilter;
import org.apache.geaflow.state.pushdown.filter.inner.IGraphFilter;
import org.apache.geaflow.state.schema.GraphDataSchema;
import org.apache.geaflow.store.api.graph.IStaticGraphStore;
import org.apache.geaflow.store.context.StoreContext;
import org.testng.Assert;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

public class GraphMemoryStoreTest {

    private final Configuration config;
    private final MemoryStoreBuilder builder;

    public GraphMemoryStoreTest(Configuration config) {
        this.config = config;
        this.builder = new MemoryStoreBuilder();
    }

    public static class GraphMemoryStoreTestFactory {

        @Factory
        public Object[] factoryMethod() {
            return new Object[]{
                new GraphMemoryStoreTest(new Configuration()),
                new GraphMemoryStoreTest(new Configuration(ImmutableMap.of(MemoryConfigKeys.CSR_MEMORY_ENABLE.getKey(), "true"))),
            };
        }
    }

    @Test
    public void test() {
        IStaticGraphStore<String, byte[], byte[]> store =
            (IStaticGraphStore<String, byte[], byte[]>) builder.getStore(DataModel.STATIC_GRAPH, config);
        StoreContext storeContext = new StoreContext("test")
            .withConfig(new Configuration())
            .withDataSchema(new GraphDataSchema(new GraphMeta(
                new GraphMetaType<>(StringType.INSTANCE, ValueVertex.class, byte[].class, ValueEdge.class, byte[].class))));
        store.init(storeContext);

        for (int i = 0; i < 1000000; i++) {
            String value = System.currentTimeMillis() + Math.random() + "";
            IVertex<String, byte[]> vertex = new ValueVertex<>("hello" + i, value.getBytes());
            store.addVertex(vertex);
            IEdge<String, byte[]> edge = new ValueEdge<>("hello" + i, "hello" + (i + 1),
                ("hello" + (i + 1)).getBytes());
            store.addEdge(edge);
        }
        store.flush();

        Assert.assertEquals(Iterators.size(store.getVertexIterator(StatePushDown.of())), 1000000);
        List<IEdge<String, byte[]>> list = store.getEdges("hello" + 0, StatePushDown.of());
        Assert.assertEquals(list.size(), 1);
        Assert.assertEquals(new String(list.get(0).getValue()), "hello1");
    }

    @Test
    public void testGetOneDegreeGraphList() {
        IStaticGraphStore<Integer, Integer, Integer> store =
            (IStaticGraphStore<Integer, Integer, Integer>) builder.getStore(DataModel.STATIC_GRAPH, config);
        StoreContext storeContext =
            new StoreContext("test")
                .withConfig(new Configuration())
                .withDataSchema(new GraphDataSchema(new GraphMeta(
                    new GraphMetaType<>(IntegerType.INSTANCE, IDVertex.class, EmptyProperty.class,
                        IDEdge.class, EmptyProperty.class))));
        store.init(storeContext);

        IVertex vertex = new IDVertex<>(1);
        store.addVertex(vertex);

        IEdge edge = new IDEdge<>(2, 1);
        edge.setDirect(EdgeDirection.IN);
        store.addEdge(edge);

        store.flush();

        List<OneDegreeGraph<Integer, Integer, Integer>> list =
            Lists.newArrayList(store.getOneDegreeGraphIterator(StatePushDown.of()));
        Assert.assertEquals(list.size(), 2);

        list =
            Lists.newArrayList(store.getOneDegreeGraphIterator(StatePushDown.of().withFilter(
                GraphFilter.of(VertexMustContainFilter.getInstance()))));
        Assert.assertEquals(list.size(), 1);

        list =
            Lists.newArrayList(store.getOneDegreeGraphIterator(Arrays.asList(1, -1), StatePushDown.of()));
        Assert.assertEquals(list.size(), 2);
    }

    @Test
    public void testGetVertexList() {
        IStaticGraphStore<Integer, Integer, Integer> store =
            (IStaticGraphStore<Integer, Integer, Integer>) builder.getStore(DataModel.STATIC_GRAPH, config);
        StoreContext storeContext =
            new StoreContext("test")
                .withConfig(new Configuration())
                .withDataSchema(new GraphDataSchema(new GraphMeta(
                    new GraphMetaType<>(IntegerType.INSTANCE, IDVertex.class, EmptyProperty.class,
                        IDEdge.class, EmptyProperty.class))));
        store.init(storeContext);

        IVertex vertex = new IDVertex<>(1);
        store.addVertex(vertex);
        store.flush();

        List<IVertex<Integer, Integer>> vertexList =
            Lists.newArrayList(store.getVertexIterator(StatePushDown.of()));
        Assert.assertEquals(vertexList.size(), 1);
        Assert.assertEquals(vertexList.get(0).getId(), vertex.getId());
        vertexList.clear();

        vertexList = Lists.newArrayList(store.getVertexIterator(StatePushDown.of()));
        Assert.assertEquals(vertexList.size(), 1);

        vertexList =
            Lists.newArrayList(store.getVertexIterator(Arrays.asList(1, -1), StatePushDown.of()));
        Assert.assertEquals(vertexList.size(), 1);

        Map<Integer, IGraphFilter> keyFilters = new HashMap<>(2);
        keyFilters.put(0, GraphFilter.of((IVertexFilter<Integer, Integer>) value -> value.getId() != 2));
        keyFilters.put(1, GraphFilter.of((IVertexFilter<Integer, Integer>) value -> value.getId() != 2));
        vertexList =
            Lists.newArrayList(store.getVertexIterator(Arrays.asList(1, -1), StatePushDown.of().withFilters(keyFilters)));
        Assert.assertEquals(vertexList.size(), 1);

        Iterator<IEdge<Integer, Integer>> it = store.getEdgeIterator(StatePushDown.of());
        Assert.assertFalse(it.hasNext());
    }

    @Test
    public void testGetEdgeList() {
        IStaticGraphStore<Integer, Integer, Integer> store =
            (IStaticGraphStore<Integer, Integer, Integer>) builder.getStore(DataModel.STATIC_GRAPH, config);
        StoreContext storeContext =
            new StoreContext("test")
                .withConfig(new Configuration())
                .withDataSchema(new GraphDataSchema(new GraphMeta(
                    new GraphMetaType<>(IntegerType.INSTANCE, ValueVertex.class, Integer.class,
                        ValueEdge.class, Integer.class))));
        store.init(storeContext);

        store.addEdge(new ValueEdge<>(1, 1, 1));
        store.flush();

        List<IEdge<Integer, Integer>> edgeList = store.getEdges(1, StatePushDown.of());
        Assert.assertEquals(edgeList.size(), 1);

        edgeList = store.getEdges(0, StatePushDown.of());
        Assert.assertEquals(edgeList.size(), 0);

        Iterator<IEdge<Integer, Integer>> it = store.getEdgeIterator(Arrays.asList(1, -1), StatePushDown.of());
        edgeList = Lists.newArrayList(it);
        Assert.assertEquals(edgeList.size(), 1);

        Iterator<IVertex<Integer, Integer>> vIt = store.getVertexIterator(StatePushDown.of());
        Assert.assertFalse(vIt.hasNext());
    }

    private IVertex getVertex(GraphMeta graphMeta) {
        GraphElementFlag flag = GraphElementFlag.build(graphMeta.getVertexMeta().getGraphElementClass());
        boolean noProperty = graphMeta.getVertexMeta().getPropertyClass() == EmptyProperty.class;
        Random random = new Random();
        IVertex vertex;
        String label = Integer.toString(random.nextInt(10));
        long time = random.nextInt();
        int property = random.nextInt();
        int srcid = random.nextInt();

        if (flag.isLabeledAndTimed()) {
            vertex = noProperty ? new IDLabelTimeVertex<>(srcid, label, time) :
                new ValueLabelTimeVertex<>(srcid, property, label, time);
        } else if (flag.isLabeled()) {
            vertex = noProperty ? new IDLabelVertex<>(srcid, label) :
                new ValueLabelVertex<>(srcid, property, label);
        } else if (flag.isTimed()) {
            vertex = noProperty ? new IDTimeVertex<>(srcid, time) : new ValueTimeVertex<>(srcid, property, time);
        } else {
            vertex = noProperty ? new IDVertex<>(srcid) : new ValueVertex<>(srcid, property);
        }
        return vertex;
    }

    private IEdge getEdge(GraphMeta graphMeta) {
        GraphElementFlag flag = GraphElementFlag.build(graphMeta.getEdgeMeta().getGraphElementClass());
        boolean noProperty = graphMeta.getEdgeMeta().getPropertyClass() == EmptyProperty.class;
        Random random = new Random();
        IEdge edge;
        String label = Integer.toString(random.nextInt(10));
        long time = random.nextInt();
        int property = random.nextInt();
        int id = random.nextInt();

        if (flag.isLabeledAndTimed()) {
            edge = noProperty ? new IDLabelTimeEdge<>(id, id, label, time) :
                new ValueLabelTimeEdge<>(id, id, property, label, time);
        } else if (flag.isLabeled()) {
            edge = noProperty ? new IDLabelEdge<>(id, id, label) : new ValueLabelEdge<>(id, id, property, label);
        } else if (flag.isTimed()) {
            edge = noProperty ? new IDTimeEdge<>(id, id, time) : new ValueTimeEdge<>(id, id, property, time);
        } else {
            edge = noProperty ? new IDEdge<>(id, id) : new ValueEdge<>(id, property);
        }
        return edge;
    }

    @Test
    public void testDifferentType() {
        IStaticGraphStore<Integer, Integer, Integer> store =
            (IStaticGraphStore<Integer, Integer, Integer>) builder.getStore(DataModel.STATIC_GRAPH,
                config);


        List<Class> vertexClass = Arrays.asList(
            ValueLabelTimeVertex.class,
            IDLabelTimeVertex.class,
            ValueLabelVertex.class,
            IDLabelVertex.class,
            ValueTimeVertex.class,
            IDTimeVertex.class,
            ValueVertex.class,
            IDVertex.class);
        List<Supplier<?>> vertexConstructs = Arrays.asList(
            ValueLabelTimeVertex::new,
            IDLabelTimeVertex::new,
            ValueLabelVertex::new,
            IDLabelVertex::new,
            ValueTimeVertex::new,
            IDTimeVertex::new,
            ValueVertex::new,
            IDVertex::new);
        List<Class> edgeClass = Arrays.asList(
            ValueLabelTimeEdge.class,
            IDLabelTimeEdge.class,
            ValueLabelEdge.class,
            IDLabelEdge.class,
            ValueTimeEdge.class,
            IDTimeEdge.class,
            ValueEdge.class,
            IDEdge.class);
        List<Supplier<?>> edgeConstructs = Arrays.asList(
            ValueLabelTimeEdge::new,
            IDLabelTimeEdge::new,
            ValueLabelEdge::new,
            IDLabelEdge::new,
            ValueTimeEdge::new,
            IDTimeEdge::new,
            ValueEdge::new,
            IDEdge::new);
        for (int i = 0; i < vertexClass.size(); i++) {
            Class propertyClazz = i % 2 == 0 ? Integer.class : EmptyProperty.class;
            GraphMeta graphMeta = new GraphMeta(
                new GraphMetaType(IntegerType.INSTANCE, vertexClass.get(i), vertexConstructs.get(i),
                    propertyClazz, edgeClass.get(i), edgeConstructs.get(i), propertyClazz));
            StoreContext storeContext =
                new StoreContext("test").withConfig(new Configuration()).withDataSchema(new GraphDataSchema(graphMeta));
            store.init(storeContext);

            IVertex vertex = getVertex(graphMeta);
            IEdge edge = getEdge(graphMeta);
            store.addVertex(vertex);
            store.addEdge(edge);

            store.flush();
            Iterator<OneDegreeGraph<Integer, Integer, Integer>> it = store.getOneDegreeGraphIterator(
                StatePushDown.of());
            while (it.hasNext()) {
                OneDegreeGraph<Integer, Integer, Integer> next = it.next();
                if (next.getVertex() != null) {
                    Assert.assertEquals(next.getVertex(), vertex);
                }
                if (next.getEdgeIterator().hasNext()) {
                    Assert.assertEquals(next.getEdgeIterator().next(), edge);
                }
            }
        }
    }
}
