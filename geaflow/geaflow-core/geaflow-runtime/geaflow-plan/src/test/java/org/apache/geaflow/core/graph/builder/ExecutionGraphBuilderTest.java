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

package org.apache.geaflow.core.graph.builder;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.geaflow.api.collector.Collector;
import org.apache.geaflow.api.function.base.AggregateFunction;
import org.apache.geaflow.api.function.base.FlatMapFunction;
import org.apache.geaflow.api.function.base.ReduceFunction;
import org.apache.geaflow.api.function.internal.CollectionSource;
import org.apache.geaflow.api.graph.PGraphWindow;
import org.apache.geaflow.api.graph.compute.IncVertexCentricCompute;
import org.apache.geaflow.api.graph.compute.VertexCentricCompute;
import org.apache.geaflow.api.graph.function.vc.IncVertexCentricComputeFunction;
import org.apache.geaflow.api.graph.function.vc.VertexCentricCombineFunction;
import org.apache.geaflow.api.graph.function.vc.VertexCentricComputeFunction;
import org.apache.geaflow.api.graph.function.vc.VertexCentricTraversalFunction;
import org.apache.geaflow.api.graph.traversal.VertexCentricTraversal;
import org.apache.geaflow.api.pdata.PStreamSink;
import org.apache.geaflow.api.pdata.PStreamSource;
import org.apache.geaflow.api.pdata.stream.window.PWindowSource;
import org.apache.geaflow.api.pdata.stream.window.PWindowStream;
import org.apache.geaflow.api.window.WindowFactory;
import org.apache.geaflow.api.window.impl.AllWindow;
import org.apache.geaflow.api.window.impl.SizeTumblingWindow;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.FrameworkConfigKeys;
import org.apache.geaflow.common.tuple.Tuple;
import org.apache.geaflow.common.type.primitive.IntegerType;
import org.apache.geaflow.context.AbstractPipelineContext;
import org.apache.geaflow.core.graph.CycleGroupType;
import org.apache.geaflow.core.graph.ExecutionGraph;
import org.apache.geaflow.core.graph.ExecutionVertexGroup;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.edge.impl.ValueEdge;
import org.apache.geaflow.model.graph.meta.GraphMetaType;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.model.graph.vertex.impl.ValueVertex;
import org.apache.geaflow.model.traversal.ITraversalRequest;
import org.apache.geaflow.model.traversal.impl.VertexBeginTraversalRequest;
import org.apache.geaflow.operator.impl.window.MapOperator;
import org.apache.geaflow.operator.impl.window.SinkOperator;
import org.apache.geaflow.pdata.graph.view.IncGraphView;
import org.apache.geaflow.pdata.graph.window.WindowStreamGraph;
import org.apache.geaflow.pdata.stream.window.WindowDataStream;
import org.apache.geaflow.pdata.stream.window.WindowStreamSink;
import org.apache.geaflow.pdata.stream.window.WindowStreamSource;
import org.apache.geaflow.plan.PipelinePlanBuilder;
import org.apache.geaflow.plan.graph.AffinityLevel;
import org.apache.geaflow.plan.graph.PipelineGraph;
import org.apache.geaflow.plan.optimizer.PipelineGraphOptimizer;
import org.apache.geaflow.plan.optimizer.strategy.ChainCombiner;
import org.apache.geaflow.view.GraphViewBuilder;
import org.apache.geaflow.view.IViewDesc;
import org.apache.geaflow.view.IViewDesc.BackendType;
import org.apache.geaflow.view.graph.GraphViewDesc;
import org.apache.geaflow.view.graph.PGraphView;
import org.apache.geaflow.view.graph.PIncGraphView;
import org.junit.Assert;
import org.junit.Test;

public class ExecutionGraphBuilderTest {

    @Test
    public void testSingleOutput() {
        AtomicInteger idGenerator = new AtomicInteger(0);
        AbstractPipelineContext context = mock(AbstractPipelineContext.class);
        Configuration configuration = new Configuration();
        configuration.put(FrameworkConfigKeys.INC_STREAM_MATERIALIZE_DISABLE, Boolean.TRUE.toString());
        when(context.getConfig()).thenReturn(configuration);
        when(context.generateId()).then(invocation -> idGenerator.incrementAndGet());
        PStreamSink sink = new WindowStreamSource<>(context,
            new CollectionSource<>(new ArrayList<>()), SizeTumblingWindow.of(2))
            .map(p -> p).withParallelism(2).keyBy(q -> q).reduce((oldValue, newValue) -> (oldValue))
            .withParallelism(3).sink(p -> {
            }).withParallelism(3);
        when(context.getActions()).thenReturn(ImmutableList.of(sink));

        PipelinePlanBuilder planBuilder = new PipelinePlanBuilder();
        PipelineGraph pipelineGraph = planBuilder.buildPlan(context);

        ExecutionGraphBuilder builder = new ExecutionGraphBuilder(pipelineGraph);
        ExecutionGraph graph = builder.buildExecutionGraph(new Configuration());

        Assert.assertEquals(1, graph.getVertexGroupMap().size());
        Assert.assertEquals(1, graph.getCycleGroupMeta().getFlyingCount());
        Assert.assertEquals(1, graph.getCycleGroupMeta().getIterationCount());
        ExecutionVertexGroup vertexGroup = graph.getVertexGroupMap().values().stream().findFirst().get();
        Assert.assertEquals(5, vertexGroup.getVertexMap().size());
        Assert.assertEquals(Long.MAX_VALUE, vertexGroup.getCycleGroupMeta().getIterationCount());
        Assert.assertEquals(5, vertexGroup.getCycleGroupMeta().getFlyingCount());
        Assert.assertFalse(vertexGroup.getCycleGroupMeta().isIterative());
        Assert.assertTrue(vertexGroup.getCycleGroupMeta().getAffinityLevel() == AffinityLevel.worker);
        Assert.assertTrue(vertexGroup.getVertexMap().values().stream().anyMatch(
            vertex -> vertex.getAffinityLevel() == AffinityLevel.worker));
    }

    @Test
    public void testMultiOutput() {
        AtomicInteger idGenerator = new AtomicInteger(0);
        AbstractPipelineContext context = mock(AbstractPipelineContext.class);
        when(context.generateId()).then(invocation -> idGenerator.incrementAndGet());
        Configuration configuration = new Configuration();
        configuration.put(FrameworkConfigKeys.INC_STREAM_MATERIALIZE_DISABLE, Boolean.TRUE.toString());
        when(context.getConfig()).thenReturn(configuration);
        WindowStreamSource ds1 = new WindowStreamSource<>(context,
            new CollectionSource<>(new ArrayList<>()), SizeTumblingWindow.of(2));
        WindowStreamSource ds2 = new WindowStreamSource<>(context,
            new CollectionSource<>(new ArrayList<>()), SizeTumblingWindow.of(2));

        PStreamSink sink1 = ds1.map(p -> p).withParallelism(2).keyBy(q -> q).reduce((oldValue, newValue) -> (oldValue))
            .withParallelism(3).sink(p -> {
            }).withParallelism(3);
        PStreamSink sink2 = ds2.sink(v -> {
        });

        when(context.getActions()).thenReturn(ImmutableList.of(sink1, sink2));

        PipelinePlanBuilder planBuilder = new PipelinePlanBuilder();
        PipelineGraph pipelineGraph = planBuilder.buildPlan(context);

        ChainCombiner combiner = new ChainCombiner();
        combiner.combineVertex(pipelineGraph);

        ExecutionGraphBuilder builder = new ExecutionGraphBuilder(pipelineGraph);
        ExecutionGraph graph = builder.buildExecutionGraph(new Configuration());

        Assert.assertEquals(1, graph.getVertexGroupMap().size());
        Assert.assertEquals(1, graph.getCycleGroupMeta().getFlyingCount());
        Assert.assertEquals(1, graph.getCycleGroupMeta().getIterationCount());
        ExecutionVertexGroup vertexGroup = graph.getVertexGroupMap().values().stream().findFirst().get();
        Assert.assertEquals(4, vertexGroup.getVertexMap().size());
        Assert.assertEquals(Long.MAX_VALUE, vertexGroup.getCycleGroupMeta().getIterationCount());
        Assert.assertEquals(5, vertexGroup.getCycleGroupMeta().getFlyingCount());
        Assert.assertFalse(vertexGroup.getCycleGroupMeta().isIterative());
        Assert.assertTrue(vertexGroup.getCycleGroupMeta().getAffinityLevel() == AffinityLevel.worker);
    }

    @Test
    public void testAllWindowWithReduceTwoAndSinkFourConcurrency() {
        AtomicInteger idGenerator = new AtomicInteger(0);
        AbstractPipelineContext context = mock(AbstractPipelineContext.class);
        when(context.generateId()).then(invocation -> idGenerator.incrementAndGet());
        Configuration configuration = new Configuration();
        configuration.put(FrameworkConfigKeys.INC_STREAM_MATERIALIZE_DISABLE, Boolean.TRUE.toString());
        when(context.getConfig()).thenReturn(configuration);
        WindowStreamSource source = new WindowStreamSource(context,
            new CollectionSource<>(ImmutableList.of(1, 2, 3)), AllWindow.getInstance());
        PStreamSink sink = source
            .map(e -> Tuple.of(e, 1))
            .keyBy(v -> ((Tuple) v).f0)
            .reduce(new CountFunc())
            .withParallelism(2)
            .sink(v -> {
            })
            .withParallelism(4);
        when(context.getActions()).thenReturn(ImmutableList.of(sink));

        PipelinePlanBuilder planBuilder = new PipelinePlanBuilder();
        PipelineGraph pipelineGraph = planBuilder.buildPlan(context);

        PipelineGraphOptimizer optimizer = new PipelineGraphOptimizer();
        optimizer.optimizePipelineGraph(pipelineGraph);

        ExecutionGraphBuilder builder = new ExecutionGraphBuilder(pipelineGraph);
        ExecutionGraph graph = builder.buildExecutionGraph(new Configuration());

        Assert.assertEquals(3, graph.getVertexGroupMap().size());
        Assert.assertEquals(1, graph.getCycleGroupMeta().getFlyingCount());
        Assert.assertEquals(1, graph.getCycleGroupMeta().getIterationCount());
        ExecutionVertexGroup vertexGroup = graph.getVertexGroupMap().values().stream().findFirst().get();
        Assert.assertEquals(1, vertexGroup.getVertexMap().size());
        Assert.assertEquals(1, vertexGroup.getCycleGroupMeta().getIterationCount());
        Assert.assertEquals(1, vertexGroup.getCycleGroupMeta().getFlyingCount());
        Assert.assertFalse(vertexGroup.getCycleGroupMeta().isIterative());
        Assert.assertTrue(vertexGroup.getCycleGroupMeta().getGroupType() == CycleGroupType.windowed);
        Assert.assertTrue(graph.getCycleGroupMeta().getGroupType() == CycleGroupType.windowed);
    }

    @Test
    public void testAllWindowWithSingleConcurrency() {
        AtomicInteger idGenerator = new AtomicInteger(0);
        AbstractPipelineContext context = mock(AbstractPipelineContext.class);
        when(context.generateId()).then(invocation -> idGenerator.incrementAndGet());
        Configuration configuration = new Configuration();
        configuration.put(FrameworkConfigKeys.INC_STREAM_MATERIALIZE_DISABLE, Boolean.TRUE.toString());
        when(context.getConfig()).thenReturn(configuration);
        WindowStreamSource source = new WindowStreamSource(context,
            new CollectionSource<>(ImmutableList.of(1, 2, 3)), AllWindow.getInstance());
        PStreamSink sink = source
            .map(e -> Tuple.of(e, 1))
            .keyBy(v -> ((Tuple) v).f0)
            .reduce(new CountFunc())
            .withParallelism(1)
            .sink(v -> {
            })
            .withParallelism(1);
        when(context.getActions()).thenReturn(ImmutableList.of(sink));

        PipelinePlanBuilder planBuilder = new PipelinePlanBuilder();
        PipelineGraph pipelineGraph = planBuilder.buildPlan(context);

        PipelineGraphOptimizer optimizer = new PipelineGraphOptimizer();
        optimizer.optimizePipelineGraph(pipelineGraph);

        ExecutionGraphBuilder builder = new ExecutionGraphBuilder(pipelineGraph);
        ExecutionGraph graph = builder.buildExecutionGraph(new Configuration());

        Assert.assertEquals(2, graph.getVertexGroupMap().size());
        Assert.assertEquals(1, graph.getCycleGroupMeta().getFlyingCount());
        Assert.assertEquals(1, graph.getCycleGroupMeta().getIterationCount());
        ExecutionVertexGroup vertexGroup = graph.getVertexGroupMap().values().stream().findFirst().get();
        Assert.assertEquals(1, vertexGroup.getVertexMap().size());
        Assert.assertEquals(1, vertexGroup.getCycleGroupMeta().getIterationCount());
        Assert.assertEquals(1, vertexGroup.getCycleGroupMeta().getFlyingCount());
        Assert.assertFalse(vertexGroup.getCycleGroupMeta().isIterative());
        Assert.assertTrue(vertexGroup.getCycleGroupMeta().getGroupType() == CycleGroupType.windowed);
        Assert.assertTrue(graph.getCycleGroupMeta().getGroupType() == CycleGroupType.windowed);
    }

    @Test
    public void testOperatorChain() {
        AtomicInteger idGenerator = new AtomicInteger(0);
        AbstractPipelineContext context = mock(AbstractPipelineContext.class);
        when(context.generateId()).then(invocation -> idGenerator.incrementAndGet());

        WindowStreamSource source = new WindowStreamSource(context,
            new CollectionSource<>(ImmutableList.of(1, 2, 3)), SizeTumblingWindow.of(2));
        WindowDataStream filter1 = new WindowDataStream(source, new MapOperator(x -> x));
        WindowDataStream filter2 = new WindowDataStream(filter1, new MapOperator(x -> x));
        WindowDataStream mapper1 = new WindowDataStream(filter2, new MapOperator(x -> x));
        WindowDataStream mapper2 = new WindowDataStream(mapper1, new MapOperator(x -> x));
        PStreamSink sink1 = new WindowStreamSink(mapper2, new SinkOperator(x -> {
        }));

        when(context.getActions()).thenReturn(ImmutableList.of(sink1));

        PipelinePlanBuilder planBuilder = new PipelinePlanBuilder();
        PipelineGraph pipelineGraph = planBuilder.buildPlan(context);
        PipelineGraphOptimizer optimizer = new PipelineGraphOptimizer();
        optimizer.optimizePipelineGraph(pipelineGraph);

        ExecutionGraphBuilder builder = new ExecutionGraphBuilder(pipelineGraph);
        ExecutionGraph graph = builder.buildExecutionGraph(new Configuration());

        Assert.assertEquals(1, graph.getVertexGroupMap().size());
        Assert.assertEquals(1, graph.getCycleGroupMeta().getFlyingCount());
        Assert.assertEquals(1, graph.getCycleGroupMeta().getIterationCount());
        ExecutionVertexGroup vertexGroup = graph.getVertexGroupMap().values().stream().findFirst().get();
        Assert.assertEquals(1, vertexGroup.getVertexMap().size());
        Assert.assertEquals(Long.MAX_VALUE, vertexGroup.getCycleGroupMeta().getIterationCount());
        Assert.assertEquals(5, vertexGroup.getCycleGroupMeta().getFlyingCount());
        Assert.assertFalse(vertexGroup.getCycleGroupMeta().isIterative());
        Assert.assertTrue(vertexGroup.getCycleGroupMeta().getGroupType() == CycleGroupType.pipelined);
    }

    @Test
    public void testKeyAgg() {
        AtomicInteger idGenerator = new AtomicInteger(0);
        AbstractPipelineContext context = mock(AbstractPipelineContext.class);
        when(context.generateId()).then(invocation -> idGenerator.incrementAndGet());
        Configuration configuration = new Configuration();
        when(context.getConfig()).thenReturn(configuration);
        PWindowSource source = new WindowStreamSource(context,
            new CollectionSource<>(ImmutableList.of(1, 2, 3)), SizeTumblingWindow.of(2));
        PStreamSink sink = source.flatMap(new FlatMapFunction<String, Long>() {
                @Override
                public void flatMap(String value, Collector collector) {
                    String[] records = value.split(",");
                    for (String record : records) {
                        collector.partition(Long.valueOf(record));
                    }
                }
            })
            .map(p -> Tuple.of(p, p))
            .keyBy(p -> ((long) ((Tuple) p).f0) % 7)
            .aggregate(new AggFunc())
            .withParallelism(3)
            .map(v -> String.format("%s,%s", ((Tuple) v).f0, ((Tuple) v).f1))
            .sink(v -> {
            }).withParallelism(2);

        when(context.getActions()).thenReturn(ImmutableList.of(sink));

        PipelinePlanBuilder planBuilder = new PipelinePlanBuilder();
        PipelineGraph pipelineGraph = planBuilder.buildPlan(context);
        PipelineGraphOptimizer optimizer = new PipelineGraphOptimizer();
        optimizer.optimizePipelineGraph(pipelineGraph);

        ExecutionGraphBuilder builder = new ExecutionGraphBuilder(pipelineGraph);
        ExecutionGraph graph = builder.buildExecutionGraph(new Configuration());

        Assert.assertEquals(1, graph.getVertexGroupMap().size());
        Assert.assertEquals(1, graph.getCycleGroupMeta().getFlyingCount());
        Assert.assertEquals(1, graph.getCycleGroupMeta().getIterationCount());
        ExecutionVertexGroup vertexGroup = graph.getVertexGroupMap().values().stream().findFirst().get();
        Assert.assertEquals(3, vertexGroup.getVertexMap().size());
        Assert.assertEquals(Long.MAX_VALUE, vertexGroup.getCycleGroupMeta().getIterationCount());
        Assert.assertEquals(5, vertexGroup.getCycleGroupMeta().getFlyingCount());
        Assert.assertFalse(vertexGroup.getCycleGroupMeta().isIterative());
        Assert.assertTrue(vertexGroup.getVertexMap().get(1).getNumPartitions() == 4);
        Assert.assertTrue(vertexGroup.getVertexMap().get(5).getNumPartitions() == 2);
        Assert.assertTrue(vertexGroup.getVertexMap().get(7).getNumPartitions() == 2);
        Assert.assertTrue(vertexGroup.getCycleGroupMeta().getGroupType() == CycleGroupType.pipelined);
        Assert.assertTrue(graph.getCycleGroupMeta().getGroupType() == CycleGroupType.pipelined);
    }

    @Test
    public void testIncGraphCompute() {
        AtomicInteger idGenerator = new AtomicInteger(0);
        AbstractPipelineContext context = mock(AbstractPipelineContext.class);
        when(context.generateId()).then(invocation -> idGenerator.incrementAndGet());
        Configuration configuration = new Configuration();
        when(context.getConfig()).thenReturn(configuration);

        PStreamSource<IVertex<Integer, Integer>> vertices =
            new WindowStreamSource<>(context, new CollectionSource<>(new ArrayList<>()), SizeTumblingWindow.of(2));
        PStreamSource<IEdge<Integer, Integer>> edges =
            new WindowStreamSource(context, new CollectionSource<>(new ArrayList<>()), SizeTumblingWindow.of(2));

        final String graphName = "graph_view_name";
        GraphViewDesc graphViewDesc = GraphViewBuilder.createGraphView(graphName)
            .withShardNum(4)
            .withBackend(IViewDesc.BackendType.RocksDB)
            .withSchema(new GraphMetaType(IntegerType.INSTANCE, ValueVertex.class,
                Integer.class, ValueEdge.class, IntegerType.class))
            .build();
        PGraphView<Integer, Integer, Integer> fundGraphView =
            new IncGraphView<>(context, graphViewDesc);

        PIncGraphView<Integer, Integer, Integer> incGraphView =
            fundGraphView.appendGraph(
                vertices.window(WindowFactory.createSizeTumblingWindow(1)),
                edges.window(WindowFactory.createSizeTumblingWindow(1)));

        PStreamSink sink = incGraphView.incrementalCompute(new IncGraphAlgorithms(3))
            .getVertices()
            .map(v -> String.format("%s,%s", v.getId(), v.getValue()))
            .sink(v -> {
            });

        when(context.getActions()).thenReturn(ImmutableList.of(sink));

        PipelinePlanBuilder planBuilder = new PipelinePlanBuilder();
        PipelineGraph pipelineGraph = planBuilder.buildPlan(context);
        PipelineGraphOptimizer optimizer = new PipelineGraphOptimizer();
        optimizer.optimizePipelineGraph(pipelineGraph);

        ExecutionGraphBuilder builder = new ExecutionGraphBuilder(pipelineGraph);
        ExecutionGraph graph = builder.buildExecutionGraph(new Configuration());

        Assert.assertEquals(4, graph.getVertexGroupMap().size());
        Assert.assertEquals(1, graph.getCycleGroupMeta().getFlyingCount());
        Assert.assertEquals(Long.MAX_VALUE, graph.getCycleGroupMeta().getIterationCount());

        ExecutionVertexGroup vertexGroup = graph.getVertexGroupMap().get(3);
        Assert.assertEquals(3, vertexGroup.getCycleGroupMeta().getIterationCount());
        Assert.assertEquals(1, vertexGroup.getCycleGroupMeta().getFlyingCount());
        Assert.assertTrue(vertexGroup.getCycleGroupMeta().isIterative());
        Assert.assertTrue(vertexGroup.getCycleGroupMeta().getAffinityLevel() == AffinityLevel.worker);
        Assert.assertTrue(vertexGroup.getCycleGroupMeta().getGroupType() == CycleGroupType.incremental);
        Assert.assertTrue(graph.getCycleGroupMeta().getGroupType() == CycleGroupType.incremental);
    }

    @Test
    public void testStaticGraphCompute() {
        AtomicInteger idGenerator = new AtomicInteger(0);
        AbstractPipelineContext context = mock(AbstractPipelineContext.class);
        when(context.generateId()).then(invocation -> idGenerator.incrementAndGet());
        Configuration configuration = new Configuration();
        when(context.getConfig()).thenReturn(configuration);

        PWindowSource<IVertex<Integer, Integer>> vertices =
            new WindowStreamSource<>(context, new CollectionSource<>(new ArrayList<>()), SizeTumblingWindow.of(2));
        PWindowSource<IEdge<Integer, Integer>> edges =
            new WindowStreamSource<>(context, new CollectionSource<>(new ArrayList<>()), SizeTumblingWindow.of(2));


        PGraphWindow graphWindow = new WindowStreamGraph(createGraphViewDesc(4), context,
            vertices, edges);
        PStreamSink sink = graphWindow.compute(new PRAlgorithms(3))
            .compute(3)
            .getVertices()
            .sink(v -> {
            })
            .withParallelism(2);

        when(context.getActions()).thenReturn(ImmutableList.of(sink));

        PipelinePlanBuilder planBuilder = new PipelinePlanBuilder();
        PipelineGraph pipelineGraph = planBuilder.buildPlan(context);
        PipelineGraphOptimizer optimizer = new PipelineGraphOptimizer();
        optimizer.optimizePipelineGraph(pipelineGraph);

        ExecutionGraphBuilder builder = new ExecutionGraphBuilder(pipelineGraph);
        ExecutionGraph graph = builder.buildExecutionGraph(new Configuration());

        Assert.assertEquals(4, graph.getVertexGroupMap().size());
        Assert.assertEquals(4, graph.getGroupEdgeMap().size());
        Assert.assertEquals(1, graph.getCycleGroupMeta().getFlyingCount());
        Assert.assertEquals(Long.MAX_VALUE, graph.getCycleGroupMeta().getIterationCount());

        ExecutionVertexGroup vertexGroup = graph.getVertexGroupMap().get(3);
        Assert.assertEquals(3, vertexGroup.getCycleGroupMeta().getIterationCount());
        Assert.assertEquals(1, vertexGroup.getCycleGroupMeta().getFlyingCount());
        Assert.assertTrue(vertexGroup.getCycleGroupMeta().isIterative());
        Assert.assertTrue(vertexGroup.getCycleGroupMeta().getAffinityLevel() == AffinityLevel.worker);
        Assert.assertTrue(vertexGroup.getCycleGroupMeta().getGroupType() == CycleGroupType.statical);
        Assert.assertTrue(graph.getCycleGroupMeta().getGroupType() == CycleGroupType.statical);
    }

    @Test
    public void testAllWindowStaticGraphCompute() {
        AtomicInteger idGenerator = new AtomicInteger(0);
        AbstractPipelineContext context = mock(AbstractPipelineContext.class);
        when(context.generateId()).then(invocation -> idGenerator.incrementAndGet());
        Configuration configuration = new Configuration();
        when(context.getConfig()).thenReturn(configuration);

        PWindowSource<IVertex<Integer, Integer>> vertices =
            new WindowStreamSource<>(context, new CollectionSource<>(new ArrayList<>()), AllWindow.getInstance());
        PWindowSource<IEdge<Integer, Integer>> edges =
            new WindowStreamSource<>(context, new CollectionSource<>(new ArrayList<>()), AllWindow.getInstance());

        PGraphWindow graphWindow = new WindowStreamGraph(createGraphViewDesc(4), context,
            vertices, edges);
        PStreamSink sink = graphWindow.compute(new PRAlgorithms(3))
            .compute(3)
            .getVertices()
            .sink(v -> {
            })
            .withParallelism(2);

        when(context.getActions()).thenReturn(ImmutableList.of(sink));

        PipelinePlanBuilder planBuilder = new PipelinePlanBuilder();
        PipelineGraph pipelineGraph = planBuilder.buildPlan(context);
        PipelineGraphOptimizer optimizer = new PipelineGraphOptimizer();
        optimizer.optimizePipelineGraph(pipelineGraph);

        ExecutionGraphBuilder builder = new ExecutionGraphBuilder(pipelineGraph);
        ExecutionGraph graph = builder.buildExecutionGraph(new Configuration());

        Assert.assertEquals(4, graph.getVertexGroupMap().size());
        Assert.assertEquals(4, graph.getGroupEdgeMap().size());
        Assert.assertEquals(1, graph.getCycleGroupMeta().getFlyingCount());
        Assert.assertEquals(1, graph.getCycleGroupMeta().getIterationCount());

        ExecutionVertexGroup vertexGroup = graph.getVertexGroupMap().get(3);
        Assert.assertEquals(3, vertexGroup.getCycleGroupMeta().getIterationCount());
        Assert.assertEquals(1, vertexGroup.getCycleGroupMeta().getFlyingCount());
        Assert.assertTrue(vertexGroup.getCycleGroupMeta().isIterative());
        Assert.assertTrue(vertexGroup.getCycleGroupMeta().getAffinityLevel() == AffinityLevel.worker);
        Assert.assertTrue(vertexGroup.getCycleGroupMeta().getGroupType() == CycleGroupType.statical);
        Assert.assertTrue(graph.getCycleGroupMeta().getGroupType() == CycleGroupType.statical);
    }

    @Test
    public void testWindowGraphTraversal() {
        AtomicInteger idGenerator = new AtomicInteger(0);
        AbstractPipelineContext context = mock(AbstractPipelineContext.class);
        when(context.generateId()).then(invocation -> idGenerator.incrementAndGet());
        Configuration configuration = new Configuration();
        when(context.getConfig()).thenReturn(configuration);

        PWindowSource<IVertex<Integer, Integer>> vertices =
            new WindowStreamSource<>(context, new CollectionSource<>(new ArrayList<>()), SizeTumblingWindow.of(2));
        PWindowSource<IEdge<Integer, Integer>> edges =
            new WindowStreamSource<>(context, new CollectionSource<>(new ArrayList<>()), SizeTumblingWindow.of(2));

        PStreamSource<ITraversalRequest<Integer>> triggerSource =
            new WindowStreamSource<>(context,
                new CollectionSource<>(Lists.newArrayList(new VertexBeginTraversalRequest(3))),
                AllWindow.getInstance());
        PWindowStream<ITraversalRequest<Integer>> windowTrigger =
            triggerSource.window(WindowFactory.createSizeTumblingWindow(3));

        PGraphWindow graphWindow = new WindowStreamGraph(createGraphViewDesc(4), context, vertices, edges);
        PStreamSink sink = graphWindow.traversal(new GraphTraversalAlgorithms(3)).start(windowTrigger).sink(v -> {
        });
        when(context.getActions()).thenReturn(ImmutableList.of(sink));

        PipelinePlanBuilder planBuilder = new PipelinePlanBuilder();
        PipelineGraph pipelineGraph = planBuilder.buildPlan(context);
        PipelineGraphOptimizer optimizer = new PipelineGraphOptimizer();
        optimizer.optimizePipelineGraph(pipelineGraph);

        ExecutionGraphBuilder builder = new ExecutionGraphBuilder(pipelineGraph);
        ExecutionGraph graph = builder.buildExecutionGraph(new Configuration());
        Assert.assertEquals(5, graph.getVertexGroupMap().size());
        Assert.assertEquals(5, graph.getGroupEdgeMap().size());
        Assert.assertEquals(1, graph.getCycleGroupMeta().getFlyingCount());
        Assert.assertEquals(Long.MAX_VALUE, graph.getCycleGroupMeta().getIterationCount());

        ExecutionVertexGroup vertexGroup = graph.getVertexGroupMap().get(4);
        Assert.assertEquals(3, vertexGroup.getCycleGroupMeta().getIterationCount());
        Assert.assertEquals(1, vertexGroup.getCycleGroupMeta().getFlyingCount());
        Assert.assertTrue(vertexGroup.getCycleGroupMeta().isIterative());
        Assert.assertTrue(vertexGroup.getCycleGroupMeta().getAffinityLevel() == AffinityLevel.worker);
        Assert.assertTrue(vertexGroup.getCycleGroupMeta().getGroupType() == CycleGroupType.statical);
        Assert.assertTrue(graph.getCycleGroupMeta().getGroupType() == CycleGroupType.statical);
    }

    @Test
    public void testMultiSourceWindowGraphTraversal() {
        AtomicInteger idGenerator = new AtomicInteger(0);
        AbstractPipelineContext context = mock(AbstractPipelineContext.class);
        when(context.generateId()).then(invocation -> idGenerator.incrementAndGet());
        Configuration configuration = new Configuration();
        when(context.getConfig()).thenReturn(configuration);

        PWindowSource<IVertex<Integer, Integer>> vertices1 =
            new WindowStreamSource<>(context, new CollectionSource<>(new ArrayList<>()), SizeTumblingWindow.of(2));
        PWindowSource<IVertex<Integer, Integer>> vertices2 =
            new WindowStreamSource<>(context, new CollectionSource<>(new ArrayList<>()), SizeTumblingWindow.of(2));


        PWindowSource<IEdge<Integer, Integer>> edges =
            new WindowStreamSource<>(context, new CollectionSource<>(new ArrayList<>()), SizeTumblingWindow.of(2));

        PStreamSource<ITraversalRequest<Integer>> triggerSource =
            new WindowStreamSource<>(context,
                new CollectionSource<>(Lists.newArrayList(new VertexBeginTraversalRequest(3))),
                AllWindow.getInstance());

        PGraphWindow graphWindow = new WindowStreamGraph(createGraphViewDesc(4), context,
            vertices1.union(vertices2), edges);
        PStreamSink sink = graphWindow.traversal(new GraphTraversalAlgorithms(3)).start(triggerSource).sink(v -> {
        });
        when(context.getActions()).thenReturn(ImmutableList.of(sink));

        PipelinePlanBuilder planBuilder = new PipelinePlanBuilder();
        PipelineGraph pipelineGraph = planBuilder.buildPlan(context);
        PipelineGraphOptimizer optimizer = new PipelineGraphOptimizer();
        optimizer.optimizePipelineGraph(pipelineGraph);

        ExecutionGraphBuilder builder = new ExecutionGraphBuilder(pipelineGraph);
        ExecutionGraph graph = builder.buildExecutionGraph(new Configuration());
        Assert.assertEquals(4, graph.getVertexGroupMap().size());
        Assert.assertEquals(4, graph.getGroupEdgeMap().size());
        Assert.assertEquals(1, graph.getCycleGroupMeta().getFlyingCount());
        Assert.assertEquals(Long.MAX_VALUE, graph.getCycleGroupMeta().getIterationCount());

        ExecutionVertexGroup vertexGroup = graph.getVertexGroupMap().get(3);
        Assert.assertEquals(3, vertexGroup.getCycleGroupMeta().getIterationCount());
        Assert.assertEquals(1, vertexGroup.getCycleGroupMeta().getFlyingCount());
        Assert.assertTrue(vertexGroup.getCycleGroupMeta().isIterative());
        Assert.assertTrue(vertexGroup.getCycleGroupMeta().getGroupType() == CycleGroupType.statical);
        Assert.assertTrue(graph.getCycleGroupMeta().getGroupType() == CycleGroupType.statical);
    }

    @Test
    public void testAllWindowGraphTraversal() {
        AtomicInteger idGenerator = new AtomicInteger(0);
        AbstractPipelineContext context = mock(AbstractPipelineContext.class);
        when(context.generateId()).then(invocation -> idGenerator.incrementAndGet());
        Configuration configuration = new Configuration();
        when(context.getConfig()).thenReturn(configuration);

        PWindowSource<IVertex<Integer, Integer>> vertices =
            new WindowStreamSource<>(context, new CollectionSource<>(new ArrayList<>()), AllWindow.getInstance());
        PWindowSource<IEdge<Integer, Integer>> edges =
            new WindowStreamSource<>(context, new CollectionSource<>(new ArrayList<>()), AllWindow.getInstance());

        PStreamSource<ITraversalRequest<Integer>> triggerSource =
            new WindowStreamSource<>(context,
                new CollectionSource<>(Lists.newArrayList(new VertexBeginTraversalRequest(3))),
                AllWindow.getInstance());

        PGraphWindow graphWindow = new WindowStreamGraph(createGraphViewDesc(4), context, vertices, edges);
        PStreamSink sink = graphWindow.traversal(new GraphTraversalAlgorithms(3)).start(triggerSource).sink(v -> {
        });
        when(context.getActions()).thenReturn(ImmutableList.of(sink));

        PipelinePlanBuilder planBuilder = new PipelinePlanBuilder();
        PipelineGraph pipelineGraph = planBuilder.buildPlan(context);
        PipelineGraphOptimizer optimizer = new PipelineGraphOptimizer();
        optimizer.optimizePipelineGraph(pipelineGraph);

        ExecutionGraphBuilder builder = new ExecutionGraphBuilder(pipelineGraph);
        ExecutionGraph graph = builder.buildExecutionGraph(new Configuration());
        Assert.assertEquals(4, graph.getVertexGroupMap().size());
        Assert.assertEquals(4, graph.getGroupEdgeMap().size());
        Assert.assertEquals(1, graph.getCycleGroupMeta().getFlyingCount());
        Assert.assertEquals(1, graph.getCycleGroupMeta().getIterationCount());

        ExecutionVertexGroup vertexGroup = graph.getVertexGroupMap().get(3);
        Assert.assertEquals(3, vertexGroup.getCycleGroupMeta().getIterationCount());
        Assert.assertEquals(1, vertexGroup.getCycleGroupMeta().getFlyingCount());
        Assert.assertTrue(vertexGroup.getCycleGroupMeta().isIterative());
        Assert.assertTrue(vertexGroup.getCycleGroupMeta().getAffinityLevel() == AffinityLevel.worker);
        Assert.assertTrue(vertexGroup.getCycleGroupMeta().getGroupType() == CycleGroupType.statical);
        Assert.assertTrue(graph.getCycleGroupMeta().getGroupType() == CycleGroupType.statical);
    }

    @Test
    public void testTwoSourceWithGraphUnion() {
        AtomicInteger idGenerator = new AtomicInteger(0);
        AbstractPipelineContext context = mock(AbstractPipelineContext.class);
        when(context.generateId()).then(invocation -> idGenerator.incrementAndGet());
        Configuration configuration = new Configuration();
        when(context.getConfig()).thenReturn(configuration);

        PWindowSource<Integer> source1 =
            new WindowStreamSource<>(context, new CollectionSource<>(new ArrayList<>()), SizeTumblingWindow.of(10));
        PWindowSource<Integer> source2 =
            new WindowStreamSource<>(context, new CollectionSource<>(new ArrayList<>()), SizeTumblingWindow.of(10));

        PWindowStream<IVertex<Integer, Double>> v1 = source1.map(i -> new ValueVertex<>(i, (double) i));
        PWindowStream<IEdge<Integer, Integer>> e1 = source1.map(i -> new ValueEdge<>(i, i, i));

        PWindowStream<IVertex<Integer, Double>> v2 = source2.map(i -> new ValueVertex<>(i, (double) i));
        PWindowStream<IEdge<Integer, Integer>> e2 = source2.map(i -> new ValueEdge<>(i, i, i));

        PWindowStream<IVertex<Integer, Double>> v = v1.union(v2);
        PWindowStream<IEdge<Integer, Integer>> e = e1.union(e2);

        PGraphWindow<Integer, Double, Integer> graphWindow = new WindowStreamGraph(createGraphViewDesc(1), context, v, e);
        PStreamSink sink = graphWindow.compute(new PRAlgorithms(3))
            .getVertices()
            .sink(r -> {
            });
        when(context.getActions()).thenReturn(ImmutableList.of(sink));

        PipelinePlanBuilder planBuilder = new PipelinePlanBuilder();
        PipelineGraph pipelineGraph = planBuilder.buildPlan(context);
        PipelineGraphOptimizer optimizer = new PipelineGraphOptimizer();
        optimizer.optimizePipelineGraph(pipelineGraph);

        ExecutionGraphBuilder builder = new ExecutionGraphBuilder(pipelineGraph);
        ExecutionGraph graph = builder.buildExecutionGraph(new Configuration());
        Assert.assertEquals(3, graph.getVertexGroupMap().size());
        Assert.assertEquals(4, graph.getGroupEdgeMap().size());
        Assert.assertEquals(1, graph.getCycleGroupMeta().getFlyingCount());
        Assert.assertEquals(Long.MAX_VALUE, graph.getCycleGroupMeta().getIterationCount());

        ExecutionVertexGroup vertexGroup = graph.getVertexGroupMap().get(2);
        Assert.assertEquals(3, vertexGroup.getCycleGroupMeta().getIterationCount());
        Assert.assertEquals(1, vertexGroup.getCycleGroupMeta().getFlyingCount());
        Assert.assertTrue(vertexGroup.getCycleGroupMeta().isIterative());
        Assert.assertTrue(vertexGroup.getCycleGroupMeta().getAffinityLevel() == AffinityLevel.worker);
    }

    @Test
    public void testThreeSourceWithGraphUnion() {
        AtomicInteger idGenerator = new AtomicInteger(0);
        AbstractPipelineContext context = mock(AbstractPipelineContext.class);
        when(context.generateId()).then(invocation -> idGenerator.incrementAndGet());
        Configuration configuration = new Configuration();
        when(context.getConfig()).thenReturn(configuration);

        PWindowSource<Integer> source1 =
            new WindowStreamSource<>(context, new CollectionSource<>(new ArrayList<>()), SizeTumblingWindow.of(10));
        PWindowSource<Integer> source2 =
            new WindowStreamSource<>(context, new CollectionSource<>(new ArrayList<>()), SizeTumblingWindow.of(10));
        PWindowSource<Integer> source3 =
            new WindowStreamSource<>(context, new CollectionSource<>(new ArrayList<>()), SizeTumblingWindow.of(10));

        PWindowStream<IVertex<Integer, Double>> v1 = source1.map(i -> new ValueVertex<>(i, (double) i));
        PWindowStream<IEdge<Integer, Integer>> e1 = source1.map(i -> new ValueEdge<>(i, i, i));

        PWindowStream<IVertex<Integer, Double>> v2 = source2.map(i -> new ValueVertex<>(i, (double) i));
        PWindowStream<IEdge<Integer, Integer>> e2 = source2.map(i -> new ValueEdge<>(i, i, i));

        PWindowStream<IVertex<Integer, Double>> v3 = source3.map(i -> new ValueVertex<>(i, (double) i));
        PWindowStream<IEdge<Integer, Integer>> e3 = source3.map(i -> new ValueEdge<>(i, i, i));

        PWindowStream<IVertex<Integer, Double>> v = v1.union(v2).union(v3);
        PWindowStream<IEdge<Integer, Integer>> e = e1.union(e2).union(e3);

        PGraphWindow<Integer, Double, Integer> graphWindow = new WindowStreamGraph(createGraphViewDesc(1),
            context, v, e);
        PStreamSink sink = graphWindow.compute(new PRAlgorithms(3))
            .getVertices()
            .sink(r -> {
            });
        when(context.getActions()).thenReturn(ImmutableList.of(sink));

        PipelinePlanBuilder planBuilder = new PipelinePlanBuilder();
        PipelineGraph pipelineGraph = planBuilder.buildPlan(context);
        PipelineGraphOptimizer optimizer = new PipelineGraphOptimizer();
        optimizer.optimizePipelineGraph(pipelineGraph);

        ExecutionGraphBuilder builder = new ExecutionGraphBuilder(pipelineGraph);
        ExecutionGraph graph = builder.buildExecutionGraph(new Configuration());
        Assert.assertEquals(3, graph.getVertexGroupMap().size());
        Assert.assertEquals(4, graph.getGroupEdgeMap().size());
        Assert.assertEquals(1, graph.getCycleGroupMeta().getFlyingCount());
        Assert.assertEquals(Long.MAX_VALUE, graph.getCycleGroupMeta().getIterationCount());

        ExecutionVertexGroup vertexGroup = graph.getVertexGroupMap().get(2);
        Assert.assertEquals(3, vertexGroup.getCycleGroupMeta().getIterationCount());
        Assert.assertEquals(1, vertexGroup.getCycleGroupMeta().getFlyingCount());
        Assert.assertTrue(vertexGroup.getCycleGroupMeta().isIterative());
        Assert.assertTrue(vertexGroup.getCycleGroupMeta().getAffinityLevel() == AffinityLevel.worker);
    }

    @Test
    public void testTenSourceWithGraphUnion() {
        AtomicInteger idGenerator = new AtomicInteger(0);
        AbstractPipelineContext context = mock(AbstractPipelineContext.class);
        when(context.generateId()).then(invocation -> idGenerator.incrementAndGet());
        Configuration configuration = new Configuration();
        when(context.getConfig()).thenReturn(configuration);

        PWindowSource<Integer> source1 =
            new WindowStreamSource<>(context, new CollectionSource<>(new ArrayList<>()), SizeTumblingWindow.of(10));
        PWindowSource<Integer> source2 =
            new WindowStreamSource<>(context, new CollectionSource<>(new ArrayList<>()), SizeTumblingWindow.of(10));
        PWindowSource<Integer> source3 =
            new WindowStreamSource<>(context, new CollectionSource<>(new ArrayList<>()), SizeTumblingWindow.of(10));
        PWindowSource<Integer> source4 =
            new WindowStreamSource<>(context, new CollectionSource<>(new ArrayList<>()), SizeTumblingWindow.of(10));
        PWindowSource<Integer> source5 =
            new WindowStreamSource<>(context, new CollectionSource<>(new ArrayList<>()), SizeTumblingWindow.of(10));
        PWindowSource<Integer> source6 =
            new WindowStreamSource<>(context, new CollectionSource<>(new ArrayList<>()), SizeTumblingWindow.of(10));
        PWindowSource<Integer> source7 =
            new WindowStreamSource<>(context, new CollectionSource<>(new ArrayList<>()), SizeTumblingWindow.of(10));
        PWindowSource<Integer> source8 =
            new WindowStreamSource<>(context, new CollectionSource<>(new ArrayList<>()), SizeTumblingWindow.of(10));
        PWindowSource<Integer> source9 =
            new WindowStreamSource<>(context, new CollectionSource<>(new ArrayList<>()), SizeTumblingWindow.of(10));

        PWindowStream<IVertex<Integer, Double>> v1 = source1.map(i -> new ValueVertex<>(i, (double) i));
        PWindowStream<IEdge<Integer, Integer>> e1 = source1.map(i -> new ValueEdge<>(i, i, i));

        PWindowStream<IVertex<Integer, Double>> v2 = source2.map(i -> new ValueVertex<>(i, (double) i));
        PWindowStream<IEdge<Integer, Integer>> e2 = source2.map(i -> new ValueEdge<>(i, i, i));

        PWindowStream<IVertex<Integer, Double>> v3 = source3.map(i -> new ValueVertex<>(i, (double) i));
        PWindowStream<IEdge<Integer, Integer>> e3 = source3.map(i -> new ValueEdge<>(i, i, i));

        PWindowStream<IVertex<Integer, Double>> v4 = source4.map(i -> new ValueVertex<>(i, (double) i));
        PWindowStream<IEdge<Integer, Integer>> e4 = source4.map(i -> new ValueEdge<>(i, i, i));

        PWindowStream<IVertex<Integer, Double>> v5 = source5.map(i -> new ValueVertex<>(i, (double) i));
        PWindowStream<IEdge<Integer, Integer>> e5 = source5.map(i -> new ValueEdge<>(i, i, i));

        PWindowStream<IVertex<Integer, Double>> v6 = source6.map(i -> new ValueVertex<>(i, (double) i));
        PWindowStream<IEdge<Integer, Integer>> e6 = source6.map(i -> new ValueEdge<>(i, i, i));

        PWindowStream<IVertex<Integer, Double>> v7 = source7.map(i -> new ValueVertex<>(i, (double) i));
        PWindowStream<IEdge<Integer, Integer>> e7 = source7.map(i -> new ValueEdge<>(i, i, i));

        PWindowStream<IVertex<Integer, Double>> v8 = source8.map(i -> new ValueVertex<>(i, (double) i));
        PWindowStream<IEdge<Integer, Integer>> e8 = source8.map(i -> new ValueEdge<>(i, i, i));

        PWindowStream<IVertex<Integer, Double>> v9 = source9.map(i -> new ValueVertex<>(i, (double) i));
        PWindowStream<IEdge<Integer, Integer>> e9 = source9.map(i -> new ValueEdge<>(i, i, i));

        PWindowStream<IVertex<Integer, Double>> v =
            v1.union(v2).union(v3).union(v4).union(v5).union(v6).union(v7).union(v8).union(v9);
        PWindowStream<IEdge<Integer, Integer>> e =
            e1.union(e2).union(e3).union(e4).union(e5).union(e6).union(e7).union(e8).union(e9);

        PGraphWindow<Integer, Double, Integer> graphWindow = new WindowStreamGraph(createGraphViewDesc(1),
            context, v, e);
        PStreamSink sink = graphWindow.compute(new PRAlgorithms(3))
            .getVertices()
            .sink(r -> {
            });
        when(context.getActions()).thenReturn(ImmutableList.of(sink));

        PipelinePlanBuilder planBuilder = new PipelinePlanBuilder();
        PipelineGraph pipelineGraph = planBuilder.buildPlan(context);
        PipelineGraphOptimizer optimizer = new PipelineGraphOptimizer();
        optimizer.optimizePipelineGraph(pipelineGraph);

        ExecutionGraphBuilder builder = new ExecutionGraphBuilder(pipelineGraph);
        ExecutionGraph graph = builder.buildExecutionGraph(new Configuration());
        Assert.assertEquals(3, graph.getVertexGroupMap().size());
        Assert.assertEquals(4, graph.getGroupEdgeMap().size());
        Assert.assertEquals(1, graph.getCycleGroupMeta().getFlyingCount());
        Assert.assertEquals(Long.MAX_VALUE, graph.getCycleGroupMeta().getIterationCount());

        ExecutionVertexGroup vertexGroup = graph.getVertexGroupMap().get(2);
        Assert.assertEquals(3, vertexGroup.getCycleGroupMeta().getIterationCount());
        Assert.assertEquals(1, vertexGroup.getCycleGroupMeta().getFlyingCount());
        Assert.assertTrue(vertexGroup.getCycleGroupMeta().isIterative());
        Assert.assertTrue(vertexGroup.getCycleGroupMeta().getAffinityLevel() == AffinityLevel.worker);
    }

    @Test
    public void testGroupVertexDiamondDependency() {
        AtomicInteger idGenerator = new AtomicInteger(0);
        AbstractPipelineContext context = mock(AbstractPipelineContext.class);
        when(context.generateId()).then(invocation -> idGenerator.incrementAndGet());

        PWindowSource<Integer> source1 =
            new WindowStreamSource<>(context, new CollectionSource<>(new ArrayList<>()), SizeTumblingWindow.of(10));
        PWindowSource<Integer> source2 =
            new WindowStreamSource<>(context, new CollectionSource<>(new ArrayList<>()), SizeTumblingWindow.of(10));
        PWindowStream<Integer> map1 = source2.map(i -> i + 1);
        new WindowStreamSource<>(context, new CollectionSource<>(new ArrayList<>()), SizeTumblingWindow.of(10));

        PWindowStream<Integer> union1 = source1.union(source2).map(i -> i * 2);
        PWindowStream<Integer> union2 = union1.union(map1);
        PStreamSink sink = union2.sink(r -> {
        });

        when(context.getActions()).thenReturn(ImmutableList.of(sink));

        PipelinePlanBuilder planBuilder = new PipelinePlanBuilder();
        PipelineGraph pipelineGraph = planBuilder.buildPlan(context);
        PipelineGraphOptimizer optimizer = new PipelineGraphOptimizer();
        optimizer.optimizePipelineGraph(pipelineGraph);

        ExecutionGraphBuilder builder = new ExecutionGraphBuilder(pipelineGraph);
        ExecutionGraph graph = builder.buildExecutionGraph(new Configuration());
        Assert.assertEquals(1, graph.getVertexGroupMap().size());
        Assert.assertEquals(0, graph.getGroupEdgeMap().size());
        Assert.assertEquals(5, graph.getVertexGroupMap().get(1).getCycleGroupMeta().getFlyingCount());
        Assert.assertEquals(Long.MAX_VALUE, graph.getVertexGroupMap().get(1).getCycleGroupMeta().getIterationCount());
    }

    @Test
    public void testMultiGraphTraversal() {
        AtomicInteger idGenerator = new AtomicInteger(0);
        AbstractPipelineContext context = mock(AbstractPipelineContext.class);
        when(context.generateId()).then(invocation -> idGenerator.incrementAndGet());
        Configuration configuration = new Configuration();
        when(context.getConfig()).thenReturn(configuration);

        PWindowSource<IVertex<Integer, Integer>> vertices1 =
            new WindowStreamSource<>(context, new CollectionSource<>(new ArrayList<>()), SizeTumblingWindow.of(2));
        PWindowSource<IVertex<Integer, Integer>> vertices2 =
            new WindowStreamSource<>(context, new CollectionSource<>(new ArrayList<>()), SizeTumblingWindow.of(2));

        PWindowSource<IEdge<Integer, Integer>> edges =
            new WindowStreamSource<>(context, new CollectionSource<>(new ArrayList<>()), SizeTumblingWindow.of(2));

        PStreamSource<ITraversalRequest<Integer>> triggerSource =
            new WindowStreamSource<>(context,
                new CollectionSource<>(Lists.newArrayList(new VertexBeginTraversalRequest(3))),
                AllWindow.getInstance());


        PGraphWindow graphWindow1 = new WindowStreamGraph(createGraphViewDesc(4), context,
            vertices1, edges);
        PWindowStream union = graphWindow1.traversal(new GraphTraversalAlgorithms(3))
            .start(triggerSource).union(vertices2);

        PGraphWindow graphWindow2 = new WindowStreamGraph(createGraphViewDesc(4), context,
            union, edges);

        PStreamSink sink = graphWindow2.traversal(new GraphTraversalAlgorithms(3)).start(triggerSource)
            .sink(v -> {
            });


        when(context.getActions()).thenReturn(ImmutableList.of(sink));

        PipelinePlanBuilder planBuilder = new PipelinePlanBuilder();
        PipelineGraph pipelineGraph = planBuilder.buildPlan(context);
        PipelineGraphOptimizer optimizer = new PipelineGraphOptimizer();
        optimizer.optimizePipelineGraph(pipelineGraph);

        ExecutionGraphBuilder builder = new ExecutionGraphBuilder(pipelineGraph);
        ExecutionGraph graph = builder.buildExecutionGraph(new Configuration());
        Assert.assertEquals(7, graph.getVertexGroupMap().size());
        Assert.assertEquals(9, graph.getGroupEdgeMap().size());
        Assert.assertTrue(graph.getCycleGroupMeta().getGroupType() == CycleGroupType.statical);
    }

    public static class IncGraphAlgorithms extends IncVertexCentricCompute<Integer, Integer, Integer, Integer> {

        public IncGraphAlgorithms(long iterations) {
            super(iterations);
        }

        @Override
        public IncVertexCentricComputeFunction<Integer, Integer, Integer, Integer> getIncComputeFunction() {
            return new PRVertexCentricComputeFunction();
        }

        @Override
        public VertexCentricCombineFunction<Integer> getCombineFunction() {
            return null;
        }

    }

    public static class PRVertexCentricComputeFunction implements IncVertexCentricComputeFunction<Integer, Integer, Integer,
        Integer> {

        @Override
        public void init(IncGraphComputeContext<Integer, Integer, Integer, Integer> graphContext) {
        }

        @Override
        public void evolve(Integer vertexId, TemporaryGraph<Integer, Integer, Integer> temporaryGraph) {
        }


        @Override
        public void compute(Integer vertexId, Iterator<Integer> messageIterator) {
        }


        @Override
        public void finish(Integer vertexId, MutableGraph<Integer, Integer, Integer> mutableGraph) {
        }
    }

    public static class PRAlgorithms extends VertexCentricCompute<Integer, Double, Integer,
        Double> {

        public PRAlgorithms(long iterations) {
            super(iterations);
        }

        @Override
        public VertexCentricComputeFunction<Integer, Double, Integer, Double> getComputeFunction() {
            return new PRVertexCentricComputeFunction2();
        }

        @Override
        public VertexCentricCombineFunction<Double> getCombineFunction() {
            return null;
        }

    }

    public static class PRVertexCentricComputeFunction2 implements VertexCentricComputeFunction<Integer, Double, Integer, Double> {

        @Override
        public void init(VertexCentricComputeFuncContext<Integer, Double, Integer, Double> vertexCentricFuncContext) {
        }

        @Override
        public void compute(Integer vertexId, Iterator<Double> messageIterator) {
        }

        @Override
        public void finish() {

        }
    }

    public static class GraphTraversalAlgorithms extends VertexCentricTraversal<Integer,
        Integer, Integer, Integer, Integer> {

        public GraphTraversalAlgorithms(long iterations) {
            super(iterations);
        }

        @Override
        public VertexCentricCombineFunction<Integer> getCombineFunction() {
            return null;
        }

        @Override
        public VertexCentricTraversalFunction<Integer, Integer, Integer, Integer, Integer> getTraversalFunction() {
            return null;
        }
    }

    public static class CountFunc implements ReduceFunction<Tuple<String, Integer>> {

        @Override
        public Tuple<String, Integer> reduce(Tuple<String, Integer> oldValue, Tuple<String, Integer> newValue) {
            return Tuple.of(oldValue.f0, oldValue.f1 + newValue.f1);
        }
    }

    public static class AggFunc implements
        AggregateFunction<Tuple<Long, Long>, Tuple<Long, Long>, Tuple<Long, Long>> {

        @Override
        public Tuple<Long, Long> createAccumulator() {
            return Tuple.of(0L, 0L);
        }

        @Override
        public void add(Tuple<Long, Long> value, Tuple<Long, Long> accumulator) {
            accumulator.setF0(value.f0);
            accumulator.setF1(value.f1 + accumulator.f1);
        }

        @Override
        public Tuple<Long, Long> getResult(Tuple<Long, Long> accumulator) {
            return Tuple.of(accumulator.f0, accumulator.f1);
        }

        @Override
        public Tuple<Long, Long> merge(Tuple<Long, Long> a, Tuple<Long, Long> b) {
            return null;
        }
    }

    private GraphViewDesc createGraphViewDesc(int shardNum) {
        return GraphViewBuilder
            .createGraphView(GraphViewBuilder.DEFAULT_GRAPH)
            .withShardNum(shardNum)
            .withBackend(BackendType.Memory)
            .build();
    }
}
