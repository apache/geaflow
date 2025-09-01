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

package org.apache.geaflow.plan.optimizer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.geaflow.api.function.internal.CollectionSource;
import org.apache.geaflow.api.pdata.PStreamSink;
import org.apache.geaflow.api.window.impl.SizeTumblingWindow;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.FrameworkConfigKeys;
import org.apache.geaflow.context.AbstractPipelineContext;
import org.apache.geaflow.operator.base.AbstractOperator;
import org.apache.geaflow.operator.impl.window.MapOperator;
import org.apache.geaflow.operator.impl.window.SinkOperator;
import org.apache.geaflow.pdata.stream.window.WindowDataStream;
import org.apache.geaflow.pdata.stream.window.WindowStreamSink;
import org.apache.geaflow.pdata.stream.window.WindowStreamSource;
import org.apache.geaflow.plan.PipelinePlanBuilder;
import org.apache.geaflow.plan.graph.PipelineGraph;
import org.apache.geaflow.plan.graph.PipelineVertex;
import org.apache.geaflow.plan.optimizer.strategy.ChainCombiner;
import org.testng.Assert;
import org.testng.annotations.Test;

public class PlanOptimizerTest {

    @Test
    public void testSingleOutput() {
        AtomicInteger idGenerator = new AtomicInteger(0);
        AbstractPipelineContext context = mock(AbstractPipelineContext.class);
        when(context.generateId()).then(invocation -> idGenerator.incrementAndGet());
        Configuration configuration = new Configuration();
        configuration.put(FrameworkConfigKeys.INC_STREAM_MATERIALIZE_DISABLE, Boolean.TRUE.toString());
        when(context.getConfig()).thenReturn(configuration);
        PStreamSink stream = new WindowStreamSource<>(context,
            new CollectionSource<>(new ArrayList<>()), SizeTumblingWindow.of(3))
            .map(p -> p).withParallelism(2).keyBy(q -> q).reduce((oldValue, newValue) -> (oldValue))
            .withParallelism(3).sink(p -> {
            }).withParallelism(3);

        when(context.getActions()).thenReturn(ImmutableList.of(stream));

        Assert.assertEquals(1, context.getActions().size());

        PipelinePlanBuilder planBuilder = new PipelinePlanBuilder();
        PipelineGraph pipelineGraph = planBuilder.buildPlan(context);

        Assert.assertEquals(4, pipelineGraph.getPipelineEdgeList().size());
        Assert.assertEquals(5, pipelineGraph.getPipelineVertices().size());

        ChainCombiner combiner = new ChainCombiner();
        combiner.combineVertex(pipelineGraph);

        Assert.assertEquals(2, pipelineGraph.getPipelineEdgeList().size());
        Assert.assertEquals(3, pipelineGraph.getPipelineVertices().size());

        Assert.assertEquals(1, pipelineGraph.getVertexOutEdges(1).size());
        Assert.assertEquals(2,
            pipelineGraph.getVertexOutEdges(1).stream().findFirst().get().getTargetId());

        Assert.assertEquals(1, pipelineGraph.getVertexOutEdges(2).size());
        Assert.assertEquals(4,
            pipelineGraph.getVertexOutEdges(2).stream().findFirst().get().getTargetId());
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

        Assert.assertEquals(2, context.getActions().size());

        PipelinePlanBuilder planBuilder = new PipelinePlanBuilder();
        PipelineGraph pipelineGraph = planBuilder.buildPlan(context);

        Assert.assertEquals(5, pipelineGraph.getPipelineEdgeList().size());
        Assert.assertEquals(7, pipelineGraph.getPipelineVertices().size());

        ChainCombiner combiner = new ChainCombiner();
        combiner.combineVertex(pipelineGraph);

        Assert.assertEquals(2, pipelineGraph.getPipelineEdgeList().size());
        Assert.assertEquals(4, pipelineGraph.getPipelineVertices().size());

        Assert.assertEquals(1, pipelineGraph.getVertexOutEdges(1).size());
        Assert.assertEquals(3,
            pipelineGraph.getVertexOutEdges(1).stream().findFirst().get().getTargetId());
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
        Assert.assertNotNull(pipelineGraph);
        for (PipelineVertex vertex : pipelineGraph.getPipelineVertices()) {
            Assert.assertTrue(((AbstractOperator) vertex.getOperator()).getNextOperators().isEmpty());
        }

        PipelineGraphOptimizer optimizer = new PipelineGraphOptimizer();
        optimizer.optimizePipelineGraph(pipelineGraph);
        Assert.assertEquals(pipelineGraph.getVertexMap().size(), 1);
        PipelineVertex sourceVertex = pipelineGraph.getVertexMap().get(1);
        Assert.assertEquals(((AbstractOperator) sourceVertex.getOperator()).getNextOperators().size(), 1);
    }
}
