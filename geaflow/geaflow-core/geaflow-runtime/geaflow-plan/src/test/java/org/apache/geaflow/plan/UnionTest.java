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

package org.apache.geaflow.plan;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.geaflow.api.function.base.FilterFunction;
import org.apache.geaflow.api.function.internal.CollectionSource;
import org.apache.geaflow.api.pdata.PStreamSink;
import org.apache.geaflow.api.window.impl.SizeTumblingWindow;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.FrameworkConfigKeys;
import org.apache.geaflow.common.tuple.Tuple;
import org.apache.geaflow.context.AbstractPipelineContext;
import org.apache.geaflow.pdata.stream.window.WindowStreamSource;
import org.apache.geaflow.plan.graph.PipelineGraph;
import org.apache.geaflow.plan.graph.PipelineVertex;
import org.apache.geaflow.plan.optimizer.PipelineGraphOptimizer;
import org.testng.Assert;
import org.testng.annotations.Test;

public class UnionTest {

    @Test
    public void testUnionPlan() {
        AtomicInteger idGenerator = new AtomicInteger(0);
        AbstractPipelineContext context = mock(AbstractPipelineContext.class);
        when(context.generateId()).then(invocation -> idGenerator.incrementAndGet());
        Configuration configuration = new Configuration();
        configuration.put(FrameworkConfigKeys.INC_STREAM_MATERIALIZE_DISABLE, Boolean.TRUE.toString());
        when(context.getConfig()).thenReturn(configuration);

        WindowStreamSource<Tuple<Long, Long>> ds1 = new WindowStreamSource<>(context,
            new CollectionSource<>(Tuple.of(1L, 3L), Tuple.of(2L, 5L), Tuple.of(3L, 7L),
                Tuple.of(1L, 3L), Tuple.of(1L, 7L), Tuple.of(3L, 7L)), SizeTumblingWindow.of(2));

        WindowStreamSource<Tuple<Long, Long>> ds2 = new WindowStreamSource<>(context,
            new CollectionSource<>(Tuple.of(1L, 3L), Tuple.of(2L, 5L), Tuple.of(3L, 7L),
                Tuple.of(1L, 3L), Tuple.of(1L, 7L), Tuple.of(3L, 7L)), SizeTumblingWindow.of(2));

        PStreamSink sink = ds1.union(ds2).keyBy(p -> p).filter((FilterFunction<Tuple<Long, Long>>) record -> true).sink(p -> {
        });
        when(context.getActions()).thenReturn(ImmutableList.of(sink));

        //ds1_source->union->ds2_source->filter->print;
        PipelinePlanBuilder planBuilder = new PipelinePlanBuilder();
        PipelineGraph pipelineGraph = planBuilder.buildPlan(context);
        Map<Integer, PipelineVertex> vertexMap = pipelineGraph.getVertexMap();
        Assert.assertEquals(vertexMap.size(), 6);

        PipelineGraphOptimizer optimizer = new PipelineGraphOptimizer();
        optimizer.optimizePipelineGraph(pipelineGraph, new Configuration());
        Assert.assertEquals(vertexMap.size(), 4);
    }

    @Test
    public void testMultiUnionPlan() {
        AtomicInteger idGenerator = new AtomicInteger(0);
        AbstractPipelineContext context = mock(AbstractPipelineContext.class);
        when(context.generateId()).then(invocation -> idGenerator.incrementAndGet());
        Configuration configuration = new Configuration();
        configuration.put(FrameworkConfigKeys.INC_STREAM_MATERIALIZE_DISABLE, Boolean.TRUE.toString());
        when(context.getConfig()).thenReturn(configuration);

        WindowStreamSource<Tuple<Long, Long>> ds1 = new WindowStreamSource<>(context,
            new CollectionSource<>(Tuple.of(1L, 3L), Tuple.of(2L, 5L), Tuple.of(3L, 7L),
                Tuple.of(1L, 3L), Tuple.of(1L, 7L), Tuple.of(3L, 7L)), SizeTumblingWindow.of(2));

        WindowStreamSource<Tuple<Long, Long>> ds2 = new WindowStreamSource<>(context,
            new CollectionSource<>(Tuple.of(1L, 3L), Tuple.of(2L, 5L), Tuple.of(3L, 7L),
                Tuple.of(1L, 3L), Tuple.of(1L, 7L), Tuple.of(3L, 7L)), SizeTumblingWindow.of(2));

        WindowStreamSource<Tuple<Long, Long>> ds3 = new WindowStreamSource<>(context,
            new CollectionSource<>(Tuple.of(1L, 3L), Tuple.of(2L, 5L), Tuple.of(3L, 7L),
                Tuple.of(1L, 3L), Tuple.of(1L, 7L), Tuple.of(3L, 7L)), SizeTumblingWindow.of(2));

        PStreamSink sink = ds1.union(ds2).union(ds3).keyBy(p -> p)
            .filter((FilterFunction<Tuple<Long, Long>>) record -> true).sink(p -> {
            });
        when(context.getActions()).thenReturn(ImmutableList.of(sink));

        //ds1_source->union->ds2_source->ds3_source->filter->print;
        PipelinePlanBuilder planBuilder = new PipelinePlanBuilder();
        PipelineGraph pipelineGraph = planBuilder.buildPlan(context);
        Map<Integer, PipelineVertex> vertexMap = pipelineGraph.getVertexMap();
        Assert.assertEquals(vertexMap.size(), 7);

        PipelineGraphOptimizer optimizer = new PipelineGraphOptimizer();
        optimizer.optimizePipelineGraph(pipelineGraph, new Configuration());
        Assert.assertEquals(vertexMap.size(), 5);
    }

    @Test
    public void testUnionWithKeyByPlan() {
        AtomicInteger idGenerator = new AtomicInteger(0);
        AbstractPipelineContext context = mock(AbstractPipelineContext.class);
        when(context.generateId()).then(invocation -> idGenerator.incrementAndGet());
        Configuration configuration = new Configuration();
        when(context.getConfig()).thenReturn(configuration);

        WindowStreamSource<Tuple<Long, Long>> ds1 = new WindowStreamSource<>(context,
            new CollectionSource<>(Tuple.of(1L, 3L), Tuple.of(2L, 5L), Tuple.of(3L, 7L),
                Tuple.of(1L, 3L), Tuple.of(1L, 7L), Tuple.of(3L, 7L)), SizeTumblingWindow.of(2));

        WindowStreamSource<Tuple<Long, Long>> ds2 = new WindowStreamSource<>(context,
            new CollectionSource<>(Tuple.of(1L, 3L), Tuple.of(2L, 5L), Tuple.of(3L, 7L),
                Tuple.of(1L, 3L), Tuple.of(1L, 7L), Tuple.of(3L, 7L)), SizeTumblingWindow.of(2));

        PStreamSink sink = ds1.union(ds2).keyBy(p -> p).reduce((v1, v2) -> v2).sink(p -> {
        });
        when(context.getActions()).thenReturn(ImmutableList.of(sink));

        //ds1_source->union->ds2_source->keyBy->reduce->print;
        PipelinePlanBuilder planBuilder = new PipelinePlanBuilder();
        PipelineGraph pipelineGraph = planBuilder.buildPlan(context);
        Map<Integer, PipelineVertex> vertexMap = pipelineGraph.getVertexMap();
        Assert.assertEquals(vertexMap.size(), 6);

        PipelineGraphOptimizer optimizer = new PipelineGraphOptimizer();
        optimizer.optimizePipelineGraph(pipelineGraph, new Configuration());
        Assert.assertEquals(vertexMap.size(), 4);
    }

    @Test
    public void testWindowUnionWithKeyByPlan() {
        AtomicInteger idGenerator = new AtomicInteger(0);
        AbstractPipelineContext context = mock(AbstractPipelineContext.class);
        when(context.generateId()).then(invocation -> idGenerator.incrementAndGet());
        Configuration configuration = new Configuration();
        configuration.put(FrameworkConfigKeys.INC_STREAM_MATERIALIZE_DISABLE, Boolean.TRUE.toString());
        when(context.getConfig()).thenReturn(configuration);

        WindowStreamSource<Tuple<Long, Long>> ds1 = new WindowStreamSource<>(context,
            new CollectionSource<>(Tuple.of(1L, 3L), Tuple.of(2L, 5L), Tuple.of(3L, 7L),
                Tuple.of(1L, 3L), Tuple.of(1L, 7L), Tuple.of(3L, 7L)), SizeTumblingWindow.of(2));

        WindowStreamSource<Tuple<Long, Long>> ds2 = new WindowStreamSource<>(context,
            new CollectionSource<>(Tuple.of(1L, 3L), Tuple.of(2L, 5L), Tuple.of(3L, 7L),
                Tuple.of(1L, 3L), Tuple.of(1L, 7L), Tuple.of(3L, 7L)), SizeTumblingWindow.of(2));

        PStreamSink sink = ds1.union(ds2).keyBy(p -> p).reduce((v1, v2) -> v2).sink(p -> {
        });
        when(context.getActions()).thenReturn(ImmutableList.of(sink));

        //ds1_source->union->ds2_source->keyBy->reduce->print;
        PipelinePlanBuilder planBuilder = new PipelinePlanBuilder();
        PipelineGraph pipelineGraph = planBuilder.buildPlan(context);
        Map<Integer, PipelineVertex> vertexMap = pipelineGraph.getVertexMap();
        Assert.assertEquals(vertexMap.size(), 6);

        PipelineGraphOptimizer optimizer = new PipelineGraphOptimizer();
        optimizer.optimizePipelineGraph(pipelineGraph, new Configuration());
        Assert.assertEquals(vertexMap.size(), 4);
    }

}
