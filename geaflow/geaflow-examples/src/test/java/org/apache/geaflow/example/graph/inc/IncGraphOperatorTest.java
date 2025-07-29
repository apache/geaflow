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

package org.apache.geaflow.example.graph.inc;

import com.google.common.collect.Lists;
import java.lang.reflect.Field;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.geaflow.api.function.internal.CollectionSource;
import org.apache.geaflow.api.graph.compute.IncVertexCentricCompute;
import org.apache.geaflow.api.graph.function.vc.IncVertexCentricComputeFunction;
import org.apache.geaflow.api.graph.function.vc.VertexCentricCombineFunction;
import org.apache.geaflow.api.pdata.stream.window.PWindowSource;
import org.apache.geaflow.api.window.WindowFactory;
import org.apache.geaflow.api.window.impl.SizeTumblingWindow;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.common.type.primitive.IntegerType;
import org.apache.geaflow.env.EnvironmentFactory;
import org.apache.geaflow.example.base.BaseTest;
import org.apache.geaflow.file.FileConfigKeys;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.edge.impl.ValueEdge;
import org.apache.geaflow.model.graph.meta.GraphMetaType;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.model.graph.vertex.impl.ValueVertex;
import org.apache.geaflow.operator.impl.graph.algo.vc.context.dynamic.IncHistoricalGraph;
import org.apache.geaflow.operator.impl.graph.algo.vc.context.dynamic.IncTemporaryGraph;
import org.apache.geaflow.operator.impl.graph.compute.dynamic.cache.TemporaryGraphCache;
import org.apache.geaflow.pipeline.IPipelineResult;
import org.apache.geaflow.pipeline.Pipeline;
import org.apache.geaflow.pipeline.PipelineFactory;
import org.apache.geaflow.pipeline.task.PipelineTask;
import org.apache.geaflow.state.GraphState;
import org.apache.geaflow.view.GraphViewBuilder;
import org.apache.geaflow.view.IViewDesc;
import org.apache.geaflow.view.graph.GraphViewDesc;
import org.apache.geaflow.view.graph.PGraphView;
import org.apache.geaflow.view.graph.PIncGraphView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class IncGraphOperatorTest extends BaseTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(IncGraphOperatorTest.class);

    @Test
    public void testDynamicGraphVertexCentricComputeOp() {
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration config = environment.getEnvironmentContext().getConfig();
        config.put(ExecutionConfigKeys.JOB_APP_NAME.getKey(), getClass().getSimpleName());
        config.put(FileConfigKeys.ROOT.getKey(), "/tmp/");

        Pipeline pipeline = PipelineFactory.buildPipeline(environment);

        int sourceParallelism = 1;
        int shardNum = 1;
        int iterationParallelism = 1;
        int sinkParallelism = 1;        //build graph view

        final String graphName = "graph_view_name";
        GraphViewDesc graphViewDesc = GraphViewBuilder.createGraphView(graphName)
            .withShardNum(shardNum)
            .withBackend(IViewDesc.BackendType.RocksDB)
            .withSchema(new GraphMetaType<>(IntegerType.INSTANCE, ValueVertex.class,
                Object.class, ValueEdge.class, Object.class))
            .build();
        pipeline.withView(graphName, graphViewDesc);

        pipeline.submit((PipelineTask) pipelineTaskCxt -> {
            List<IVertex<Integer, Integer>> vertexSource = Lists.newArrayList(
                new ValueVertex<>(1), new ValueVertex<>(2),
                new ValueVertex<>(3), new ValueVertex<>(4),
                new ValueVertex<>(5), new ValueVertex<>(6));

            List<IEdge<Integer, Integer>> edgeSource = Lists.newArrayList(
                new ValueEdge<>(1, 2),
                new ValueEdge<>(3, 4),
                new ValueEdge<>(5, 6));

            PWindowSource<IVertex<Integer, Integer>> vertices =
                pipelineTaskCxt.buildSource(new CollectionSource<>(vertexSource),
                        SizeTumblingWindow.of(2))
                    .window(WindowFactory.createSizeTumblingWindow(3))
                    .withParallelism(sourceParallelism);

            PWindowSource<IEdge<Integer, Integer>> edges =
                pipelineTaskCxt.buildSource(new CollectionSource<>(edgeSource),
                        SizeTumblingWindow.of(1))
                    .window(WindowFactory.createSizeTumblingWindow(3))
                    .withParallelism(sourceParallelism);

            PGraphView<Integer, Integer, Integer> fundGraphView =
                pipelineTaskCxt.getGraphView(graphName);

            PIncGraphView<Integer, Integer, Integer> incGraphView =
                fundGraphView.appendGraph(vertices, edges);

            incGraphView.incrementalCompute(new IncGraphAlgorithms(2))
                // incremental compute operator with 2 parallelism.
                .compute(iterationParallelism)
                .getVertices()
                .sink(v -> {
                })
                .withParallelism(sinkParallelism);

        });
        IPipelineResult result = pipeline.execute();
        result.get();
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

    public static class PRVertexCentricComputeFunction implements IncVertexCentricComputeFunction<Integer, Integer, Integer, Integer> {

        private IncGraphComputeContext<Integer, Integer, Integer, Integer> graphContext;

        @Override
        public void init(IncGraphComputeContext<Integer, Integer, Integer, Integer> graphContext) {
            this.graphContext = graphContext;
            TemporaryGraph<Integer, Integer, Integer> temporaryGraph =
                this.graphContext.getTemporaryGraph();
            TemporaryGraphCache temporaryGraphCache = (TemporaryGraphCache) getObjectField(temporaryGraph,
                IncTemporaryGraph.class, "temporaryGraphCache");
            Set<Integer> allEvolveVId = temporaryGraphCache.getAllEvolveVId();
            Assert.assertEquals(allEvolveVId.size(), 0);
        }

        @Override
        public void evolve(Integer vertexId, TemporaryGraph<Integer, Integer, Integer> temporaryGraph) {
            this.graphContext.sendMessage(999, 1);
            TemporaryGraphCache temporaryGraphCache = (TemporaryGraphCache) getObjectField(temporaryGraph,
                IncTemporaryGraph.class, "temporaryGraphCache");
            Set<Integer> allEvolveVId = temporaryGraphCache.getAllEvolveVId();
            Assert.assertEquals(allEvolveVId.size(), 2);

            long nestedWindowId = this.graphContext.getRuntimeContext().getWindowId();
            HistoricalGraph<Integer, Integer, Integer> historicalGraph = this.graphContext.getHistoricalGraph();
            GraphState graphState = (GraphState) getObjectField(historicalGraph,
                IncHistoricalGraph.class, "graphState");
            if (nestedWindowId == 2L) {
                List<Long> allVersions = graphState.dynamicGraph().V().getAllVersions(1);
                Assert.assertEquals(allVersions.size(), 1);
            }
        }

        @Override
        public void compute(Integer vertexId, Iterator<Integer> messageIterator) {
            int sum = 0;
            Assert.assertEquals((int) vertexId, 999);
            while (messageIterator.hasNext()) {
                sum += messageIterator.next();
            }
            Assert.assertEquals(sum, 2);
        }

        @Override
        public void finish(Integer vertexId, MutableGraph<Integer, Integer, Integer> mutableGraph) {
            IVertex<Integer, Integer> vertex = graphContext.getTemporaryGraph().getVertex();
            List<IEdge<Integer, Integer>> edges = graphContext.getTemporaryGraph().getEdges();
            if (vertex != null) {
                mutableGraph.addVertex(0, vertex);
                graphContext.collect(vertex);
            } else {
                LOGGER.info("not found vertex {} in temporaryGraph ", vertexId);
            }
            if (edges != null) {
                edges.stream().forEach(edge -> {
                    mutableGraph.addEdge(0, edge);
                });
            }
        }

    }

    private static Object getObjectField(Object instance, Class clazz, String fieldName) {
        try {
            Field field = clazz.getDeclaredField(fieldName);
            field.setAccessible(true);
            return field.get(instance);
        } catch (IllegalAccessException | NoSuchFieldException e) {
            throw new GeaflowRuntimeException(e);
        }
    }

}
