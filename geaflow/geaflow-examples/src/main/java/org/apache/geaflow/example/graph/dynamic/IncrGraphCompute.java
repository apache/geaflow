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

package org.apache.geaflow.example.graph.dynamic;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import org.apache.geaflow.api.function.io.SinkFunction;
import org.apache.geaflow.api.graph.compute.IncVertexCentricCompute;
import org.apache.geaflow.api.graph.function.vc.IncVertexCentricComputeFunction;
import org.apache.geaflow.api.graph.function.vc.VertexCentricCombineFunction;
import org.apache.geaflow.api.pdata.stream.window.PWindowSource;
import org.apache.geaflow.api.window.impl.SizeTumblingWindow;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.type.primitive.IntegerType;
import org.apache.geaflow.env.Environment;
import org.apache.geaflow.example.config.ExampleConfigKeys;
import org.apache.geaflow.example.function.FileSink;
import org.apache.geaflow.example.function.RecoverableFileSource;
import org.apache.geaflow.example.util.EnvironmentUtil;
import org.apache.geaflow.example.util.ExampleSinkFunctionFactory;
import org.apache.geaflow.example.util.ResultValidator;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.edge.impl.ValueEdge;
import org.apache.geaflow.model.graph.meta.GraphMetaType;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.model.graph.vertex.impl.ValueVertex;
import org.apache.geaflow.pipeline.IPipelineResult;
import org.apache.geaflow.pipeline.Pipeline;
import org.apache.geaflow.pipeline.PipelineFactory;
import org.apache.geaflow.pipeline.task.IPipelineTaskContext;
import org.apache.geaflow.pipeline.task.PipelineTask;
import org.apache.geaflow.view.GraphViewBuilder;
import org.apache.geaflow.view.IViewDesc.BackendType;
import org.apache.geaflow.view.graph.GraphViewDesc;
import org.apache.geaflow.view.graph.PGraphView;
import org.apache.geaflow.view.graph.PIncGraphView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IncrGraphCompute {

    private static final Logger LOGGER = LoggerFactory.getLogger(IncrGraphCompute.class);

    public static final String RESULT_FILE_PATH = "./target/tmp/data/result/incr_graph";
    public static final String REF_FILE_PATH = "data/reference/incr_graph";

    public static void main(String[] args) {
        Environment environment = EnvironmentUtil.loadEnvironment(args);
        submit(environment);
    }

    public static IPipelineResult<?> submit(Environment environment) {
        final Pipeline pipeline = PipelineFactory.buildPipeline(environment);
        Configuration envConfig = environment.getEnvironmentContext().getConfig();
        envConfig.put(FileSink.OUTPUT_DIR, RESULT_FILE_PATH);
        ResultValidator.cleanResult(RESULT_FILE_PATH);

        //build graph view
        final String graphName = "graph_view_name";
        GraphViewDesc graphViewDesc = GraphViewBuilder.createGraphView(graphName)
            .withShardNum(envConfig.getInteger(ExampleConfigKeys.ITERATOR_PARALLELISM))
            .withBackend(BackendType.RocksDB)
            .withSchema(new GraphMetaType(IntegerType.INSTANCE, ValueVertex.class, Integer.class,
                ValueEdge.class, IntegerType.class))
            .build();
        pipeline.withView(graphName, graphViewDesc);

        pipeline.submit(new PipelineTask() {
            @Override
            public void execute(IPipelineTaskContext pipelineTaskCxt) {
                Configuration conf = pipelineTaskCxt.getConfig();
                PWindowSource<IVertex<Integer, Integer>> vertices =
                    // extract vertex from edge file
                    pipelineTaskCxt.buildSource(new RecoverableFileSource<>("data/input/email_edge",
                        line -> {
                            String[] fields = line.split(",");
                            IVertex<Integer, Integer> vertex1 = new ValueVertex<>(
                                Integer.valueOf(fields[0]), 1);
                            IVertex<Integer, Integer> vertex2 = new ValueVertex<>(
                                Integer.valueOf(fields[1]), 1);
                            return Arrays.asList(vertex1, vertex2);
                        }), SizeTumblingWindow.of(10000))
                        .withParallelism(pipelineTaskCxt.getConfig().getInteger(ExampleConfigKeys.SOURCE_PARALLELISM));

                PWindowSource<IEdge<Integer, Integer>> edges =
                    pipelineTaskCxt.buildSource(new RecoverableFileSource<>("data/input/email_edge",
                        line -> {
                            String[] fields = line.split(",");
                            IEdge<Integer, Integer> edge = new ValueEdge<>(Integer.valueOf(fields[0]),
                                Integer.valueOf(fields[1]), 1);
                            return Collections.singletonList(edge);
                        }), SizeTumblingWindow.of(5000));


                PGraphView<Integer, Integer, Integer> fundGraphView =
                    pipelineTaskCxt.getGraphView(graphName);

                PIncGraphView<Integer, Integer, Integer> incGraphView =
                    fundGraphView.appendGraph(vertices, edges);
                int mapParallelism = pipelineTaskCxt.getConfig().getInteger(ExampleConfigKeys.MAP_PARALLELISM);
                int sinkParallelism = pipelineTaskCxt.getConfig().getInteger(ExampleConfigKeys.SINK_PARALLELISM);
                SinkFunction<String> sink = ExampleSinkFunctionFactory.getSinkFunction(conf);
                incGraphView.incrementalCompute(new IncGraphAlgorithms(3))
                    .getVertices()
                    .map(v -> String.format("%s,%s", v.getId(), v.getValue()))
                    .withParallelism(mapParallelism)
                    .sink(sink)
                    .withParallelism(sinkParallelism);
            }
        });

        return pipeline.execute();
    }

    public static void validateResult() throws IOException {
        ResultValidator.validateMapResult(REF_FILE_PATH, RESULT_FILE_PATH,
            Comparator.comparingLong(IncrGraphCompute::parseSumValue));
    }

    private static long parseSumValue(String result) {
        String sumValue = result.split(",")[1];
        return Long.parseLong(sumValue);
    }

    public static class IncGraphAlgorithms extends IncVertexCentricCompute<Integer, Integer,
        Integer, Integer> {

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

    public static class PRVertexCentricComputeFunction implements
        IncVertexCentricComputeFunction<Integer, Integer, Integer, Integer> {

        private IncGraphComputeContext<Integer, Integer, Integer, Integer> graphContext;

        @Override
        public void init(IncGraphComputeContext<Integer, Integer, Integer, Integer> graphContext) {
            this.graphContext = graphContext;
        }

        @Override
        public void evolve(Integer vertexId,
                           TemporaryGraph<Integer, Integer, Integer> temporaryGraph) {
            long lastVersionId = 0L;
            IVertex<Integer, Integer> vertex = temporaryGraph.getVertex();
            HistoricalGraph<Integer, Integer, Integer> historicalGraph = graphContext
                .getHistoricalGraph();
            if (vertex == null) {
                vertex = historicalGraph.getSnapShot(lastVersionId).vertex().get();
            }

            if (vertex != null) {
                List<IEdge<Integer, Integer>> newEs = temporaryGraph.getEdges();
                List<IEdge<Integer, Integer>> oldEs = historicalGraph.getSnapShot(lastVersionId)
                    .edges().getOutEdges();
                if (newEs != null) {
                    for (IEdge<Integer, Integer> edge : newEs) {

                        graphContext.sendMessage(edge.getTargetId(), vertexId);
                    }
                }
                if (oldEs != null) {
                    for (IEdge<Integer, Integer> edge : oldEs) {
                        graphContext.sendMessage(edge.getTargetId(), vertexId);
                    }
                }
            }

        }

        @Override
        public void compute(Integer vertexId, Iterator<Integer> messageIterator) {
            int max = 0;
            while (messageIterator.hasNext()) {
                int value = messageIterator.next();
                max = max > value ? max : value;
            }
            IVertex<Integer, Integer> vertex = graphContext.getTemporaryGraph().getVertex();
            IVertex<Integer, Integer> historyVertex = graphContext.getHistoricalGraph().getSnapShot(0).vertex().get();
            if (vertex != null && max < vertex.getValue()) {
                max = vertex.getValue();
            }
            if (historyVertex != null && max < historyVertex.getValue()) {
                max = historyVertex.getValue();
            }
            graphContext.getTemporaryGraph().updateVertexValue(max);
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

}
