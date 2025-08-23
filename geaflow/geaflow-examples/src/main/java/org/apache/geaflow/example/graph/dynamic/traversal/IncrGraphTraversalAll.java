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

package org.apache.geaflow.example.graph.dynamic.traversal;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import org.apache.geaflow.api.function.io.SinkFunction;
import org.apache.geaflow.api.graph.function.vc.IncVertexCentricTraversalFunction;
import org.apache.geaflow.api.graph.function.vc.VertexCentricCombineFunction;
import org.apache.geaflow.api.graph.traversal.IncVertexCentricTraversal;
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
import org.apache.geaflow.model.traversal.ITraversalRequest;
import org.apache.geaflow.model.traversal.ITraversalResponse;
import org.apache.geaflow.model.traversal.TraversalType.ResponseType;
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

public class IncrGraphTraversalAll {

    private static final Logger LOGGER = LoggerFactory.getLogger(IncrGraphTraversalAll.class);

    public static final String RESULT_FILE_PATH = "./target/tmp/data/result/incr_graph_traversal_all";
    public static final String REF_FILE_PATH = "data/reference/incr_graph_traversal_all";

    public static void main(String[] args) {
        Environment environment = EnvironmentUtil.loadEnvironment(args);
        submit(environment);
    }

    public static IPipelineResult submit(Environment environment) {
        final Pipeline pipeline = PipelineFactory.buildPipeline(environment);
        Configuration envConfig = environment.getEnvironmentContext().getConfig();
        envConfig.getConfigMap().put(FileSink.OUTPUT_DIR, RESULT_FILE_PATH);
        ResultValidator.cleanResult(RESULT_FILE_PATH);

        //build graph view
        final String graphName = "graph_view_name";
        GraphViewDesc graphViewDesc = GraphViewBuilder.createGraphView(graphName)
            .withShardNum(envConfig.getInteger(ExampleConfigKeys.ITERATOR_PARALLELISM))
            .withBackend(BackendType.RocksDB)
            .withSchema(new GraphMetaType(IntegerType.INSTANCE, ValueVertex.class,
                Integer.class, ValueEdge.class, IntegerType.class))
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
                        .withParallelism(conf.getInteger(ExampleConfigKeys.SOURCE_PARALLELISM));

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
                incGraphView.incrementalTraversal(new IncGraphTraversalAlgorithms(3))
                    .start()
                    .map(res -> res.toString())
                    .withParallelism(mapParallelism)
                    .sink(sink)
                    .withParallelism(sinkParallelism);

            }
        });

        return pipeline.execute();
    }

    public static void validateResult() throws IOException {
        ResultValidator.validateMapResult(REF_FILE_PATH, RESULT_FILE_PATH,
            Comparator.comparingLong(IncrGraphTraversalAll::parseSumValue));
    }

    private static long parseSumValue(String result) {
        String sumValue = result.split(",")[1];
        return Long.parseLong(sumValue);
    }

    public static class IncGraphTraversalAlgorithms extends IncVertexCentricTraversal<Integer,
        Integer, Integer,
        Integer, Integer> {

        public IncGraphTraversalAlgorithms(long iterations) {
            super(iterations);
        }

        @Override
        public IncVertexCentricTraversalFunction<Integer, Integer, Integer, Integer, Integer> getIncTraversalFunction() {
            return new IncVertexCentricTraversalFunction<Integer, Integer, Integer, Integer, Integer>() {

                private IncVertexCentricTraversalFuncContext<Integer, Integer, Integer, Integer, Integer> vertexCentricFuncContext;

                @Override
                public void open(
                    IncVertexCentricTraversalFuncContext<Integer, Integer, Integer, Integer,
                        Integer> vertexCentricFuncContext) {
                    this.vertexCentricFuncContext = vertexCentricFuncContext;
                }

                @Override
                public void evolve(Integer vertexId,
                                   TemporaryGraph<Integer, Integer, Integer> temporaryGraph) {
                    LOGGER.debug("evolve vId:{}", vertexId);
                    MutableGraph<Integer, Integer,
                        Integer> mutableGraph = this.vertexCentricFuncContext.getMutableGraph();
                    IVertex<Integer, Integer> vertex = temporaryGraph.getVertex();
                    if (vertex != null) {
                        mutableGraph.addVertex(0, vertex);
                    }
                    List<IEdge<Integer, Integer>> edges = temporaryGraph.getEdges();
                    if (edges != null) {
                        for (IEdge<Integer, Integer> edge : edges) {
                            mutableGraph.addEdge(0, edge);
                        }
                    }
                }

                @Override
                public void init(ITraversalRequest<Integer> traversalRequest) {
                    int requestId = traversalRequest.getVId();
                    List<IEdge<Integer, Integer>> edges =
                        this.vertexCentricFuncContext.getHistoricalGraph().getSnapShot(0).edges().getEdges();
                    int sum = 0;
                    if (edges != null) {
                        for (IEdge<Integer, Integer> edge : edges) {
                            sum += edge.getValue();
                        }
                    }
                    this.vertexCentricFuncContext.takeResponse(new TraversalResponse(requestId,
                        sum));
                }

                @Override
                public void finish() {

                }

                @Override
                public void close() {

                }

                @Override
                public void compute(Integer vertexId,
                                    Iterator<Integer> messageIterator) {
                }

                @Override
                public void finish(Integer vertexId,
                                   MutableGraph<Integer, Integer, Integer> mutableGraph) {
                }
            };
        }

        @Override
        public VertexCentricCombineFunction<Integer> getCombineFunction() {
            return null;
        }

    }

    static class TraversalResponse implements ITraversalResponse<Integer> {

        private long responseId;
        private int value;

        public TraversalResponse(long responseId, int value) {
            this.responseId = responseId;
            this.value = value;
        }

        @Override
        public long getResponseId() {
            return responseId;
        }

        @Override
        public Integer getResponse() {
            return value;
        }

        @Override
        public ResponseType getType() {
            return ResponseType.Vertex;
        }

        @Override
        public String toString() {
            return responseId + "," + value;
        }
    }

}
