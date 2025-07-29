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

package org.apache.geaflow.example.graph.statical.compute.personalrank;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.geaflow.api.function.io.SinkFunction;
import org.apache.geaflow.api.graph.PGraphWindow;
import org.apache.geaflow.api.graph.compute.VertexCentricCompute;
import org.apache.geaflow.api.graph.function.vc.VertexCentricCombineFunction;
import org.apache.geaflow.api.graph.function.vc.VertexCentricComputeFunction;
import org.apache.geaflow.api.pdata.stream.window.PWindowSource;
import org.apache.geaflow.api.window.impl.AllWindow;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.env.Environment;
import org.apache.geaflow.example.config.ExampleConfigKeys;
import org.apache.geaflow.example.function.AbstractVcFunc;
import org.apache.geaflow.example.function.FileSink;
import org.apache.geaflow.example.function.FileSource;
import org.apache.geaflow.example.util.EnvironmentUtil;
import org.apache.geaflow.example.util.ExampleSinkFunctionFactory;
import org.apache.geaflow.example.util.PipelineResultCollect;
import org.apache.geaflow.example.util.ResultValidator;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.edge.impl.ValueEdge;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.model.graph.vertex.impl.ValueVertex;
import org.apache.geaflow.pipeline.IPipelineResult;
import org.apache.geaflow.pipeline.Pipeline;
import org.apache.geaflow.pipeline.PipelineFactory;
import org.apache.geaflow.pipeline.task.PipelineTask;
import org.apache.geaflow.view.GraphViewBuilder;
import org.apache.geaflow.view.IViewDesc.BackendType;
import org.apache.geaflow.view.graph.GraphViewDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PersonalRank {

    private static final Logger LOGGER = LoggerFactory.getLogger(PersonalRank.class);

    private static final int ROOT_ID = 1;

    private static final int MAX_ITERATIONS = 10;

    private static final double ALPHA = 0.85;

    private static final double CONVERGENCE = 0.00001;

    public static final String RESULT_FILE_DIR = "./target/tmp/data/result/personalrank";

    public static final String REF_FILE_DIR = "data/reference/personalrank";

    public static void main(String[] args) {
        Environment environment = EnvironmentUtil.loadEnvironment(args);
        IPipelineResult result = submit(environment);
        PipelineResultCollect.get(result);
        environment.shutdown();
    }

    public static IPipelineResult submit(Environment environment) {
        ResultValidator.cleanResult(RESULT_FILE_DIR);
        Configuration envConfig = environment.getEnvironmentContext().getConfig();
        envConfig.put(FileSink.OUTPUT_DIR, RESULT_FILE_DIR);

        Pipeline pipeline = PipelineFactory.buildPipeline(environment);
        pipeline.submit((PipelineTask) pipelineTaskCxt -> {
            Configuration config = pipelineTaskCxt.getConfig();
            int sourceParallelism = config.getInteger(ExampleConfigKeys.SOURCE_PARALLELISM);
            int iterationParallelism = config.getInteger(ExampleConfigKeys.ITERATOR_PARALLELISM);
            int sinkParallelism = config.getInteger(ExampleConfigKeys.SINK_PARALLELISM);
            LOGGER.info("with {} {} {}", sourceParallelism, iterationParallelism, sinkParallelism);

            PWindowSource<IVertex<Integer, Double>> prVertices =
                pipelineTaskCxt.buildSource(new FileSource<>("data/input/email_vertex",
                    line -> {
                        String[] fields = line.split(",");
                        IVertex<Integer, Double> vertex = new ValueVertex<>(
                            Integer.valueOf(fields[0]), Double.valueOf(fields[1]));
                        return Collections.singletonList(vertex);
                    }), AllWindow.getInstance())
                    .withParallelism(sourceParallelism);

            PWindowSource<IEdge<Integer, Integer>> prEdges =
                pipelineTaskCxt.buildSource(new FileSource<>("data/input/email_edge",
                    line -> {
                        String[] fields = line.split(",");
                        IEdge<Integer, Integer> edge = new ValueEdge<>(
                            Integer.valueOf(fields[0]), Integer.valueOf(fields[1]), 1);
                        return Collections.singletonList(edge);
                    }), AllWindow.getInstance())
                    .withParallelism(sourceParallelism);

            GraphViewDesc graphViewDesc = GraphViewBuilder
                .createGraphView(GraphViewBuilder.DEFAULT_GRAPH)
                .withShardNum(iterationParallelism)
                .withBackend(BackendType.Memory)
                .build();
            PGraphWindow<Integer, Double, Integer> graphWindow =
                pipelineTaskCxt.buildWindowStreamGraph(prVertices, prEdges, graphViewDesc);

            SinkFunction<IVertex<Integer, Double>> sink = ExampleSinkFunctionFactory.getSinkFunction(config);
            graphWindow
                .compute(new PersonalRankAlgorithms(ROOT_ID, MAX_ITERATIONS, ALPHA, CONVERGENCE))
                .compute(iterationParallelism)
                .getVertices()
                .sink(sink)
                .withParallelism(sinkParallelism);
        });

        return pipeline.execute();
    }

    public static void validateResult() throws IOException {
        ResultValidator.validateResult(REF_FILE_DIR, RESULT_FILE_DIR);
    }

    public static class PersonalRankAlgorithms extends VertexCentricCompute<Integer, Double, Integer, Double> {

        private final int rootId;

        private final double alpha;

        private final double convergence;

        public PersonalRankAlgorithms(int rootId, long iterations, double alpha, double convergence) {
            super(iterations);
            this.rootId = rootId;
            this.alpha = alpha;
            this.convergence = convergence;
        }

        @Override
        public VertexCentricComputeFunction<Integer, Double, Integer, Double> getComputeFunction() {
            return new PersonalRankVertexCentricComputeFunction(rootId, alpha, convergence);
        }

        @Override
        public VertexCentricCombineFunction<Double> getCombineFunction() {
            return null;
        }

    }

    public static class PersonalRankVertexCentricComputeFunction extends AbstractVcFunc<Integer, Double, Integer, Double> {

        private final int rootId;

        private final double alpha;

        private final double convergence;

        public PersonalRankVertexCentricComputeFunction(int rootId, double alpha, double convergence) {
            this.rootId = rootId;
            this.convergence = convergence;
            this.alpha = alpha;
        }

        @Override
        public void compute(Integer vertexId,
                            Iterator<Double> messageIterator) {
            IVertex<Integer, Double> vertex = this.context.vertex().get();
            if (this.context.getIterationId() == 1) {
                if (vertex.getId() == rootId) {
                    double score = 1.0;
                    calc(score);
                } else {
                    this.context.setNewVertexValue(0.0);
                }
            } else {
                double score = vertex.getValue();
                while (messageIterator.hasNext()) {
                    score += messageIterator.next();
                }
                calc(score);
            }
        }

        private void calc(double score) {
            if (score > convergence) {
                this.context.setNewVertexValue(score * (1 - alpha));
                List<IEdge<Integer, Integer>> outEdges = this.context.edges().getOutEdges();
                if (outEdges.size() > 0) {
                    this.context.sendMessageToNeighbors((score * alpha) / outEdges.size());
                }
            }
        }
    }

}
