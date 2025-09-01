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

package org.apache.geaflow.example.graph.statical.compute.closenesscentrality;

import java.io.IOException;
import java.util.Iterator;
import org.apache.geaflow.api.function.io.SinkFunction;
import org.apache.geaflow.api.graph.compute.VertexCentricCompute;
import org.apache.geaflow.api.graph.function.vc.VertexCentricCombineFunction;
import org.apache.geaflow.api.graph.function.vc.VertexCentricComputeFunction;
import org.apache.geaflow.api.pdata.stream.window.PWindowStream;
import org.apache.geaflow.api.window.impl.AllWindow;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.tuple.Tuple;
import org.apache.geaflow.env.Environment;
import org.apache.geaflow.example.config.ExampleConfigKeys;
import org.apache.geaflow.example.data.GraphDataSet;
import org.apache.geaflow.example.function.AbstractVcFunc;
import org.apache.geaflow.example.function.FileSink;
import org.apache.geaflow.example.function.FileSource;
import org.apache.geaflow.example.util.EnvironmentUtil;
import org.apache.geaflow.example.util.ExampleSinkFunctionFactory;
import org.apache.geaflow.example.util.PipelineResultCollect;
import org.apache.geaflow.example.util.ResultValidator;
import org.apache.geaflow.example.util.VertexEdgeParser;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.pipeline.IPipelineResult;
import org.apache.geaflow.pipeline.Pipeline;
import org.apache.geaflow.pipeline.PipelineFactory;
import org.apache.geaflow.pipeline.task.PipelineTask;
import org.apache.geaflow.view.GraphViewBuilder;
import org.apache.geaflow.view.IViewDesc.BackendType;
import org.apache.geaflow.view.graph.GraphViewDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClosenessCentrality {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClosenessCentrality.class);
    public static final int SOURCE_ID = 11342;

    public static final String REFERENCE_FILE_PATH = "data/reference/closeness";

    public static final String RESULT_FILE_DIR = "./target/tmp/data/result/closeness";

    public static void main(String[] args) {
        Environment environment = EnvironmentUtil.loadEnvironment(args);
        IPipelineResult<?> result = ClosenessCentrality.submit(environment);
        PipelineResultCollect.get(result);
        environment.shutdown();
    }

    public static IPipelineResult<?> submit(Environment environment) {
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

            FileSource<IVertex<Integer, Tuple<Double, Integer>>> vSource = new FileSource<>(
                GraphDataSet.DATASET_FILE, VertexEdgeParser::vertexParserTuple);
            PWindowStream<IVertex<Integer, Tuple<Double, Integer>>> vertices =
                pipelineTaskCxt.buildSource(vSource, AllWindow.getInstance()).withParallelism(sourceParallelism);

            FileSource<IEdge<Integer, Integer>> eSource = new FileSource<>(
                GraphDataSet.DATASET_FILE, VertexEdgeParser::edgeParserInteger);
            PWindowStream<IEdge<Integer, Integer>> edges =
                pipelineTaskCxt.buildSource(eSource, AllWindow.getInstance()).withParallelism(sourceParallelism);

            GraphViewDesc graphViewDesc = GraphViewBuilder
                .createGraphView(GraphViewBuilder.DEFAULT_GRAPH)
                .withShardNum(iterationParallelism)
                .withBackend(BackendType.Memory)
                .build();
            PWindowStream<IVertex<Integer, Tuple<Double, Integer>>> result =
                pipelineTaskCxt.buildWindowStreamGraph(vertices, edges, graphViewDesc)
                    .compute(new ClosenessCentralityAlgorithm(SOURCE_ID, 10))
                    .compute(iterationParallelism)
                    .getVertices();
            SinkFunction<String> sink = ExampleSinkFunctionFactory.getSinkFunction(config);
            result.map(v -> String.format("%s,%s", v.getId(), v.getValue()))
                .sink(sink).withParallelism(sinkParallelism);
        });

        return pipeline.execute();
    }

    public static void validateResult() throws IOException {
        ResultValidator.validateMapResult(REFERENCE_FILE_PATH, RESULT_FILE_DIR);
    }

    public static class ClosenessCentralityAlgorithm extends VertexCentricCompute<Integer, Tuple<Double, Integer>, Integer, Integer> {

        private final int srcId;

        public ClosenessCentralityAlgorithm(int srcId, long iterations) {
            super(iterations);
            this.srcId = srcId;
        }

        @Override
        public VertexCentricComputeFunction<Integer, Tuple<Double, Integer>, Integer, Integer> getComputeFunction() {
            return new ClosenessCentralityVCCFunction(this.srcId);
        }

        @Override
        public VertexCentricCombineFunction<Integer> getCombineFunction() {
            return null;
        }

    }

    public static class ClosenessCentralityVCCFunction extends AbstractVcFunc<Integer, Tuple<Double, Integer>, Integer, Integer> {

        private final int srcId;

        public ClosenessCentralityVCCFunction(int srcId) {
            this.srcId = srcId;
        }

        @Override
        public void compute(Integer vertexId, Iterator<Integer> messageIterator) {
            IVertex<Integer, Tuple<Double, Integer>> vertex = this.context.vertex().get();
            if (this.context.getIterationId() == 1) {
                this.context.setNewVertexValue(Tuple.of(0.0, 0));
                if (vertex.getId().equals(this.srcId)) {
                    this.context.setNewVertexValue(Tuple.of(1.0, 0));
                    this.context.sendMessageToNeighbors(1);
                }
            } else {
                if (vertex.getId().equals(this.srcId)) {
                    int vertexNum = vertex.getValue().getF1();
                    double sum = vertexNum / vertex.getValue().getF0();
                    while (messageIterator.hasNext()) {
                        sum += messageIterator.next();
                        vertexNum++;
                    }
                    this.context.setNewVertexValue(Tuple.of(vertexNum / sum, vertexNum));
                } else {
                    if (vertex.getValue().getF1() < 1) {
                        Integer msg = messageIterator.next();
                        this.context.sendMessage(this.srcId, msg);
                        for (IEdge<Integer, Integer> edge : this.context.edges().getOutEdges()) {
                            if (!edge.getTargetId().equals(this.srcId)) {
                                this.context.sendMessage(edge.getTargetId(), msg + 1);
                            }
                        }
                        this.context.setNewVertexValue(Tuple.of(0.0, 1));
                    }
                }
            }
        }

    }

}
