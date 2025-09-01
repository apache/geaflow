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

package org.apache.geaflow.example.graph.statical.compute.commonneighbors;

import java.io.IOException;
import java.util.Iterator;
import org.apache.geaflow.api.function.io.SinkFunction;
import org.apache.geaflow.api.graph.compute.VertexCentricCompute;
import org.apache.geaflow.api.graph.function.vc.VertexCentricCombineFunction;
import org.apache.geaflow.api.graph.function.vc.VertexCentricComputeFunction;
import org.apache.geaflow.api.pdata.stream.window.PWindowStream;
import org.apache.geaflow.api.window.impl.AllWindow;
import org.apache.geaflow.common.config.Configuration;
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

public class CommonNeighbors {

    private static final Logger LOGGER = LoggerFactory.getLogger(CommonNeighbors.class);

    public static final String REFERENCE_FILE_PATH = "data/reference/commonneighbor";

    public static final String RESULT_FILE_DIR = "./target/tmp/data/result/commonneighbor";

    public static void main(String[] args) {
        Environment environment = EnvironmentUtil.loadEnvironment(args);
        IPipelineResult<?> result = CommonNeighbors.submit(environment);
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

            FileSource<IVertex<Integer, Integer>> vSource = new FileSource<>(
                GraphDataSet.DATASET_FILE, VertexEdgeParser::vertexParserInteger);
            PWindowStream<IVertex<Integer, Integer>> vertices =
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
            PWindowStream<IVertex<Integer, Integer>> result =
                pipelineTaskCxt.buildWindowStreamGraph(vertices, edges, graphViewDesc)
                    .compute(new CommonNeighborsAlgorithm(398191, 722800, 50))
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


    public static class CommonNeighborsAlgorithm extends VertexCentricCompute<Integer, Integer, Integer, Integer> {

        private final int id1;
        private final int id2;

        public CommonNeighborsAlgorithm(int id1, int id2, long iterations) {
            super(iterations);
            this.id1 = id1;
            this.id2 = id2;
        }

        @Override
        public VertexCentricComputeFunction<Integer, Integer, Integer, Integer> getComputeFunction() {
            return new CommonNeighborsVCCFunction(this.id1, this.id2);
        }

        @Override
        public VertexCentricCombineFunction<Integer> getCombineFunction() {
            return null;
        }

    }

    public static class CommonNeighborsVCCFunction extends AbstractVcFunc<Integer, Integer, Integer, Integer> {

        private final int vertexId1;
        private final int vertexId2;

        public CommonNeighborsVCCFunction(int vertexId1, int vertexId2) {
            this.vertexId1 = vertexId1;
            this.vertexId2 = vertexId2;
        }

        @Override
        public void compute(Integer vertexId, Iterator<Integer> messageIterator) {
            IVertex<Integer, Integer> vertex = this.context.vertex().get();
            if (this.context.getIterationId() == 1) {
                this.context.setNewVertexValue(0);
                if (vertex.getId().equals(this.vertexId1) || vertex.getId().equals(this.vertexId2)) {
                    sendMessage(vertex.getId());
                }
            } else {
                int meg = messageIterator.next();
                while (messageIterator.hasNext()) {
                    if (meg != messageIterator.next()) {
                        this.context.setNewVertexValue(1);
                        break;
                    }
                }
            }
        }

        private void sendMessage(int meg) {
            for (IEdge<Integer, Integer> edge : this.context.edges().getInEdges()) {
                this.context.sendMessage(edge.getTargetId(), meg);
            }
            for (IEdge<Integer, Integer> edge : this.context.edges().getOutEdges()) {
                this.context.sendMessage(edge.getTargetId(), meg);
            }
        }

    }

}
