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

package org.apache.geaflow.example.graph.statical.compute.npaths;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
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
import org.apache.geaflow.model.graph.edge.EdgeDirection;
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

public class NPaths {

    private static final Logger LOGGER = LoggerFactory.getLogger(NPaths.class);

    private static final int SOURCE_ID = 11342;

    private static final int TARGET_ID = 748615;

    public static final String REFERENCE_FILE_PATH = "data/reference/npath";

    public static final String RESULT_FILE_DIR = "./target/tmp/data/result/npath";

    public static void main(String[] args) {
        Environment environment = EnvironmentUtil.loadEnvironment(args);
        IPipelineResult result = NPaths.submit(environment);
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

            FileSource<IVertex<Integer, Map<String, String>>> vSource = new FileSource<>(
                GraphDataSet.DATASET_FILE, VertexEdgeParser::vertexParserMap);
            PWindowStream<IVertex<Integer, Map<String, String>>> vertices =
                pipelineTaskCxt.buildSource(vSource, AllWindow.getInstance()).withParallelism(sourceParallelism);

            FileSource<IEdge<Integer, Map<String, Integer>>> eSource = new FileSource<>(
                GraphDataSet.DATASET_FILE, VertexEdgeParser::edgeParserMap);
            PWindowStream<IEdge<Integer, Map<String, Integer>>> edges = pipelineTaskCxt.buildSource(eSource, AllWindow.getInstance()).withParallelism(sourceParallelism);

            GraphViewDesc graphViewDesc = GraphViewBuilder
                .createGraphView(GraphViewBuilder.DEFAULT_GRAPH)
                .withShardNum(iterationParallelism)
                .withBackend(BackendType.Memory)
                .build();
            PWindowStream<IVertex<Integer, Map<String, String>>> result =
                pipelineTaskCxt.buildWindowStreamGraph(vertices, edges, graphViewDesc)
                    .compute(new NPathsAlgorithms(SOURCE_ID, TARGET_ID, 10))
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

    public static class NPathsAlgorithms extends VertexCentricCompute<Integer, Map<String, String>, Map<String, Integer>, List<String>> {

        private final int sourceId;

        private final int targetId;

        public NPathsAlgorithms(int sourceId, int targetId, long iterations) {
            super(iterations);
            this.sourceId = sourceId;
            this.targetId = targetId;
        }

        @Override
        public VertexCentricComputeFunction<Integer, Map<String, String>, Map<String, Integer>, List<String>> getComputeFunction() {
            return new NPathsVCFunction(sourceId, targetId, EdgeDirection.OUT);
        }

        @Override
        public VertexCentricCombineFunction<List<String>> getCombineFunction() {
            return null;
        }

    }

    public static class NPathsVCFunction extends AbstractVcFunc<Integer, Map<String, String>, Map<String, Integer>, List<String>> {

        private static final String KEY_FIELD = "dis";

        private final int sourceId;

        private final int targetId;

        private final EdgeDirection edgeType;

        private final AtomicInteger pathNum = new AtomicInteger(10);


        public NPathsVCFunction(int sourceId, int targetId, EdgeDirection edgeType) {
            this.edgeType = edgeType;
            this.sourceId = sourceId;
            this.targetId = targetId;
        }

        @Override
        public void compute(Integer vertexId, Iterator<List<String>> messageIterator) {
            IVertex<Integer, Map<String, String>> vertex = this.context.vertex().get();
            Map<String, String> property = vertex.getValue();
            if (this.context.getIterationId() == 1) {
                property.put(KEY_FIELD, "");
                this.context.setNewVertexValue(property);
                if (vertex.getId().equals(sourceId)) {
                    sendMessage(Collections.singletonList(String.valueOf(sourceId)));
                }
            } else {
                if (vertex.getId().equals(targetId)) {
                    StringBuilder builder = new StringBuilder();
                    builder.append(property.get(KEY_FIELD));
                    while (messageIterator.hasNext() && pathNum.get() > 0) {
                        List<String> tmp = new LinkedList<>(messageIterator.next());
                        tmp.add(String.valueOf(vertex.getId()));
                        String sep = ";";
                        builder.append(tmp).append(sep);
                        pathNum.getAndDecrement();
                    }
                    property.put(KEY_FIELD, builder.toString());
                    this.context.setNewVertexValue(property);
                } else {
                    while (messageIterator.hasNext()) {
                        List<String> tmp = new LinkedList<>(messageIterator.next());
                        tmp.add(String.valueOf(vertex.getId()));
                        sendMessage(tmp);
                    }
                }
            }
        }

        private void sendMessage(List<String> msg) {
            switch (this.edgeType) {
                case OUT:
                    for (IEdge<Integer, Map<String, Integer>> edge : this.context.edges()
                        .getOutEdges()) {
                        this.context.sendMessage(edge.getTargetId(), msg);
                    }
                    break;
                case IN:
                    for (IEdge<Integer, Map<String, Integer>> edge : this.context.edges()
                        .getInEdges()) {
                        this.context.sendMessage(edge.getTargetId(), msg);
                    }
                    break;
                case BOTH:
                    for (IEdge<Integer, Map<String, Integer>> edge : this.context.edges().getEdges()) {
                        this.context.sendMessage(edge.getTargetId(), msg);
                    }
                    break;
                default:
                    break;
            }
        }
    }

}
