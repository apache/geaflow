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

package org.apache.geaflow.example.graph.statical.compute.khop;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;
import org.apache.geaflow.api.function.io.SinkFunction;
import org.apache.geaflow.api.graph.compute.VertexCentricCompute;
import org.apache.geaflow.api.graph.function.vc.VertexCentricCombineFunction;
import org.apache.geaflow.api.graph.function.vc.VertexCentricComputeFunction;
import org.apache.geaflow.api.pdata.stream.window.PWindowSource;
import org.apache.geaflow.api.pdata.stream.window.PWindowStream;
import org.apache.geaflow.api.window.impl.AllWindow;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.env.Environment;
import org.apache.geaflow.example.config.ExampleConfigKeys;
import org.apache.geaflow.example.function.AbstractVcFunc;
import org.apache.geaflow.example.function.FileSink;
import org.apache.geaflow.example.function.FileSource;
import org.apache.geaflow.example.util.EnvironmentUtil;
import org.apache.geaflow.example.util.ExampleSinkFunctionFactory;
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

public class KHop {

    private static final Logger LOGGER = LoggerFactory.getLogger(KHop.class);

    public static final String RESULT_FILE_PATH = "./target/tmp/data/result/KHop";
    public static final String REF_FILE_PATH = "data/reference/KHop";

    private static int k = 2;
    private static Object srcId = 990;

    public KHop(Object inputId, int inputK) {
        srcId = inputId;
        k = inputK;
    }

    public static void main(String[] args) {
        Environment environment = EnvironmentUtil.loadEnvironment(args);
        submit(environment);
    }

    public static IPipelineResult submit(Environment environment) {
        Pipeline pipeline = PipelineFactory.buildPipeline(environment);
        Configuration envConfig = environment.getEnvironmentContext().getConfig();
        envConfig.put(FileSink.OUTPUT_DIR, RESULT_FILE_PATH);
        ResultValidator.cleanResult(RESULT_FILE_PATH);

        pipeline.submit((PipelineTask) pipelineTaskCxt -> {
            Configuration conf = pipelineTaskCxt.getConfig();
            int sinkParallelism = conf.getInteger(ExampleConfigKeys.SINK_PARALLELISM);
            PWindowSource<IVertex<Object, Integer>> vertices =
                pipelineTaskCxt.buildSource(new FileSource<>("data/input/email_vertex",
                    line -> {
                        String[] fields = line.split(",");
                        IVertex<Object, Integer> vertex = new ValueVertex<>(
                            fields[0], Integer.valueOf(fields[1]));
                        return Collections.singletonList(vertex);
                    }), AllWindow.getInstance())
                    .withParallelism(conf.getInteger(ExampleConfigKeys.SOURCE_PARALLELISM));

            PWindowSource<IEdge<Object, Object>> edges = pipelineTaskCxt.buildSource(new FileSource<>("data/input/email_edge",
                line -> {
                    String[] fields = line.split(",");
                    IEdge<Object, Object> edge = new ValueEdge<>(fields[0], fields[1], 1);
                    return Collections.singletonList(edge);
                }), AllWindow.getInstance())
                .withParallelism(conf.getInteger(ExampleConfigKeys.SOURCE_PARALLELISM));

            int iterationParallelism = conf.getInteger(ExampleConfigKeys.ITERATOR_PARALLELISM);
            GraphViewDesc graphViewDesc = GraphViewBuilder
                .createGraphView(GraphViewBuilder.DEFAULT_GRAPH)
                .withShardNum(2)
                .withBackend(BackendType.Memory)
                .build();

            PWindowStream<IVertex<Object, Integer>> result =
                pipelineTaskCxt.buildWindowStreamGraph(vertices, edges, graphViewDesc)
                    .compute(new KHAlgorithms(k + 1))
                    .compute(iterationParallelism)
                    .getVertices();

            SinkFunction<String> sink = ExampleSinkFunctionFactory.getSinkFunction(conf);
            result.filter(v -> v.getValue() < k + 1).map(v -> String.format("%s,%s", v.getId(), v.getValue()))
                .sink(sink).withParallelism(sinkParallelism);
        });

        return pipeline.execute();
    }

    public static void validateResult() throws IOException {
        ResultValidator.validateResult(REF_FILE_PATH, RESULT_FILE_PATH);
    }

    public static class KHAlgorithms extends VertexCentricCompute<Object, Integer, Object, Integer> {

        public KHAlgorithms(long iterations) {
            super(iterations);
        }

        @Override
        public VertexCentricComputeFunction<Object, Integer, Object, Integer> getComputeFunction() {
            return new KHVertexCentricComputeFunction();
        }

        @Override
        public VertexCentricCombineFunction<Integer> getCombineFunction() {
            return null;
        }

    }

    public static class KHVertexCentricComputeFunction extends AbstractVcFunc<Object, Integer, Object, Integer> {

        @Override
        public void compute(Object vertexId,
                            Iterator<Integer> messageIterator) {
            IVertex<Object, Integer> vertex = this.context.vertex().get();
            if (this.context.getIterationId() == 1L) {
                if (Objects.equals(vertex.getId(), srcId)) {
                    this.context.sendMessageToNeighbors(1);
                    this.context.setNewVertexValue(0);
                } else {
                    this.context.setNewVertexValue(Integer.MAX_VALUE);
                }
            } else {
                if (vertex.getValue() == Integer.MAX_VALUE && messageIterator.hasNext()) {
                    int value = messageIterator.next();
                    this.context.sendMessageToNeighbors(value + 1);
                    this.context.setNewVertexValue(value);
                }
            }
        }
    }
}
