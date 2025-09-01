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

package org.apache.geaflow.example.graph.statical.traversal;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.geaflow.api.function.io.SinkFunction;
import org.apache.geaflow.api.graph.PGraphWindow;
import org.apache.geaflow.api.graph.function.vc.VertexCentricCombineFunction;
import org.apache.geaflow.api.graph.function.vc.VertexCentricTraversalFunction;
import org.apache.geaflow.api.graph.traversal.VertexCentricTraversal;
import org.apache.geaflow.api.pdata.stream.window.PWindowSource;
import org.apache.geaflow.api.window.impl.AllWindow;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.env.Environment;
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

public class StaticGraphTraversalAllExample {

    public static final String REFERENCE_FILE_PATH = "data/reference/static_graph_traversal_all";

    public static final String RESULT_FILE_PATH = "./target/tmp/data/result"
        + "/static_graph_traversal_all";

    public static void main(String[] args) {
        Environment environment = EnvironmentUtil.loadEnvironment(args);
        IPipelineResult result = submit(environment);
        PipelineResultCollect.get(result);
        environment.shutdown();
    }

    public static IPipelineResult submit(Environment environment) {

        Pipeline pipeline = PipelineFactory.buildPipeline(environment);
        Configuration envConfig = environment.getEnvironmentContext().getConfig();
        envConfig.getConfigMap().put(FileSink.OUTPUT_DIR, RESULT_FILE_PATH);
        ResultValidator.cleanResult(RESULT_FILE_PATH);

        pipeline.submit(new PipelineTask() {
            @Override
            public void execute(IPipelineTaskContext pipelineTaskCxt) {
                PWindowSource<IVertex<Integer, Integer>> prVertices =
                    pipelineTaskCxt.buildSource(new FileSource<>("data/input/email_vertex",
                        line -> {
                            String[] fields = line.split(",");
                            IVertex<Integer, Integer> vertex = new ValueVertex<>(Integer.valueOf(fields[0]),
                                Integer.valueOf(fields[1]));
                            return Collections.singletonList(vertex);
                        }), AllWindow.getInstance()).withParallelism(1);

                PWindowSource<IEdge<Integer, Integer>> prEdges =
                    pipelineTaskCxt.buildSource(new FileSource<>("data/input/email_edge",
                        line -> {
                            String[] fields = line.split(",");
                            IEdge<Integer, Integer> edge = new ValueEdge<>(Integer.valueOf(fields[0]),
                                Integer.valueOf(fields[1]), 1);
                            return Collections.singletonList(edge);
                        }), AllWindow.getInstance()).withParallelism(1);

                GraphViewDesc graphViewDesc = GraphViewBuilder
                    .createGraphView(GraphViewBuilder.DEFAULT_GRAPH)
                    .withShardNum(1)
                    .withBackend(BackendType.Memory)
                    .build();
                PGraphWindow<Integer, Integer, Integer> graphWindow =
                    pipelineTaskCxt.buildWindowStreamGraph(prVertices, prEdges, graphViewDesc);

                SinkFunction<String> sink = ExampleSinkFunctionFactory.getSinkFunction(
                    pipelineTaskCxt.getConfig());

                graphWindow.traversal(new VertexCentricTraversal<Integer, Integer, Integer, Integer, Integer>(3) {
                    @Override
                    public VertexCentricTraversalFunction<Integer, Integer, Integer, Integer,
                        Integer> getTraversalFunction() {
                        return new VertexCentricTraversalFunction<Integer, Integer, Integer,
                            Integer, Integer>() {

                            private VertexCentricTraversalFuncContext<Integer, Integer, Integer,
                                Integer, Integer> vertexCentricFuncContext;

                            @Override
                            public void open(
                                VertexCentricTraversalFuncContext<Integer, Integer, Integer,
                                    Integer, Integer> vertexCentricFuncContext) {
                                this.vertexCentricFuncContext = vertexCentricFuncContext;
                            }

                            @Override
                            public void init(ITraversalRequest<Integer> traversalRequest) {
                                List<IEdge<Integer, Integer>> outEdges =
                                    this.vertexCentricFuncContext.edges().getOutEdges();
                                int sum = outEdges.size();
                                this.vertexCentricFuncContext.takeResponse(
                                    new TraversalResponse(traversalRequest.getRequestId(), sum));
                            }

                            @Override
                            public void compute(Integer vertexId,
                                                Iterator<Integer> messageIterator) {

                            }

                            @Override
                            public void finish() {

                            }

                            @Override
                            public void close() {

                            }
                        };
                    }

                    @Override
                    public VertexCentricCombineFunction<Integer> getCombineFunction() {
                        return null;
                    }

                }).start().withParallelism(1).map(x -> x.toString()).sink(sink);

            }
        });

        return pipeline.execute();
    }

    public static class TraversalResponse implements ITraversalResponse<Integer> {

        private long responseId;
        private int response;

        public TraversalResponse(long responseId, int response) {
            this.responseId = responseId;
            this.response = response;
        }

        @Override
        public long getResponseId() {
            return responseId;
        }

        @Override
        public Integer getResponse() {
            return response;
        }

        @Override
        public ResponseType getType() {
            return ResponseType.Vertex;
        }

        @Override
        public String toString() {
            return "TraversalResponse{" + "responseId=" + responseId + ", response=" + response
                + '}';
        }
    }

    public static void validateResult() throws IOException {
        ResultValidator.validateMapResult(REFERENCE_FILE_PATH, RESULT_FILE_PATH, String::compareTo);
    }

}
