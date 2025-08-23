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
import org.apache.geaflow.api.graph.PGraphWindow;
import org.apache.geaflow.api.graph.function.vc.VertexCentricCombineFunction;
import org.apache.geaflow.api.graph.function.vc.VertexCentricTraversalFunction;
import org.apache.geaflow.api.graph.traversal.VertexCentricTraversal;
import org.apache.geaflow.api.pdata.stream.window.PWindowSource;
import org.apache.geaflow.api.window.impl.AllWindow;
import org.apache.geaflow.env.Environment;
import org.apache.geaflow.example.function.FileSource;
import org.apache.geaflow.example.util.EnvironmentUtil;
import org.apache.geaflow.example.util.PipelineResultCollect;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StaticGraphTraversalExample {

    private static final Logger LOGGER = LoggerFactory.getLogger(StaticGraphTraversalExample.class);

    public static void main(String[] args) {
        Environment environment = EnvironmentUtil.loadEnvironment(args);
        IPipelineResult result = submit(environment);
        PipelineResultCollect.get(result);
        environment.shutdown();
    }

    public static IPipelineResult submit(Environment environment) {
        Pipeline pipeline = PipelineFactory.buildPipeline(environment);

        pipeline.submit(new PipelineTask() {
            @Override
            public void execute(IPipelineTaskContext pipelineTaskCxt) {
                PWindowSource<IVertex<Integer, Integer>> prVertices =
                    pipelineTaskCxt.buildSource(new FileSource<>("data/input/email_vertex",
                        line -> {
                            String[] fields = line.split(",");
                            IVertex<Integer, Integer> vertex = new ValueVertex<>(Integer.valueOf(fields[0]), Integer.valueOf(fields[1]));
                            return Collections.singletonList(vertex);
                        }), AllWindow.getInstance()).withParallelism(1);

                PWindowSource<IEdge<Integer, Integer>> prEdges =
                    pipelineTaskCxt.buildSource(new FileSource<>("data/input/email_edge",
                        line -> {
                            String[] fields = line.split(",");
                            IEdge<Integer, Integer> edge = new ValueEdge<>(Integer.valueOf(fields[0]), Integer.valueOf(fields[1]), 1);
                            return Collections.singletonList(edge);
                        }), AllWindow.getInstance()).withParallelism(1);

                int iterationParallelism = 2;
                GraphViewDesc graphViewDesc = GraphViewBuilder
                    .createGraphView(GraphViewBuilder.DEFAULT_GRAPH)
                    .withShardNum(iterationParallelism)
                    .withBackend(BackendType.Memory)
                    .build();
                PGraphWindow<Integer, Integer, Integer> graphWindow =
                    pipelineTaskCxt.buildWindowStreamGraph(prVertices, prEdges, graphViewDesc);

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
                                for (IEdge<Integer, Integer> edge : outEdges) {
                                    LOGGER.info("out edge:{}", edge);
                                    this.vertexCentricFuncContext.takeResponse(new TraversalResponse(traversalRequest.getRequestId(), edge.getTargetId()));
                                }
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

                }).start(300).withParallelism(iterationParallelism).sink(x -> LOGGER.info("x:{}", x));

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
    }

}
