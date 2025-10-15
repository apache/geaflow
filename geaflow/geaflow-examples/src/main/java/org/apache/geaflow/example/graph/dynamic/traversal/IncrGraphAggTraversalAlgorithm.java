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

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.geaflow.api.graph.function.vc.IncVertexCentricAggTraversalFunction;
import org.apache.geaflow.api.graph.function.vc.VertexCentricAggregateFunction;
import org.apache.geaflow.api.graph.function.vc.VertexCentricCombineFunction;
import org.apache.geaflow.api.graph.traversal.IncVertexCentricAggTraversal;
import org.apache.geaflow.common.tuple.Tuple;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.model.traversal.ITraversalRequest;
import org.apache.geaflow.model.traversal.ITraversalResponse;
import org.apache.geaflow.model.traversal.TraversalType.ResponseType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IncrGraphAggTraversalAlgorithm
    extends IncVertexCentricAggTraversal<Integer, Integer,
    Integer, Integer, Integer, Integer, Tuple<Integer, Integer>, Tuple<Integer, Integer>,
    Tuple<Integer, Integer>, Integer> {

    private static final Logger LOGGER = LoggerFactory.getLogger(IncrGraphAggTraversalAlgorithm.class);

    public IncrGraphAggTraversalAlgorithm(long iterations) {
        super(iterations);
    }

    @Override
    public IncVertexCentricAggTraversalFunction<Integer, Integer, Integer, Integer, Integer, Integer, Integer> getIncTraversalFunction() {

        return new IncVertexCentricAggTraversalFunction<Integer, Integer, Integer, Integer, Integer, Integer, Integer>() {

            private IncVertexCentricTraversalFuncContext<Integer, Integer, Integer, Integer, Integer> vertexCentricFuncContext;
            private VertexCentricAggContext<Integer, Integer> aggContext;
            private Map<Integer, Integer> vertexValue;

            @Override
            public void initContext(VertexCentricAggContext<Integer, Integer> aggContext) {
                this.aggContext = aggContext;
            }

            @Override
            public void open(IncVertexCentricTraversalFuncContext<Integer, Integer,
                Integer, Integer, Integer> vertexCentricFuncContext) {
                this.vertexCentricFuncContext = vertexCentricFuncContext;
                this.vertexValue = new HashMap<>();
            }

            @Override
            public void evolve(Integer vertexId,
                               TemporaryGraph<Integer, Integer, Integer> temporaryGraph) {
                MutableGraph<Integer, Integer, Integer> mutableGraph =
                    this.vertexCentricFuncContext.getMutableGraph();
                IVertex<Integer, Integer> vertex = temporaryGraph.getVertex();
                if (vertex != null) {
                    vertex.withValue(0);
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
                List<IEdge<Integer, Integer>> edges =
                    this.vertexCentricFuncContext.getHistoricalGraph().getSnapShot(0).edges().getEdges();
                if (edges != null) {
                    for (IEdge<Integer, Integer> edge : edges) {
                        this.vertexCentricFuncContext.sendMessage(edge.getTargetId(), edges.size());
                    }
                    aggContext.aggregate(edges.size());
                    vertexValue.put(traversalRequest.getVId(), 0);
                }
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

                int sum = 0;
                while (messageIterator.hasNext()) {
                    sum += messageIterator.next();
                }
                List<IEdge<Integer, Integer>> edges =
                    this.vertexCentricFuncContext.getHistoricalGraph().getSnapShot(0).edges().getEdges();

                if (edges == null || edges.isEmpty()) {
                    aggContext.aggregate(0);
                    return;
                }

                int average = sum / edges.size();
                IVertex<Integer, Integer> vertex = this.vertexCentricFuncContext.getTemporaryGraph().getVertex();

                if (vertex != null) {
                    for (IEdge<Integer, Integer> edge : edges) {
                        this.vertexCentricFuncContext.sendMessage(edge.getTargetId(), average);
                    }
                    aggContext.aggregate(edges.size());
                    vertexValue.put(vertexId, vertex.getValue());
                } else {
                    aggContext.aggregate(0);
                    vertexValue.put(vertexId, 0);
                }
            }

            @Override
            public void finish(Integer vertexId,
                               MutableGraph<Integer, Integer, Integer> mutableGraph) {
                this.vertexCentricFuncContext.takeResponse(new TraversalResponse(vertexId,
                    Math.toIntExact(vertexCentricFuncContext.getIterationId())));
            }
        };
    }

    @Override
    public VertexCentricAggregateFunction<Integer, Tuple<Integer, Integer>, Tuple<Integer, Integer>, Tuple<Integer, Integer>, Integer> getAggregateFunction() {
        return new VertexCentricAggregateFunction<Integer, Tuple<Integer, Integer>,
            Tuple<Integer, Integer>, Tuple<Integer, Integer>, Integer>() {
            @Override
            public IPartialGraphAggFunction<Integer, Tuple<Integer, Integer>, Tuple<Integer, Integer>> getPartialAggregation() {
                return new IPartialGraphAggFunction<Integer, Tuple<Integer, Integer>, Tuple<Integer, Integer>>() {

                    private IPartialAggContext<Tuple<Integer, Integer>> partialAggContext;

                    @Override
                    public Tuple<Integer, Integer> create(
                        IPartialAggContext<Tuple<Integer, Integer>> partialAggContext) {
                        this.partialAggContext = partialAggContext;
                        return Tuple.of(0, 0);
                    }


                    @Override
                    public Tuple<Integer, Integer> aggregate(Integer integer, Tuple<Integer, Integer> result) {
                        result.f0 += 1;
                        result.f1 += integer;
                        return result;
                    }

                    @Override
                    public void finish(Tuple<Integer, Integer> result) {
                        partialAggContext.collect(result);
                    }
                };
            }

            @Override
            public IGraphAggregateFunction<Tuple<Integer, Integer>, Tuple<Integer, Integer>,
                Integer> getGlobalAggregation() {
                return new IGraphAggregateFunction<Tuple<Integer, Integer>, Tuple<Integer, Integer>, Integer>() {

                    private IGlobalGraphAggContext<Integer> globalGraphAggContext;

                    @Override
                    public Tuple<Integer, Integer> create(
                        IGlobalGraphAggContext<Integer> globalGraphAggContext) {
                        this.globalGraphAggContext = globalGraphAggContext;
                        return Tuple.of(0, 0);
                    }

                    @Override
                    public Integer aggregate(Tuple<Integer, Integer> integerIntegerTuple2,
                                             Tuple<Integer, Integer> integerIntegerTuple22) {
                        return (integerIntegerTuple22.f1 + integerIntegerTuple2.f1) / 1005;
                    }

                    @Override
                    public void finish(Integer value) {
                        long iterationId = this.globalGraphAggContext.getIteration();
                        if (iterationId == 2) {
                            LOGGER.info("current iterationId:{} value is {}, do terminate", iterationId, value);
                            this.globalGraphAggContext.terminate();
                        } else {
                            LOGGER.info("current iterationId:{} value is {}, do broadcast", iterationId, value);
                            // Set a dummy value, without any global result
                            this.globalGraphAggContext.broadcast(0);
                        }
                    }
                };
            }
        };
    }

    @Override
    public VertexCentricCombineFunction<Integer> getCombineFunction() {
        return null;
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
    }
}
