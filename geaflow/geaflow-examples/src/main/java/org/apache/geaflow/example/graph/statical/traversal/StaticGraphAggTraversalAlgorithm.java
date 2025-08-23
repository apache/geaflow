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

import java.util.Iterator;
import org.apache.geaflow.api.graph.function.vc.VertexCentricAggTraversalFunction;
import org.apache.geaflow.api.graph.function.vc.VertexCentricAggregateFunction;
import org.apache.geaflow.api.graph.function.vc.VertexCentricCombineFunction;
import org.apache.geaflow.api.graph.traversal.VertexCentricAggTraversal;
import org.apache.geaflow.common.tuple.Tuple;
import org.apache.geaflow.model.traversal.ITraversalRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StaticGraphAggTraversalAlgorithm extends VertexCentricAggTraversal<Integer, Double,
    Integer, Integer, Integer, Integer, Tuple<Integer, Integer>, Tuple<Integer, Integer>,
    Tuple<Integer, Integer>, Integer> {

    private static final Logger LOGGER = LoggerFactory.getLogger(StaticGraphAggTraversalAlgorithm.class);


    public StaticGraphAggTraversalAlgorithm(long iterations) {
        super(iterations);
    }

    @Override
    public VertexCentricAggTraversalFunction<Integer, Double, Integer, Integer, Integer, Integer, Integer> getTraversalFunction() {
        return new StaticGraphAggTraversalAlgorithm.TraversalFunction();
    }

    @Override
    public VertexCentricAggregateFunction<Integer, Tuple<Integer, Integer>, Tuple<Integer,
        Integer>, Tuple<Integer,
        Integer>, Integer> getAggregateFunction() {
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
                        integerIntegerTuple22.f0 += integerIntegerTuple2.f0;
                        integerIntegerTuple22.f1 += integerIntegerTuple2.f1;
                        return (int) (integerIntegerTuple22.f1 / integerIntegerTuple22.f0);
                    }

                    @Override
                    public void finish(Integer value) {
                        long iterationId = this.globalGraphAggContext.getIteration();
                        if (value > 0) {
                            LOGGER.info("current iterationId:{} value is {}, do terminate", iterationId, value);
                            this.globalGraphAggContext.terminate();
                        } else {
                            LOGGER.info("current iterationId:{} value is {}, do broadcast", iterationId, value);
                            this.globalGraphAggContext.broadcast(value);
                        }
                    }
                };
            }
        };
    }

    @Override
    public VertexCentricCombineFunction<Integer> getCombineFunction() {
        return new VertexCentricCombineFunction<Integer>() {
            @Override
            public Integer combine(Integer oldMessage, Integer newMessage) {
                return oldMessage + newMessage;
            }
        };
    }

    public class TraversalFunction implements
        VertexCentricAggTraversalFunction<Integer, Double, Integer, Integer, Integer, Integer, Integer> {

        private VertexCentricTraversalFuncContext<Integer, Double, Integer, Integer, Integer> vertexCentricFuncContext;
        private VertexCentricAggContext<Integer, Integer> aggContext;

        @Override
        public void initContext(VertexCentricAggContext<Integer, Integer> aggContext) {
            this.aggContext = aggContext;
        }

        @Override
        public void open(VertexCentricTraversalFuncContext<Integer, Double, Integer, Integer, Integer> vertexCentricFuncContext) {
            this.vertexCentricFuncContext = vertexCentricFuncContext;
        }

        @Override
        public void init(ITraversalRequest<Integer> traversalRequest) {

            int degreeSize = vertexCentricFuncContext.edges().getOutEdges().size();
            aggContext.aggregate(0);
            vertexCentricFuncContext.sendMessageToNeighbors(degreeSize);
        }

        @Override
        public void compute(Integer vertexId, Iterator<Integer> messageIterator) {

            int sum = 0;
            while (messageIterator.hasNext()) {
                sum += messageIterator.next();
            }
            aggContext.aggregate(sum);
            this.vertexCentricFuncContext.takeResponse(new TraversalResponseExample(vertexId, sum));
            LOGGER.info("vertexId {} aggregate {}", vertexId, sum);
            vertexCentricFuncContext.sendMessageToNeighbors(sum);
        }

        @Override
        public void finish() {

        }

        @Override
        public void close() {

        }
    }
}