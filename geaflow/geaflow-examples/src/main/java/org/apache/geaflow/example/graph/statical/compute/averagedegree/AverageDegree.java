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

package org.apache.geaflow.example.graph.statical.compute.averagedegree;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import org.apache.geaflow.api.function.io.SinkFunction;
import org.apache.geaflow.api.graph.PGraphWindow;
import org.apache.geaflow.api.graph.compute.VertexCentricAggCompute;
import org.apache.geaflow.api.graph.function.vc.VertexCentricAggComputeFunction;
import org.apache.geaflow.api.graph.function.vc.VertexCentricAggregateFunction;
import org.apache.geaflow.api.graph.function.vc.VertexCentricCombineFunction;
import org.apache.geaflow.api.pdata.stream.window.PWindowSource;
import org.apache.geaflow.api.window.impl.AllWindow;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.tuple.Tuple;
import org.apache.geaflow.env.Environment;
import org.apache.geaflow.example.config.ExampleConfigKeys;
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

public class AverageDegree {

    private static final Logger LOGGER = LoggerFactory.getLogger(AverageDegree.class);

    public static final String RESULT_FILE_PATH = "./target/tmp/data/result/average";
    public static final String REF_FILE_PATH = "data/reference/averagedegree";

    public static void main(String[] args) {
        Environment environment = EnvironmentUtil.loadEnvironment(args);
        IPipelineResult<?> result = AverageDegree.submit(environment);
        PipelineResultCollect.get(result);
        environment.shutdown();
    }

    public static IPipelineResult<?> submit(Environment environment) {
        Pipeline pipeline = PipelineFactory.buildPipeline(environment);
        Configuration envConfig = environment.getEnvironmentContext().getConfig();
        envConfig.put(FileSink.OUTPUT_DIR, RESULT_FILE_PATH);
        ResultValidator.cleanResult(RESULT_FILE_PATH);

        pipeline.submit((PipelineTask) pipelineTaskCxt -> {
            Configuration conf = pipelineTaskCxt.getConfig();
            PWindowSource<IVertex<Integer, Double>> prVertices =
                pipelineTaskCxt.buildSource(new FileSource<>("data/input/email_vertex",
                    line -> {
                        String[] fields = line.split(",");
                        IVertex<Integer, Double> vertex = new ValueVertex<>(
                            Integer.valueOf(fields[0]), Double.valueOf(fields[1]));
                        return Collections.singletonList(vertex);
                    }), AllWindow.getInstance())
                    .withParallelism(conf.getInteger(ExampleConfigKeys.SOURCE_PARALLELISM));

            PWindowSource<IEdge<Integer, Integer>> prEdges = pipelineTaskCxt.buildSource(new FileSource<>("data/input/email_edge",
                line -> {
                    String[] fields = line.split(",");
                    IEdge<Integer, Integer> edge = new ValueEdge<>(Integer.valueOf(fields[0]), Integer.valueOf(fields[1]), 1);
                    return Collections.singletonList(edge);
                }), AllWindow.getInstance())
                .withParallelism(conf.getInteger(ExampleConfigKeys.SOURCE_PARALLELISM));

            GraphViewDesc graphViewDesc = GraphViewBuilder
                .createGraphView(GraphViewBuilder.DEFAULT_GRAPH)
                .withShardNum(2)
                .withBackend(BackendType.Memory)
                .build();

            PGraphWindow<Integer, Double, Integer> graphWindow =
                pipelineTaskCxt.buildWindowStreamGraph(prVertices, prEdges, graphViewDesc);

            SinkFunction<IVertex<Integer, Double>> sink = ExampleSinkFunctionFactory.getSinkFunction(conf);
            graphWindow.compute(new AverageDegreeAlgorithms(3))
                .compute(conf.getInteger(ExampleConfigKeys.ITERATOR_PARALLELISM))
                .getVertices()
                .sink(sink)
                .withParallelism(conf.getInteger(ExampleConfigKeys.SINK_PARALLELISM));
        });

        return pipeline.execute();
    }

    public static void validateResult() throws IOException {
        ResultValidator.validateResult(REF_FILE_PATH, RESULT_FILE_PATH);
    }

    public static class AverageDegreeAlgorithms extends VertexCentricAggCompute<Integer, Double,
        Integer, Integer, Integer, Tuple<Integer, Integer>, Tuple<Integer, Integer>,
        Tuple<Integer, Integer>, Integer> {

        public AverageDegreeAlgorithms(long iterations) {
            super(iterations);
        }

        @Override
        public VertexCentricAggComputeFunction<Integer, Double, Integer, Integer, Integer,
            Integer> getComputeFunction() {
            return new VertexCentricAggComputeFunction<Integer, Double, Integer, Integer, Integer, Integer>() {

                private VertexCentricComputeFuncContext<Integer, Double, Integer, Integer> vertexCentricFuncContext;
                private VertexCentricAggContext<Integer, Integer> aggContext;

                @Override
                public void init(VertexCentricComputeFuncContext<Integer, Double, Integer, Integer> vertexCentricFuncContext) {
                    this.vertexCentricFuncContext = vertexCentricFuncContext;
                }

                @Override
                public void initContext(VertexCentricAggContext<Integer, Integer> aggContext) {
                    this.aggContext = aggContext;
                }


                @Override
                public void compute(Integer vertex, Iterator<Integer> messageIterator) {
                    if (vertexCentricFuncContext.getIterationId() == 1) {
                        int degreeSize = vertexCentricFuncContext.edges().getOutEdges().size();
                        vertexCentricFuncContext.setNewVertexValue(Double.valueOf(degreeSize));
                        aggContext.aggregate(degreeSize);
                        vertexCentricFuncContext.sendMessage(vertex, degreeSize);
                    } else {
                        int sum = 0;
                        while (messageIterator.hasNext()) {
                            sum += messageIterator.next();
                        }
                        aggContext.aggregate(sum);
                        vertexCentricFuncContext.setNewVertexValue(Double.valueOf(aggContext.getAggregateResult()));
                    }
                }

                @Override
                public void finish() {

                }
            };
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
                            if (value == 0) {
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
    }
}
