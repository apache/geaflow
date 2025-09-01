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

package org.apache.geaflow.example.graph.statical.compute.shortestpath;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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

public class ShortestPath {

    private static final Logger LOGGER = LoggerFactory.getLogger(ShortestPath.class);

    private static final int SOURCE_ID = 498021;

    public static final String REFERENCE_FILE_PATH = "data/reference/shortestpath";

    public static final String RESULT_FILE_DIR = "./target/tmp/data/result/shortestpath";

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

            FileSource<IVertex<Integer, Map<String, String>>> vSource = new FileSource<>(
                GraphDataSet.DATASET_FILE, VertexEdgeParser::vertexParserMap);
            PWindowStream<IVertex<Integer, Map<String, String>>> vertices =
                pipelineTaskCxt.buildSource(vSource, AllWindow.getInstance()).withParallelism(sourceParallelism);

            FileSource<IEdge<Integer, Map<String, Integer>>> eSource = new FileSource<>(
                GraphDataSet.DATASET_FILE, VertexEdgeParser::edgeParserMap);
            PWindowStream<IEdge<Integer, Map<String, Integer>>> edges =
                pipelineTaskCxt.buildSource(eSource, AllWindow.getInstance()).withParallelism(sourceParallelism);

            GraphViewDesc graphViewDesc = GraphViewBuilder
                .createGraphView(GraphViewBuilder.DEFAULT_GRAPH)
                .withShardNum(iterationParallelism)
                .withBackend(BackendType.Memory)
                .build();
            PWindowStream<IVertex<Integer, Map<String, String>>> result =
                pipelineTaskCxt.buildWindowStreamGraph(vertices, edges, graphViewDesc)
                    .compute(new ShortestPathAlgorithms(SOURCE_ID, 10))
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

    public static class ShortestPathAlgorithms extends VertexCentricCompute<Integer, Map<String, String>, Map<String, Integer>, Tuple<Integer, List<String>>> {

        private final int sourceId;

        public ShortestPathAlgorithms(int sourceId, long iterations) {
            super(iterations);
            this.sourceId = sourceId;
        }

        @Override
        public VertexCentricComputeFunction<Integer, Map<String, String>, Map<String, Integer>, Tuple<Integer, List<String>>> getComputeFunction() {
            return new ShortestPathVertexCentricComputeFunction(sourceId, EdgeDirection.OUT);
        }

        @Override
        public VertexCentricCombineFunction<Tuple<Integer, List<String>>> getCombineFunction() {
            return null;
        }

    }

    public static class ShortestPathVertexCentricComputeFunction extends AbstractVcFunc<Integer, Map<String, String>, Map<String, Integer>, Tuple<Integer, List<String>>> {

        private static final String KEY_FIELD = "dis";

        private static final String PATH = "paths";

        private final int sourceId;
        private final EdgeDirection edgeType;

        public ShortestPathVertexCentricComputeFunction(int sourceId, EdgeDirection edgeType) {
            this.sourceId = sourceId;
            this.edgeType = edgeType;
        }

        @Override
        public void compute(Integer vertexId,
                            Iterator<Tuple<Integer, List<String>>> messageIterator) {
            IVertex<Integer, Map<String, String>> vertex = this.context.vertex().get();
            Map<String, String> vertexValue = vertex.getValue();
            int dis = vertex.getId().equals(sourceId) ? 0 : Integer.MAX_VALUE;
            if (this.context.getIterationId() == 1) {
                vertexValue.put(PATH, "[]");
                vertexValue.put(KEY_FIELD, String.valueOf(Integer.MAX_VALUE));
                this.context.setNewVertexValue(vertexValue);
                return;
            }
            List<String> path = new LinkedList<>();
            while (messageIterator.hasNext()) {
                Tuple<Integer, List<String>> msg = messageIterator.next();
                int tmp = msg.getF0();
                if (tmp < dis) {
                    path.clear();
                    dis = tmp;
                    path.addAll(msg.getF1());
                }
            }
            if (dis < Integer.parseInt(String.valueOf(vertexValue.get(KEY_FIELD)))) {
                vertexValue.put(KEY_FIELD, String.valueOf(dis));
                path.add(String.valueOf(vertex.getId()));
                vertexValue.put(PATH, path.toString());
                this.context.setNewVertexValue(vertexValue);
                switch (edgeType) {
                    case IN:
                        for (IEdge<Integer, Map<String, Integer>> inEdge : this.context.edges()
                            .getInEdges()) {
                            this.context.sendMessage(inEdge.getTargetId(),
                                new Tuple<>(dis + inEdge.getValue().get(KEY_FIELD), path));
                        }
                        break;
                    case OUT:
                        for (IEdge<Integer, Map<String, Integer>> outEdge : this.context.edges()
                            .getOutEdges()) {
                            this.context.sendMessage(outEdge.getTargetId(),
                                new Tuple<>(dis + outEdge.getValue().get(KEY_FIELD), path));
                        }
                        break;
                    default:
                        break;
                }
            }
        }

    }

}
