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

package org.apache.geaflow.example.graph.statical.compute.allshortestpath;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

public class AllShortestPath {

    private static final Logger LOGGER = LoggerFactory.getLogger(AllShortestPath.class);

    public static final String REFERENCE_FILE_PATH = "data/reference/allshortestpath";

    public static final String RESULT_FILE_DIR = "./target/tmp/data/result/allshortestpath";

    private static final int SOURCE_ID = 1525;

    public static void main(String[] args) {
        Environment environment = EnvironmentUtil.loadEnvironment(args);
        IPipelineResult<?> result = AllShortestPath.submit(environment);
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

            FileSource<IVertex<Integer, Map<String, Object>>> vSource = new FileSource<>(
                GraphDataSet.DATASET_FILE, VertexEdgeParser::vertexParserObjectMap);
            PWindowStream<IVertex<Integer, Map<String, Object>>> vertices =
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
            PWindowStream<IVertex<Integer, Map<String, Object>>> result =
                pipelineTaskCxt.buildWindowStreamGraph(vertices, edges, graphViewDesc)
                    .compute(new AllShortestPathAlgorithms(SOURCE_ID, 10))
                    .compute(iterationParallelism)
                    .getVertices();

            SinkFunction<String> sink = ExampleSinkFunctionFactory.getSinkFunction(config);
            result.map(v -> String.format("%s,%s", v.getId(), v.getValue())).sink(sink).withParallelism(sinkParallelism);
        });

        return pipeline.execute();
    }

    public static void validateResult() throws IOException {
        ResultValidator.validateMapResult(REFERENCE_FILE_PATH, RESULT_FILE_DIR);
    }

    public static class AllShortestPathAlgorithms extends VertexCentricCompute<Integer,
        Map<String, Object>, Map<String, Integer>, Tuple<Integer, Set<List<Integer>>>> {

        private final int sourceId;

        public AllShortestPathAlgorithms(int sourceId, long iterations) {
            super(iterations);
            this.sourceId = sourceId;
        }

        @Override
        public VertexCentricComputeFunction<Integer, Map<String, Object>, Map<String, Integer>, Tuple<Integer, Set<List<Integer>>>> getComputeFunction() {
            return new AllShortestPathVertexCentricComputeFunction(this.sourceId, EdgeDirection.OUT);
        }

        @Override
        public VertexCentricCombineFunction<Tuple<Integer, Set<List<Integer>>>> getCombineFunction() {
            return null;
        }

    }

    public static class AllShortestPathVertexCentricComputeFunction extends AbstractVcFunc<Integer, Map<String, Object>, Map<String, Integer>, Tuple<Integer, Set<List<Integer>>>> {

        private static final String KEY_FIELD = "dis";
        private static final String KEY_ALL_PATHS = "allPaths";
        private final int sourceId;
        private final EdgeDirection edgeType;

        public AllShortestPathVertexCentricComputeFunction(int sourceId, EdgeDirection edgeType) {
            this.sourceId = sourceId;
            this.edgeType = edgeType;
        }

        @Override
        public void compute(Integer vertexId,
                            Iterator<Tuple<Integer, Set<List<Integer>>>> messageIterator) {
            IVertex<Integer, Map<String, Object>> vertex = this.context.vertex().get();
            Map<String, Object> property = vertex.getValue();
            int dis = Integer.MAX_VALUE;
            Set<List<Integer>> path = new HashSet<>();
            if (this.context.getIterationId() == 1) {
                if (vertex.getId().equals(sourceId)) {
                    dis = 0;
                    path.add(new ArrayList<>(Collections.singletonList(sourceId)));
                    sendMessage(dis, path);
                }
                property.put(KEY_ALL_PATHS, path);
                property.put(KEY_FIELD, dis);
                this.context.setNewVertexValue(property);
                return;
            }

            dis = (int) property.get(KEY_FIELD);
            Tuple<Integer, Set<List<Integer>>> shortestDis = messageIterator.next();
            while (messageIterator.hasNext()) {
                Tuple<Integer, Set<List<Integer>>> msg = messageIterator.next();
                if (shortestDis.getF0() > msg.getF0()) {
                    shortestDis = msg;
                } else if (shortestDis.getF0().equals(msg.getF0())) {
                    for (List<Integer> l : msg.getF1()) {
                        shortestDis.getF1().add(new ArrayList<>(l));
                    }
                }
            }

            if (shortestDis.getF0() <= dis) {
                if (shortestDis.getF0() == dis) {
                    path.addAll((Collection<? extends List<Integer>>) property.get(KEY_ALL_PATHS));
                }
                dis = shortestDis.getF0();
                property.put(KEY_FIELD, dis);
                for (List<Integer> p : shortestDis.getF1()) {
                    List<Integer> list = new ArrayList<>(p);
                    list.add(vertex.getId());
                    path.add(list);
                }
                property.put(KEY_ALL_PATHS, path);
                this.context.setNewVertexValue(property);
                sendMessage(dis, path);
            }
        }

        private void sendMessage(int dis, Set<List<Integer>> path) {
            switch (this.edgeType) {
                case OUT:
                    for (IEdge<Integer, Map<String, Integer>> edge : this.context.edges()
                        .getOutEdges()) {
                        this.context.sendMessage(edge.getTargetId(), new Tuple<>(dis + edge.getValue().get(KEY_FIELD), path));
                    }
                    break;
                case IN:
                    for (IEdge<Integer, Map<String, Integer>> edge : this.context.edges()
                        .getInEdges()) {
                        this.context.sendMessage(edge.getTargetId(), new Tuple<>(dis + edge.getValue().get(KEY_FIELD), path));
                    }
                    break;
                default:
                    break;
            }
        }

    }

}
