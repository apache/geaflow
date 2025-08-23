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

package org.apache.geaflow.example.encoder;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.geaflow.api.function.internal.CollectionSource;
import org.apache.geaflow.api.function.io.SinkFunction;
import org.apache.geaflow.api.graph.PGraphWindow;
import org.apache.geaflow.api.graph.compute.IncVertexCentricCompute;
import org.apache.geaflow.api.graph.compute.VertexCentricCompute;
import org.apache.geaflow.api.graph.function.vc.IncVertexCentricComputeFunction;
import org.apache.geaflow.api.graph.function.vc.VertexCentricCombineFunction;
import org.apache.geaflow.api.graph.function.vc.VertexCentricComputeFunction;
import org.apache.geaflow.api.pdata.PStreamSource;
import org.apache.geaflow.api.pdata.stream.window.PWindowSource;
import org.apache.geaflow.api.pdata.stream.window.PWindowStream;
import org.apache.geaflow.api.window.WindowFactory;
import org.apache.geaflow.api.window.impl.AllWindow;
import org.apache.geaflow.api.window.impl.SizeTumblingWindow;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.encoder.Encoders;
import org.apache.geaflow.common.encoder.IEncoder;
import org.apache.geaflow.common.encoder.impl.AbstractEncoder;
import org.apache.geaflow.common.tuple.Tuple;
import org.apache.geaflow.common.type.primitive.IntegerType;
import org.apache.geaflow.env.EnvironmentFactory;
import org.apache.geaflow.example.base.BaseTest;
import org.apache.geaflow.example.function.FileSink;
import org.apache.geaflow.example.function.FileSource;
import org.apache.geaflow.example.stream.StreamWordCountPipeline;
import org.apache.geaflow.example.util.ExampleSinkFunctionFactory;
import org.apache.geaflow.example.util.ResultValidator;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.edge.impl.ValueEdge;
import org.apache.geaflow.model.graph.meta.GraphMetaType;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.model.graph.vertex.impl.ValueVertex;
import org.apache.geaflow.pipeline.IPipelineResult;
import org.apache.geaflow.pipeline.Pipeline;
import org.apache.geaflow.pipeline.PipelineFactory;
import org.apache.geaflow.pipeline.task.PipelineTask;
import org.apache.geaflow.view.GraphViewBuilder;
import org.apache.geaflow.view.IViewDesc;
import org.apache.geaflow.view.graph.GraphViewDesc;
import org.apache.geaflow.view.graph.PGraphView;
import org.apache.geaflow.view.graph.PIncGraphView;
import org.testng.annotations.Test;

public class EncoderIntegrationTest extends BaseTest {

    public static final String REF_PATH_PREFIX = "data/reference/encoder/";
    public static final String RES_PATH_PREFIX = "./target/tmp/data/result/encoder/";

    public static final String TAG_KEY_BY = "key_by";
    public static final String TAG_VC = "vc";
    public static final String TAG_INC_VC = "inc_vc";
    public static final String TAG_STREAM = "stream";

    private static String getRefPath(String tag) {
        return REF_PATH_PREFIX + tag;
    }

    private static String getResPath(String tag) {
        return RES_PATH_PREFIX + tag;
    }

    @Test
    public void testKeyByWithEncoder() throws Exception {
        String tag = TAG_KEY_BY;
        String resPath = getResPath(tag);

        ResultValidator.cleanResult(resPath);
        this.environment = EnvironmentFactory.onLocalEnvironment();
        Configuration config = this.environment.getEnvironmentContext().getConfig();
        config.putAll(this.config);
        config.put(FileSink.OUTPUT_DIR, resPath);

        Pipeline pipeline = PipelineFactory.buildPipeline(this.environment);
        pipeline.submit((PipelineTask) pipelineTaskCxt -> {
            List<String> words = Lists.newArrayList("hello", "world", "hello", "word");

            IEncoder<Tuple<String, Integer>> tEncoder = Encoders.tuple(Encoders.STRING, Encoders.INTEGER);
            PStreamSource<String> streamSource =
                pipelineTaskCxt.buildSource(new CollectionSource<>(words),
                        SizeTumblingWindow.of(100))
                    .withEncoder(Encoders.STRING)
                    .window(WindowFactory.createSizeTumblingWindow(4));

            SinkFunction<String> sink = ExampleSinkFunctionFactory.getSinkFunction(pipelineTaskCxt.getConfig());
            streamSource.map(s -> Tuple.of(s, 1)).withEncoder(tEncoder)
                .keyBy(Tuple::getF0).withEncoder(tEncoder)
                .map(String::valueOf).withEncoder(Encoders.STRING)
                .sink(sink);
        });

        IPipelineResult<?> result = pipeline.execute();
        result.get();
        ResultValidator.validateResult(getRefPath(tag), resPath);
    }

    @Test
    public void testVc() throws Exception {
        String tag = TAG_VC;
        String resPath = getResPath(tag);

        ResultValidator.cleanResult(resPath);
        this.environment = EnvironmentFactory.onLocalEnvironment();
        Configuration config = this.environment.getEnvironmentContext().getConfig();
        config.putAll(this.config);
        config.put(FileSink.OUTPUT_DIR, resPath);

        Pipeline pipeline = PipelineFactory.buildPipeline(this.environment);
        pipeline.submit((PipelineTask) pipelineTaskCxt -> {
            int sourceParallelism = 3;
            int iterationParallelism = 7;
            int sinkParallelism = 5;

            PWindowStream<IVertex<Integer, Double>> prVertices =
                pipelineTaskCxt.buildSource(new FileSource<>("data/input/email_vertex",
                        (FileSource.FileLineParser<IVertex<Integer, Double>>) line -> {
                            String[] fields = line.split(",");
                            IVertex<Integer, Double> vertex = new ValueVertex<>(
                                Integer.valueOf(fields[0]), Double.valueOf(fields[1]));
                            return Collections.singletonList(vertex);
                        }), AllWindow.getInstance())
                    .withParallelism(sourceParallelism)
                    .withEncoder(VEncoder.INSTANCE);

            PWindowStream<IEdge<Integer, Integer>> prEdges =
                pipelineTaskCxt.buildSource(new FileSource<>("data/input/email_edge",
                        (FileSource.FileLineParser<IEdge<Integer, Integer>>) line -> {
                            String[] fields = line.split(",");
                            IEdge<Integer, Integer> edge = new ValueEdge<>(
                                Integer.valueOf(fields[0]), Integer.valueOf(fields[1]), 1);
                            return Collections.singletonList(edge);
                        }), AllWindow.getInstance())
                    .withParallelism(sourceParallelism)
                    .withEncoder(EEncoder.INSTANCE);

            GraphViewDesc graphViewDesc = GraphViewBuilder
                .createGraphView(GraphViewBuilder.DEFAULT_GRAPH)
                .withShardNum(8)
                .withBackend(IViewDesc.BackendType.Memory)
                .build();
            PGraphWindow<Integer, Double, Integer> graphWindow =
                pipelineTaskCxt.buildWindowStreamGraph(prVertices, prEdges, graphViewDesc);

            SinkFunction<String> sink = ExampleSinkFunctionFactory.getSinkFunction(pipelineTaskCxt.getConfig());
            graphWindow
                .compute(new VcFunc(3))
                .compute(iterationParallelism)
                .getVertices().withEncoder(VEncoder.INSTANCE)
                .map(v -> v.getId() + " " + v.getValue()).withEncoder(Encoders.STRING)
                .sink(sink)
                .withParallelism(sinkParallelism);
        });

        IPipelineResult<?> result = pipeline.execute();
        result.get();
        ResultValidator.validateResult(getRefPath(tag), resPath);
    }

    private static class VcFunc extends VertexCentricCompute<Integer, Double, Integer, Double> {

        public VcFunc(long iterations) {
            super(iterations);
        }

        @Override
        public VertexCentricComputeFunction<Integer, Double, Integer, Double> getComputeFunction() {
            return new PRVertexCentricComputeFunction();
        }

        @Override
        public VertexCentricCombineFunction<Double> getCombineFunction() {
            return null;
        }

        @Override
        public IEncoder<Integer> getKeyEncoder() {
            return Encoders.INTEGER;
        }

        @Override
        public IEncoder<Double> getMessageEncoder() {
            return Encoders.DOUBLE;
        }

    }

    private static class PRVertexCentricComputeFunction
        implements VertexCentricComputeFunction<Integer, Double, Integer, Double> {

        private VertexCentricComputeFunction.VertexCentricComputeFuncContext<Integer, Double, Integer, Double> context;

        @Override
        public void init(VertexCentricComputeFuncContext<Integer, Double, Integer, Double> context) {
            this.context = context;
        }

        @Override
        public void compute(Integer vertexId,
                            Iterator<Double> messageIterator) {
            IVertex<Integer, Double> vertex = this.context.vertex().get();
            if (this.context.getIterationId() == 1) {
                this.context.sendMessageToNeighbors(vertex.getValue());
            } else {
                double sum = 0;
                while (messageIterator.hasNext()) {
                    double value = messageIterator.next();
                    sum += value;
                }
                this.context.setNewVertexValue(sum);
            }
        }

        @Override
        public void finish() {
        }

    }

    @Test
    public void testIncVC() throws Exception {
        String tag = TAG_INC_VC;
        String resPath = getResPath(tag);

        ResultValidator.cleanResult(resPath);
        this.environment = EnvironmentFactory.onLocalEnvironment();
        Configuration config = this.environment.getEnvironmentContext().getConfig();
        config.putAll(this.config);
        config.put(FileSink.OUTPUT_DIR, resPath);

        Pipeline pipeline = PipelineFactory.buildPipeline(this.environment);

        int sourceParallelism = 3;
        int iterationParallelism = 4;
        int sinkParallelism = 2;

        final String graphName = "graph_view_name";
        GraphViewDesc graphViewDesc = GraphViewBuilder.createGraphView(graphName)
            .withShardNum(iterationParallelism)
            .withBackend(IViewDesc.BackendType.RocksDB)
            .withSchema(new GraphMetaType<>(IntegerType.INSTANCE, ValueVertex.class,
                Integer.class, ValueEdge.class, IntegerType.class))
            .build();
        pipeline.withView(graphName, graphViewDesc);

        pipeline.submit((PipelineTask) pipelineTaskCxt -> {

            PWindowSource<IVertex<Integer, Integer>> vertices =
                pipelineTaskCxt.buildSource(new FileSource<>("data/input/email_vertex",
                        (FileSource.FileLineParser<IVertex<Integer, Integer>>) line -> {
                            String[] fields = line.split(",");
                            ValueVertex<Integer, Integer> vertex = new ValueVertex<>(
                                Integer.valueOf(fields[0]), Integer.valueOf(fields[1]));
                            return Collections.singletonList(vertex);
                        }), SizeTumblingWindow.of(500))
                    .withParallelism(sourceParallelism).withEncoder(IncVEncoder.INSTANCE);

            PWindowSource<IEdge<Integer, Integer>> edges =
                pipelineTaskCxt.buildSource(new FileSource<>("data/input/email_edge",
                    (FileSource.FileLineParser<IEdge<Integer, Integer>>) line -> {
                        String[] fields = line.split(",");
                        IEdge<Integer, Integer> edge = new ValueEdge<>(Integer.valueOf(fields[0]),
                            Integer.valueOf(fields[1]), 1);
                        return Collections.singletonList(edge);
                    }), SizeTumblingWindow.of(10000)).withParallelism(sourceParallelism).withEncoder(EEncoder.INSTANCE);

            PGraphView<Integer, Integer, Integer> fundGraphView =
                pipelineTaskCxt.getGraphView(graphName);

            PIncGraphView<Integer, Integer, Integer> incGraphView =
                fundGraphView.appendGraph(vertices, edges);

            SinkFunction<String> sink = ExampleSinkFunctionFactory.getSinkFunction(pipelineTaskCxt.getConfig());
            incGraphView.incrementalCompute(new IncVcFunc(3))
                .getVertices().withEncoder(IncVEncoder.INSTANCE)
                .map(v -> v.getId() + " " + v.getValue()).withEncoder(Encoders.STRING)
                .sink(sink).withParallelism(sinkParallelism);
        });

        IPipelineResult<?> result = pipeline.execute();
        result.get();
        ResultValidator.validateResult(getRefPath(tag), resPath);
    }

    @Test
    public void testStream() throws Exception {
        String tag = TAG_STREAM;
        String resPath = getResPath(tag);

        ResultValidator.cleanResult(resPath);
        this.environment = EnvironmentFactory.onLocalEnvironment();
        Configuration config = this.environment.getEnvironmentContext().getConfig();
        config.putAll(this.config);
        config.put(FileSink.OUTPUT_DIR, resPath);

        Pipeline pipeline = PipelineFactory.buildPipeline(this.environment);
        pipeline.submit((PipelineTask) pipelineTaskCxt -> {
            int parallelism = 1;
            IEncoder<Tuple<String, Integer>> tEncoder = Encoders.tuple(Encoders.STRING, Encoders.INTEGER);
            PStreamSource<String> streamSource = pipelineTaskCxt.buildSource(new FileSource<>(
                    "data/input/email_edge",
                    line -> {
                        String[] fields = line.split(",");
                        return Collections.singletonList(fields[0]);
                    }), SizeTumblingWindow.of(5000))
                .withEncoder(Encoders.STRING)
                .withParallelism(parallelism);

            SinkFunction<String> sink = ExampleSinkFunctionFactory.getSinkFunction(pipelineTaskCxt.getConfig());
            streamSource
                .map(e -> Tuple.of(e, 1)).withEncoder(tEncoder)
                .keyBy(Tuple::getF0).withEncoder(tEncoder)
                .reduce(new StreamWordCountPipeline.CountFunc())
                .withEncoder(tEncoder).withParallelism(parallelism)
                .map(String::valueOf).withEncoder(Encoders.STRING).withParallelism(parallelism)
                .sink(sink).withParallelism(parallelism);
        });

        IPipelineResult<?> result = pipeline.execute();
        result.get();
        ResultValidator.validateResult(getRefPath(tag), resPath);
    }

    public static class IncVcFunc extends IncVertexCentricCompute<Integer, Integer, Integer, Integer> {

        public IncVcFunc(long iterations) {
            super(iterations);
        }

        @Override
        public IncVertexCentricComputeFunction<Integer, Integer, Integer, Integer> getIncComputeFunction() {
            return new PRIncVcFunc();
        }

        @Override
        public VertexCentricCombineFunction<Integer> getCombineFunction() {
            return null;
        }

        @Override
        public IEncoder<Integer> getKeyEncoder() {
            return Encoders.INTEGER;
        }

        @Override
        public IEncoder<Integer> getMessageEncoder() {
            return Encoders.INTEGER;
        }

    }

    public static class PRIncVcFunc implements IncVertexCentricComputeFunction<Integer, Integer, Integer, Integer> {

        private IncGraphComputeContext<Integer, Integer, Integer, Integer> context;

        @Override
        public void init(IncGraphComputeContext<Integer, Integer, Integer, Integer> context) {
            this.context = context;
        }

        @Override
        public void evolve(Integer vertexId,
                           TemporaryGraph<Integer, Integer, Integer> temporaryGraph) {
            long lastVersionId = 0L;
            IVertex<Integer, Integer> vertex = temporaryGraph.getVertex();
            HistoricalGraph<Integer, Integer, Integer> historicalGraph = this.context.getHistoricalGraph();
            if (vertex == null) {
                vertex = historicalGraph.getSnapShot(lastVersionId).vertex().get();
            }

            if (vertex != null) {
                List<IEdge<Integer, Integer>> newEs = temporaryGraph.getEdges();
                List<IEdge<Integer, Integer>> oldEs =
                    historicalGraph.getSnapShot(lastVersionId).edges().getOutEdges();
                if (newEs != null) {
                    for (IEdge<Integer, Integer> edge : newEs) {
                        this.context.sendMessage(edge.getTargetId(), vertexId);
                    }
                }
                if (oldEs != null) {
                    for (IEdge<Integer, Integer> edge : oldEs) {
                        this.context.sendMessage(edge.getTargetId(), vertexId);
                    }
                }
            }
        }

        @Override
        public void compute(Integer vertexId, Iterator<Integer> messageIterator) {
            int max = 0;
            while (messageIterator.hasNext()) {
                int value = messageIterator.next();
                max = Math.max(max, value);
            }
            this.context.getTemporaryGraph().updateVertexValue(max);
        }

        @Override
        public void finish(Integer vertexId, MutableGraph<Integer, Integer, Integer> mutableGraph) {
            IVertex<Integer, Integer> vertex = this.context.getTemporaryGraph().getVertex();
            List<IEdge<Integer, Integer>> edges = this.context.getTemporaryGraph().getEdges();
            if (vertex != null) {
                mutableGraph.addVertex(0, vertex);
                this.context.collect(vertex);
            }
            if (edges != null) {
                edges.forEach(edge -> mutableGraph.addEdge(0, edge));
            }
        }

    }

    private static class VEncoder extends AbstractEncoder<IVertex<Integer, Double>> {

        private static final VEncoder INSTANCE = new VEncoder();

        @Override
        public void encode(IVertex<Integer, Double> data, OutputStream outputStream) throws IOException {
            Encoders.INTEGER.encode(data.getId(), outputStream);
            Encoders.DOUBLE.encode(data.getValue(), outputStream);
        }

        @Override
        public IVertex<Integer, Double> decode(InputStream inputStream) throws IOException {
            Integer id = Encoders.INTEGER.decode(inputStream);
            Double value = Encoders.DOUBLE.decode(inputStream);
            return new ValueVertex<>(id, value);
        }

    }

    private static class EEncoder extends AbstractEncoder<IEdge<Integer, Integer>> {

        private static final EEncoder INSTANCE = new EEncoder();

        @Override
        public void encode(IEdge<Integer, Integer> data, OutputStream outputStream) throws IOException {
            Encoders.INTEGER.encode(data.getSrcId(), outputStream);
            Encoders.INTEGER.encode(data.getTargetId(), outputStream);
            Encoders.INTEGER.encode(data.getValue(), outputStream);
        }

        @Override
        public IEdge<Integer, Integer> decode(InputStream inputStream) throws IOException {
            Integer src = Encoders.INTEGER.decode(inputStream);
            Integer dst = Encoders.INTEGER.decode(inputStream);
            Integer value = Encoders.INTEGER.decode(inputStream);
            return new ValueEdge<>(src, dst, value);
        }

    }

    private static class IncVEncoder extends AbstractEncoder<IVertex<Integer, Integer>> {

        private static final IncVEncoder INSTANCE = new IncVEncoder();

        @Override
        public void encode(IVertex<Integer, Integer> data, OutputStream outputStream) throws IOException {
            Encoders.INTEGER.encode(data.getId(), outputStream);
            Encoders.INTEGER.encode(data.getValue(), outputStream);
        }

        @Override
        public IVertex<Integer, Integer> decode(InputStream inputStream) throws IOException {
            Integer id = Encoders.INTEGER.decode(inputStream);
            Integer value = Encoders.INTEGER.decode(inputStream);
            return new ValueVertex<>(id, value);
        }

    }

}
