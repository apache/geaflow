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

package com.antgroup.geaflow.state.pushdown.inner;

import com.antgroup.geaflow.model.graph.edge.EdgeDirection;
import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.model.graph.edge.impl.ValueLabelTimeEdge;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.model.graph.vertex.impl.ValueLabelTimeVertex;
import com.antgroup.geaflow.state.data.TimeRange;
import com.antgroup.geaflow.state.pushdown.filter.AndFilter;
import com.antgroup.geaflow.state.pushdown.filter.EdgeLabelFilter;
import com.antgroup.geaflow.state.pushdown.filter.EdgeTsFilter;
import com.antgroup.geaflow.state.pushdown.filter.IFilter;
import com.antgroup.geaflow.state.pushdown.filter.InEdgeFilter;
import com.antgroup.geaflow.state.pushdown.filter.OrFilter;
import com.antgroup.geaflow.state.pushdown.filter.OutEdgeFilter;
import com.antgroup.geaflow.state.pushdown.filter.VertexLabelFilter;
import com.antgroup.geaflow.state.pushdown.filter.VertexTsFilter;
import com.antgroup.geaflow.state.pushdown.filter.inner.AndGraphFilter;
import com.antgroup.geaflow.state.pushdown.filter.inner.GraphFilter;
import com.antgroup.geaflow.state.pushdown.filter.inner.IGraphFilter;
import com.antgroup.geaflow.state.pushdown.filter.inner.OrGraphFilter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@Fork(1)
@Threads(1)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@BenchmarkMode(Mode.AverageTime)
@Measurement(iterations = 3, time = 1)
@Warmup(iterations = 2, time = 1)
@State(Scope.Benchmark)
public class PushDownCodeGenJMH {

    private IFilter simpleFilter;
    private IFilter middleFilter;
    private IFilter complexFilter;
    private IGraphFilter simpleGraphFilter;
    private IGraphFilter middleGraphFilter;
    private IGraphFilter complexGraphFilter;

    @Param({"500000", "1000000", "3000000"})
    private int vertexAndEdgeNum;

    private List<IEdge<Integer, Integer>> edges = new ArrayList<>();
    private List<IVertex<Integer, Integer>> vertices = new ArrayList<>();
    private IFilterConverter converter;

    @Setup
    public void setUp() {
        simpleFilter = new EdgeLabelFilter(Arrays.asList("label1", "label2"))
            .and(new VertexTsFilter(TimeRange.of(100, 1000)));
        middleFilter = new VertexTsFilter(TimeRange.of(100, 1000))
            .or(new VertexTsFilter(TimeRange.of(10, 100)).and(new VertexLabelFilter(Arrays.asList("label2"))))
            .or(new VertexLabelFilter(Arrays.asList("label3")));
        List<IFilter> list = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            list.add(new AndFilter(i / 2 == 0 ? InEdgeFilter.getInstance() : OutEdgeFilter.getInstance(),
                new EdgeLabelFilter("label" + i % 5)).and(new EdgeTsFilter(TimeRange.of(10, 100))));
        }
        complexFilter = new OrFilter(list);

        simpleGraphFilter =
            GraphFilter.of(new EdgeLabelFilter(Arrays.asList("label1", "label2"))).and(GraphFilter.of(new VertexTsFilter(
                TimeRange.of(100, 1000))));
        middleGraphFilter =
            GraphFilter.of(new VertexTsFilter(TimeRange.of(100, 1000)))
                .or(GraphFilter.of(new VertexTsFilter(TimeRange.of(10, 100))).and(GraphFilter.of(new VertexLabelFilter(Arrays.asList("label2")))))
                .or(GraphFilter.of(new VertexLabelFilter(Arrays.asList("label3"))));

        List<IGraphFilter> graphFilters = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            graphFilters.add(new AndGraphFilter(Arrays.asList(GraphFilter.of(i / 2 == 0 ? InEdgeFilter.getInstance() : OutEdgeFilter.getInstance()),
                GraphFilter.of(new EdgeLabelFilter("label" + i)),
                GraphFilter.of(new EdgeTsFilter(TimeRange.of(10, 100))))));
        }
        complexGraphFilter = new OrGraphFilter(graphFilters);

        for (int i = 0; i < vertexAndEdgeNum; i++) {
            this.edges.add(new ValueLabelTimeEdge<>(0, i, 0,
                i / 3 == 0 ? EdgeDirection.IN : EdgeDirection.OUT,
                "label" + i % 6, i));
        }
        for (int i = 0; i < vertexAndEdgeNum; i++) {
            this.vertices.add(new ValueLabelTimeVertex<>(i, 0,
                "label" + i % 6, i));
        }
        converter = new CodeGenFilterConverter();
    }

    @Benchmark
    public void simpleCodeGenFilter(Blackhole blackhole) throws Exception {
        IGraphFilter genFilter = (IGraphFilter) converter.convert(simpleFilter);
        for (IEdge<Integer, Integer> edge: edges) {
            genFilter.filterEdge(edge);
        }
        for (IVertex<Integer, Integer> vertex: vertices) {
            genFilter.filterVertex(vertex);
        }
    }

    @Benchmark
    public void simpleGraphFilter(Blackhole blackhole) throws Exception {
        IGraphFilter genFilter = simpleGraphFilter;
        for (IEdge<Integer, Integer> edge: edges) {
            genFilter.filterEdge(edge);
        }
        for (IVertex<Integer, Integer> vertex: vertices) {
            genFilter.filterVertex(vertex);
        }
    }

    @Benchmark
    public void middleCodeGenFilter(Blackhole blackhole) throws Exception {
        IGraphFilter genFilter = (IGraphFilter) converter.convert(middleFilter);
        for (IEdge<Integer, Integer> edge: edges) {
            genFilter.filterEdge(edge);
        }
        for (IVertex<Integer, Integer> vertex: vertices) {
            genFilter.filterVertex(vertex);
        }
    }

    @Benchmark
    public void middleGraphFilter(Blackhole blackhole) throws Exception {
        IGraphFilter genFilter = middleGraphFilter;
        for (IEdge<Integer, Integer> edge: edges) {
            genFilter.filterEdge(edge);
        }
        for (IVertex<Integer, Integer> vertex: vertices) {
            genFilter.filterVertex(vertex);
        }
    }

    @Benchmark
    public void complexCodeGenFilter(Blackhole blackhole) throws Exception {
        IGraphFilter genFilter = (IGraphFilter) converter.convert(complexFilter);
        blackhole.consume(edges.stream().filter(genFilter::filterEdge).collect(Collectors.toList()));
        blackhole.consume(vertices.stream().filter(genFilter::filterVertex).collect(Collectors.toList()));
    }

    @Benchmark
    public void complexGraphFilter(Blackhole blackhole) throws Exception {
        IGraphFilter genFilter = complexGraphFilter;
        blackhole.consume(edges.stream().filter(genFilter::filterEdge).collect(Collectors.toList()));
        blackhole.consume(vertices.stream().filter(genFilter::filterVertex).collect(Collectors.toList()));
    }

    public static void main(String[] args) throws RunnerException {

        Options opt = new OptionsBuilder()
            // import test class.
            .include(PushDownCodeGenJMH.class.getSimpleName())
            .resultFormat(ResultFormatType.JSON)
            .result("allocation.json")
            .build();
        new Runner(opt).run();
    }
}
