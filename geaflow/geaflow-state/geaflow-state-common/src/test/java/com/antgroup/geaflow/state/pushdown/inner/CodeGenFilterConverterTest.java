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
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class CodeGenFilterConverterTest {

    private IFilter simpleFilter;
    private IFilter middleFilter;
    private IFilter complexFilter;
    private IGraphFilter simpleGraphFilter;
    private IGraphFilter middleGraphFilter;
    private IGraphFilter complexGraphFilter;
    private List<IEdge<Integer, Integer>> edges = new ArrayList<>();
    private List<IVertex<Integer, Integer>> vertices = new ArrayList<>();
    private IFilterConverter converter;

    @BeforeClass
    public void setUp() {
        simpleFilter = new EdgeLabelFilter(Arrays.asList("label1", "你好", "label2"))
            .and(new VertexTsFilter(TimeRange.of(100, 1000)));
        middleFilter = new VertexTsFilter(TimeRange.of(100, 1000))
            .or(new VertexTsFilter(TimeRange.of(10, 100)).and(new VertexLabelFilter(Arrays.asList("label2"))))
            .or(new VertexLabelFilter(Arrays.asList("label3")));
        List<IFilter> list = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            list.add(new AndFilter(i / 2 == 0 ? InEdgeFilter.getInstance() : OutEdgeFilter.getInstance(),
                new EdgeLabelFilter("label" + i)).and(new EdgeTsFilter(TimeRange.of(10, 100))));
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
        for (int i = 0; i < 10000000; i++) {
            this.edges.add(new ValueLabelTimeEdge<>(0, i, 0,
                i / 3 == 0 ? EdgeDirection.IN : EdgeDirection.OUT,
                "label" + i % 6, i));
        }
        for (int i = 0; i < 10000000; i++) {
            this.vertices.add(new ValueLabelTimeVertex<>(i, 0,
                "label" + i % 6, i));
        }

        converter = new CodeGenFilterConverter();
    }

    @Test
    public void testSimple() throws Exception {
        IGraphFilter genFilter = (IGraphFilter) converter.convert(simpleFilter);

        long vNumber = vertices.stream().filter(genFilter::filterVertex).count();
        long eNumber = edges.stream().filter(genFilter::filterEdge).count();

        Assert.assertEquals(vNumber, vertices.stream().filter(simpleGraphFilter::filterVertex).count());
        Assert.assertEquals(eNumber, edges.stream().filter(simpleGraphFilter::filterEdge).count());
    }

    @Test
    public void testMiddle() throws Exception {
        IGraphFilter genFilter = (IGraphFilter) converter.convert(middleFilter);

        long vNumber = vertices.stream().filter(genFilter::filterVertex).count();
        long eNumber = edges.stream().filter(genFilter::filterEdge).count();

        Assert.assertEquals(vNumber, vertices.stream().filter(middleGraphFilter::filterVertex).count());
        Assert.assertEquals(eNumber, edges.stream().filter(middleGraphFilter::filterEdge).count());
    }

    @Test
    public void testComplex() throws Exception {
        IGraphFilter genFilter = (IGraphFilter) converter.convert(complexFilter);

        long vNumber = vertices.stream().filter(genFilter::filterVertex).count();
        long eNumber = edges.stream().filter(genFilter::filterEdge).count();

        Assert.assertEquals(vNumber, vertices.stream().filter(complexGraphFilter::filterVertex).count());
        Assert.assertEquals(eNumber, edges.stream().filter(complexGraphFilter::filterEdge).count());
    }
}
