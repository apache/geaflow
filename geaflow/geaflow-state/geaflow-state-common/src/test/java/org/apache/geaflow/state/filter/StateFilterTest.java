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

package org.apache.geaflow.state.filter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.geaflow.model.graph.edge.EdgeDirection;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.edge.impl.IDEdge;
import org.apache.geaflow.model.graph.edge.impl.IDTimeEdge;
import org.apache.geaflow.model.graph.vertex.impl.IDVertex;
import org.apache.geaflow.state.data.OneDegreeGraph;
import org.apache.geaflow.state.data.TimeRange;
import org.apache.geaflow.state.pushdown.filter.AndFilter;
import org.apache.geaflow.state.pushdown.filter.EdgeLabelFilter;
import org.apache.geaflow.state.pushdown.filter.EdgeTsFilter;
import org.apache.geaflow.state.pushdown.filter.EdgeValueDropFilter;
import org.apache.geaflow.state.pushdown.filter.FilterType;
import org.apache.geaflow.state.pushdown.filter.IFilter;
import org.apache.geaflow.state.pushdown.filter.InEdgeFilter;
import org.apache.geaflow.state.pushdown.filter.OutEdgeFilter;
import org.apache.geaflow.state.pushdown.filter.VertexLabelFilter;
import org.apache.geaflow.state.pushdown.filter.VertexTsFilter;
import org.apache.geaflow.state.pushdown.filter.VertexValueDropFilter;
import org.apache.geaflow.state.pushdown.filter.inner.EmptyGraphFilter;
import org.apache.geaflow.state.pushdown.filter.inner.FilterHelper;
import org.apache.geaflow.state.pushdown.filter.inner.GraphFilter;
import org.apache.geaflow.state.pushdown.filter.inner.IGraphFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class StateFilterTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(StateFilterTest.class);

    @Test
    public void testValidate() {
        EdgeTsFilter tsFilter = new EdgeTsFilter(TimeRange.of(100, 200));
        Exception e = null;
        try {
            IFilter filter = tsFilter.or(new EdgeValueDropFilter());
        } catch (Exception ex) {
            e = ex;
        }
        Assert.assertEquals(e.getClass(), IllegalArgumentException.class);

        e = null;
        try {
            IFilter filter = tsFilter.or(new VertexTsFilter(TimeRange.of(100, 200)));
        } catch (Exception ex) {
            e = ex;
        }
        Assert.assertEquals(e.getClass(), IllegalArgumentException.class);

        e = null;
        try {
            IFilter filter = tsFilter.or(new VertexTsFilter(TimeRange.of(100, 200)).and(tsFilter));
        } catch (Exception ex) {
            e = ex;
        }
        Assert.assertEquals(e.getClass(), IllegalArgumentException.class);

        e = null;
        try {
            IFilter filter = tsFilter.or(
                new EdgeTsFilter(TimeRange.of(100, 200)).and(new EdgeValueDropFilter()));
        } catch (Exception ex) {
            e = ex;
        }
        Assert.assertEquals(e.getClass(), IllegalArgumentException.class);

        e = null;
        try {
            IFilter filter = tsFilter.or(new EdgeValueDropFilter());
        } catch (Exception ex) {
            e = ex;
        }
        Assert.assertEquals(e.getClass(), IllegalArgumentException.class);

        e = null;
        try {
            IFilter filter = tsFilter.and(new EdgeValueDropFilter())
                .or(new EdgeTsFilter(TimeRange.of(100, 200)));
        } catch (Exception ex) {
            e = ex;
        }
        Assert.assertEquals(e.getClass(), IllegalArgumentException.class);

        IFilter filter = tsFilter.and(new EdgeValueDropFilter())
            .or(new EdgeTsFilter(TimeRange.of(100, 200)).and(new EdgeValueDropFilter()));
        filter = new EdgeValueDropFilter().and(new VertexValueDropFilter())
            .or(new EdgeTsFilter(TimeRange.of(100, 200)).and(new EdgeValueDropFilter())
                .and(new VertexValueDropFilter()));
    }

    @Test
    public void testEdgeTsFilter() {
        EdgeTsFilter filter = new EdgeTsFilter(TimeRange.of(100, 200));
        Assert.assertEquals(filter.getFilterType(), FilterType.EDGE_TS);
        IEdge<String, Object> edge = new IDTimeEdge<>("hello", "world", 99);
        Assert.assertFalse(filter.filter(edge));

        IGraphFilter complexFilter = GraphFilter.of(filter);
        Assert.assertFalse(complexFilter.filterEdge(edge));
    }

    @Test
    public void testEdgeDirection() {
        IEdge<String, Object> edge1 = new IDEdge<>("hello", "world");
        edge1.setDirect(EdgeDirection.OUT);
        IEdge<String, Object> edge2 = new IDEdge<>("hello", "world");
        edge2.setDirect(EdgeDirection.IN);

        IGraphFilter filter = GraphFilter.of(new InEdgeFilter());
        Assert.assertFalse(filter.filterEdge(edge1));
        Assert.assertTrue(filter.filterEdge(edge2));

        filter = GraphFilter.of(new OutEdgeFilter());
        Assert.assertTrue(filter.filterEdge(edge1));
        Assert.assertFalse(filter.filterEdge(edge2));
    }

    @Test
    public void testAndFilter() {
        IFilter filter = new EdgeTsFilter(TimeRange.of(1L, 2L));
        IFilter appendFilter = new InEdgeFilter().and(new EdgeTsFilter(TimeRange.of(1L, 2L)));
        filter = filter.and(appendFilter);
        Assert.assertEquals(appendFilter.getFilterType(), FilterType.AND);
        Assert.assertEquals(filter.toString(), filter.and(EmptyGraphFilter.of()).toString());

        Assert.assertEquals(((AndFilter) appendFilter).getFilters().size(), 2);
        Assert.assertEquals(filter.getFilterType(), FilterType.AND);
        Assert.assertEquals(((AndFilter) filter).getFilters().size(), 3);

        filter = new InEdgeFilter().and(new EdgeTsFilter(TimeRange.of(100, 200)));
        IEdge<String, Object> edge = new IDTimeEdge<>("hello", "world", 100);
        IGraphFilter stateFilter = GraphFilter.of(filter);
        Assert.assertFalse(stateFilter.filterEdge(edge));

        edge.setDirect(EdgeDirection.IN);
        Assert.assertTrue(stateFilter.filterEdge(edge));
        Assert.assertFalse(stateFilter.dropAllRemaining());

        List<IEdge<String, Object>> list = Arrays.asList(new IDTimeEdge<>("hello", "world", 100),
            new IDTimeEdge<>("hello", "world", 120), new IDTimeEdge<>("hello", "world", 140),
            new IDTimeEdge<>("hello", "world", 160), new IDTimeEdge<>("hello", "world", 180));

        list.get(0).setDirect(EdgeDirection.IN);
        list.get(2).setDirect(EdgeDirection.IN);
        list.get(4).setDirect(EdgeDirection.IN);

        filter = new InEdgeFilter().and(new EdgeTsFilter(TimeRange.of(80, 141)));
        stateFilter = GraphFilter.of(filter);
        LOGGER.info("filter {}", stateFilter.toString());
        IGraphFilter graphFilter = GraphFilter.of(filter);
        Assert.assertTrue(graphFilter.contains(FilterType.EDGE_TS));
        Assert.assertEquals(graphFilter.retrieve(FilterType.EDGE_TS).getFilterType(), FilterType.EDGE_TS);
        List<IEdge<String, Object>> res = list.stream().filter(stateFilter::filterEdge)
            .collect(Collectors.toList());
        Assert.assertEquals(res.size(), 2);
        Assert.assertTrue(res.contains(list.get(0)));
        Assert.assertTrue(res.contains(list.get(2)));

        graphFilter = GraphFilter.of(filter);
        Assert.assertTrue(graphFilter.filterVertex(new IDVertex("1")));
        Assert.assertTrue(graphFilter.filterOneDegreeGraph(new OneDegreeGraph<>("1", null, null)));
    }

    @Test
    public void testOrFilter() {
        IFilter filter = new EdgeTsFilter(TimeRange.of(1L, 2L));
        filter = filter.or(new EdgeTsFilter(TimeRange.of(3L, 4L)));

        Assert.assertEquals(filter.getFilterType(), FilterType.OR);
        Assert.assertEquals(filter.or(EmptyGraphFilter.of()).toString(), filter.toString());

        List<IEdge<String, Object>> list = Arrays.asList(new IDTimeEdge<>("hello", "world", 1),
            new IDTimeEdge<>("hello", "world", 3), new IDTimeEdge<>("hello", "world", 5));

        IGraphFilter graphFilter = GraphFilter.of(filter);
        List<IEdge<String, Object>> res = new ArrayList<>();
        for (IEdge<String, Object> stringObjectIEdge : list) {
            if (graphFilter.filterEdge(stringObjectIEdge)) {
                res.add(stringObjectIEdge);
            }
        }
        Assert.assertEquals(res.size(), 2);

        filter = new EdgeTsFilter(TimeRange.of(5L, 6L)).or(filter);
        res.clear();
        graphFilter = GraphFilter.of(filter);
        for (IEdge<String, Object> stringObjectIEdge : list) {
            if (graphFilter.filterEdge(stringObjectIEdge)) {
                res.add(stringObjectIEdge);
            }
        }
        Assert.assertEquals(res.size(), 3);

        graphFilter = GraphFilter.of(filter);
        Assert.assertTrue(graphFilter.filterVertex(new IDVertex("1")));
        Assert.assertTrue(graphFilter.filterOneDegreeGraph(new OneDegreeGraph<>("1", null, null)));
    }

    @Test
    public void testParseLabelFilter() {
        String label1 = "person";
        String label2 = "trade";
        String label3 = "relation";

        IGraphFilter filter = GraphFilter.of(new EdgeLabelFilter(label1));
        List<String> labels = FilterHelper.parseLabel(GraphFilter.of(filter), false);
        Assert.assertEquals(labels.size(), 1);
        Assert.assertEquals(labels.get(0), label1);

        filter = GraphFilter.of(new EdgeLabelFilter(label1))
            .or(GraphFilter.of(new EdgeTsFilter(TimeRange.of(5L, 6L))));
        labels = FilterHelper.parseLabel(filter, false);
        Assert.assertEquals(labels.size(), 0);

        filter = GraphFilter.of(new EdgeLabelFilter(label1))
            .or(GraphFilter.of(new EdgeLabelFilter(label2)));
        labels = FilterHelper.parseLabel(filter, false);
        Assert.assertEquals(labels.size(), 2);
        Assert.assertEquals(labels.get(0), label1);
        Assert.assertEquals(labels.get(1), label2);

        filter = GraphFilter.of(new EdgeLabelFilter(label1))
            .and(GraphFilter.of(new EdgeLabelFilter(label2)));
        labels = FilterHelper.parseLabel(filter, false);
        Assert.assertEquals(labels.size(), 0);

        filter = GraphFilter.of(new VertexLabelFilter(label1))
            .or(GraphFilter.of(new VertexLabelFilter(label2)))
            .or(GraphFilter.of(new VertexLabelFilter(label3)));
        labels = FilterHelper.parseLabel(filter, true);
        Assert.assertEquals(labels.size(), 3);
        Assert.assertEquals(labels.get(0), label1);
        Assert.assertEquals(labels.get(1), label2);
        Assert.assertEquals(labels.get(2), label3);

        labels = FilterHelper.parseLabel(filter, false);
        Assert.assertEquals(labels.size(), 0);
    }

    @Test
    public void testParseDtFilter() {
        TimeRange range = TimeRange.of(1735660800, 1740758400);
        IGraphFilter filter = GraphFilter.of(new EdgeTsFilter(range));
        TimeRange parse_range = FilterHelper.parseDt(filter, false);
        Assert.assertEquals(parse_range, range);
        parse_range = FilterHelper.parseDt(filter, true);
        Assert.assertNull(parse_range);

        filter = GraphFilter.of(new EdgeLabelFilter("person").or(
                new EdgeLabelFilter("trade").or(new EdgeTsFilter(range)))
            .or(new EdgeLabelFilter("foo")));
        parse_range = FilterHelper.parseDt(filter, false);
        Assert.assertEquals(parse_range, range);
        parse_range = FilterHelper.parseDt(filter, true);
        Assert.assertNull(parse_range);

        filter = GraphFilter.of(new VertexLabelFilter("person").or(
                new VertexLabelFilter("trade").or(new VertexTsFilter(range)))
            .or(new VertexLabelFilter("foo")));
        parse_range = FilterHelper.parseDt(filter, true);
        Assert.assertEquals(parse_range, range);
        parse_range = FilterHelper.parseDt(filter, false);
        Assert.assertNull(parse_range);
    }
}
