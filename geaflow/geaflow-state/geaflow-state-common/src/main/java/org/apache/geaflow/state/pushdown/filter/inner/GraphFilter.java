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

package org.apache.geaflow.state.pushdown.filter.inner;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.state.data.DataType;
import org.apache.geaflow.state.data.OneDegreeGraph;
import org.apache.geaflow.state.pushdown.filter.AndFilter;
import org.apache.geaflow.state.pushdown.filter.FilterType;
import org.apache.geaflow.state.pushdown.filter.IFilter;
import org.apache.geaflow.state.pushdown.filter.OrFilter;
import org.apache.geaflow.state.pushdown.limit.IEdgeLimit;

public class GraphFilter extends BaseGraphFilter {

    private final DataType dataType;
    private IFilter filter;

    private GraphFilter(IFilter filter) {
        this.filter = filter;
        this.dataType = filter.dateType();
    }

    public static IGraphFilter of(IFilter filter) {
        if (filter instanceof IGraphFilter) {
            return (IGraphFilter) filter;
        }
        switch (filter.getFilterType()) {
            case EMPTY:
                return EmptyGraphFilter.of();
            case AND:
                List<IGraphFilter> childrenFilters =
                    ((AndFilter) filter).getFilters().stream().map(GraphFilter::of).collect(Collectors.toList());
                return new AndGraphFilter(childrenFilters);
            case OR:
                childrenFilters =
                    ((OrFilter) filter).getFilters().stream().map(GraphFilter::of).collect(Collectors.toList());
                return new OrGraphFilter(childrenFilters);
            default:
                return new GraphFilter(filter);
        }
    }

    public static IGraphFilter of(IGraphFilter graphFilter, IEdgeLimit limit) {
        return limit == null ? graphFilter : LimitFilterBuilder.build(graphFilter, limit);
    }

    public static IGraphFilter of(IFilter filter, IEdgeLimit limit) {
        IGraphFilter graphFilter = of(filter);
        return of(graphFilter, limit);
    }

    @Override
    public boolean filterEdge(IEdge edge) {
        if (dataType == DataType.E) {
            return filter.filter(edge);
        }
        return true;
    }

    @Override
    public boolean filterVertex(IVertex vertex) {
        if (dataType == DataType.V) {
            return filter.filter(vertex);
        }
        return true;
    }

    @Override
    public boolean filterOneDegreeGraph(OneDegreeGraph oneDegreeGraph) {
        if (dataType == DataType.VE) {
            return filter.filter(oneDegreeGraph);
        }
        return true;
    }

    @Override
    public DataType dateType() {
        return filter.dateType();
    }

    @Override
    public FilterType getFilterType() {
        return filter.getFilterType();
    }

    @Override
    public IFilter getFilter() {
        return filter;
    }
}
