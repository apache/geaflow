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

import org.apache.geaflow.common.errorcode.RuntimeErrors;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.model.graph.edge.EdgeDirection;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.state.data.DataType;
import org.apache.geaflow.state.data.OneDegreeGraph;
import org.apache.geaflow.state.pushdown.filter.FilterType;
import org.apache.geaflow.state.pushdown.filter.IFilter;
import org.apache.geaflow.state.pushdown.limit.IEdgeLimit;

public class LimitFilter extends BaseGraphFilter {

    protected long inCounter;
    protected long outCounter;
    protected IGraphFilter filter;

    LimitFilter(IGraphFilter filter, IEdgeLimit limit) {
        this.filter = filter;
        inCounter = limit.inEdgeLimit();
        outCounter = limit.outEdgeLimit();
    }

    @Override
    public DataType dateType() {
        return DataType.OTHER;
    }

    @Override
    public boolean filterEdge(IEdge edge) {
        if (!filter.filterEdge(edge)) {
            return false;
        }
        if (edge.getDirect() == EdgeDirection.OUT && outCounter-- > 0) {
            return true;
        } else {
            return edge.getDirect() == EdgeDirection.IN && inCounter-- > 0;
        }
    }

    @Override
    public boolean filterVertex(IVertex vertex) {
        return filter.filterVertex(vertex);
    }

    @Override
    public boolean filterOneDegreeGraph(OneDegreeGraph oneDegreeGraph) {
        return filter.filterOneDegreeGraph(oneDegreeGraph);
    }

    @Override
    public boolean dropAllRemaining() {
        return (outCounter <= 0 && inCounter <= 0) || filter.dropAllRemaining();
    }

    @Override
    public String toString() {
        return String.format("Limit(%d, %d)", inCounter, outCounter);
    }

    @Override
    public FilterType getFilterType() {
        return filter.getFilterType();
    }

    @Override
    public IGraphFilter and(IGraphFilter other) {
        throw new GeaflowRuntimeException(RuntimeErrors.INST.unsupportedError());
    }

    @Override
    public IGraphFilter or(IGraphFilter other) {
        throw new GeaflowRuntimeException(RuntimeErrors.INST.unsupportedError());
    }

    @Override
    public boolean contains(FilterType type) {
        return filter.contains(type);
    }

    @Override
    public IGraphFilter retrieve(FilterType type) {
        return filter.retrieve(type);
    }

    @Override
    public IGraphFilter clone() {
        throw new GeaflowRuntimeException(RuntimeErrors.INST.unsupportedError());
    }

    @Override
    public IFilter getFilter() {
        return filter.getFilter();
    }
}
