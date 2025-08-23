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
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.state.data.OneDegreeGraph;
import org.apache.geaflow.state.pushdown.filter.FilterType;

public class OrGraphFilter extends BaseComposeGraphFilter {

    public OrGraphFilter(List<IGraphFilter> childrenFilters) {
        super(childrenFilters);
    }

    @Override
    public IGraphFilter or(IGraphFilter filter) {
        if (filter.getFilterType() == FilterType.OR) {
            childrenFilters.addAll(((OrGraphFilter) (filter)).getFilterList());
            return this;
        } else if (filter.getFilterType() == FilterType.EMPTY) {
            return this;
        }
        childrenFilters.add(filter);
        return this;
    }

    /**
     * An edge will be filtered if return is false.
     *
     * @param edge
     */
    @Override
    public boolean filterEdge(IEdge edge) {
        if (childrenFilters.isEmpty()) {
            return true;
        }
        for (IGraphFilter filter : childrenFilters) {
            if (filter.filterEdge(edge)) {
                return true;
            }
        }
        return false;
    }

    /**
     * A Vertex will be filtered if return is false.
     *
     * @param vertex
     */
    @Override
    public boolean filterVertex(IVertex vertex) {
        if (childrenFilters.isEmpty()) {
            return true;
        }
        for (IGraphFilter filter : childrenFilters) {
            if (filter.filterVertex(vertex)) {
                return true;
            }
        }
        return false;
    }

    /**
     * A oneDegreeGraph will be filtered if return is false.
     */
    @Override
    public boolean filterOneDegreeGraph(OneDegreeGraph oneDegreeGraph) {
        if (childrenFilters.isEmpty()) {
            return true;
        }
        for (IGraphFilter filter : childrenFilters) {
            if (filter.filterOneDegreeGraph(oneDegreeGraph)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean contains(FilterType type) {
        return false;
    }

    @Override
    public IGraphFilter retrieve(FilterType type) {
        return null;
    }

    @Override
    public OrGraphFilter clone() {
        return new OrGraphFilter(cloneFilterList());
    }

    @Override
    public FilterType getFilterType() {
        return FilterType.OR;
    }
}
