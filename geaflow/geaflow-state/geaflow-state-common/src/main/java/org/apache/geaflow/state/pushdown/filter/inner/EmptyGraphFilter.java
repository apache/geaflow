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

import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.state.data.DataType;
import org.apache.geaflow.state.data.OneDegreeGraph;
import org.apache.geaflow.state.pushdown.filter.FilterType;

public class EmptyGraphFilter extends BaseGraphFilter {

    private static final EmptyGraphFilter filter = new EmptyGraphFilter();

    public static EmptyGraphFilter of() {
        return filter;
    }

    @Override
    public IGraphFilter and(IGraphFilter other) {
        return other;
    }

    @Override
    public IGraphFilter or(IGraphFilter other) {
        return other;
    }

    @Override
    public boolean filterEdge(IEdge edge) {
        return true;
    }

    @Override
    public boolean filterVertex(IVertex vertex) {
        return true;
    }

    @Override
    public boolean filterOneDegreeGraph(OneDegreeGraph oneDegreeGraph) {
        return true;
    }

    @Override
    public DataType dateType() {
        return DataType.OTHER;
    }

    @Override
    public FilterType getFilterType() {
        return FilterType.EMPTY;
    }
}
