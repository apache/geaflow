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
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.state.data.OneDegreeGraph;
import org.apache.geaflow.state.pushdown.filter.FilterType;
import org.apache.geaflow.state.pushdown.filter.IFilter;

public interface IGraphFilter extends IFilter<Object> {

    /**
     * An edge will be filtered if return is false.
     */
    boolean filterEdge(IEdge edge);

    /**
     * A Vertex will be filtered if return is false.
     */
    boolean filterVertex(IVertex vertex);

    /**
     * A oneDegreeGraph will be filtered if return is false.
     */
    boolean filterOneDegreeGraph(OneDegreeGraph oneDegreeGraph);

    /**
     * If this returns true, the edge scan will terminate.
     */
    boolean dropAllRemaining();

    /**
     * FilterTypes.
     */
    FilterType getFilterType();

    /**
     * Union other filter with AND logic.
     */
    IGraphFilter and(IGraphFilter other);

    /**
     * Union other filter with or logic.
     */
    IGraphFilter or(IGraphFilter other);

    /**
     * Check filter whether contains some type.
     * Check itself If the filter is simple.
     */
    boolean contains(FilterType type);

    /**
     * retrieve target filter.
     * return itself If the filter is simple.
     */
    IGraphFilter retrieve(FilterType type);

    /**
     * clone the filter.
     */
    IGraphFilter clone();

    /**
     * get the inner filter, only direct filter converter support.
     * return inner filter.
     */
    default IFilter getFilter() {
        throw new GeaflowRuntimeException(RuntimeErrors.INST.unsupportedError());
    }
}
