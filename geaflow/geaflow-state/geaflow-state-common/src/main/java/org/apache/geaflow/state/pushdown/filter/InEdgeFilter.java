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

package org.apache.geaflow.state.pushdown.filter;

import org.apache.geaflow.model.graph.edge.EdgeDirection;
import org.apache.geaflow.model.graph.edge.IEdge;

public class InEdgeFilter<K, EV> implements IEdgeFilter<K, EV> {

    private static final InEdgeFilter inEdgeFilter = new InEdgeFilter();

    @Override
    public boolean filter(IEdge<K, EV> value) {
        return value.getDirect() == EdgeDirection.IN;
    }

    @Override
    public FilterType getFilterType() {
        return FilterType.IN_EDGE;
    }

    public static <K, EV> InEdgeFilter<K, EV> getInstance() {
        return inEdgeFilter;
    }

    @Override
    public String toString() {
        return getFilterType().name();
    }
}
