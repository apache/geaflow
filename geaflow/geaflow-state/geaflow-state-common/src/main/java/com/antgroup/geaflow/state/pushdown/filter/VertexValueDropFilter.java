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

package com.antgroup.geaflow.state.pushdown.filter;

import com.antgroup.geaflow.model.graph.vertex.IVertex;

public class VertexValueDropFilter<K, VV> implements IVertexFilter<K, VV> {

    private static final VertexValueDropFilter filter = new VertexValueDropFilter();

    public static <K, VV> VertexValueDropFilter<K, VV> getInstance() {
        return filter;
    }

    @Override
    public boolean filter(IVertex<K, VV> value) {
        value.withValue(null);
        return true;
    }

    @Override
    public FilterType getFilterType() {
        return FilterType.VERTEX_VALUE_DROP;
    }

    @Override
    public String toString() {
        return getFilterType().name();
    }
}
