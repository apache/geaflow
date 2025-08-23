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

package org.apache.geaflow.operator.impl.graph.algo.vc.context.statical;

import org.apache.geaflow.api.graph.function.vc.base.VertexCentricFunction.VertexQuery;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.state.GraphState;
import org.apache.geaflow.state.pushdown.filter.IFilter;
import org.apache.geaflow.utils.keygroup.KeyGroup;

public class StaticVertexQueryImpl<K, VV, EV> implements VertexQuery<K, VV> {

    private K vertexId;
    protected GraphState<K, VV, EV> graphState;
    protected KeyGroup keyGroup;

    public StaticVertexQueryImpl(K vertexId, GraphState<K, VV, EV> graphState) {
        this.vertexId = vertexId;
        this.graphState = graphState;
    }

    public StaticVertexQueryImpl(K vertexId, GraphState<K, VV, EV> graphState, KeyGroup keyGroup) {
        this.vertexId = vertexId;
        this.graphState = graphState;
        this.keyGroup = keyGroup;
    }

    @Override
    public VertexQuery<K, VV> withId(K vertexId) {
        this.vertexId = vertexId;
        return this;
    }

    @Override
    public IVertex<K, VV> get() {
        return graphState.staticGraph().V().query(vertexId).get();
    }

    @Override
    public IVertex<K, VV> get(IFilter vertexFilter) {
        return graphState.staticGraph().V().query(vertexId).by(vertexFilter).get();
    }
}
