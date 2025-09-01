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

import java.util.List;
import org.apache.geaflow.api.graph.function.vc.base.VertexCentricFunction.EdgeQuery;
import org.apache.geaflow.common.iterator.CloseableIterator;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.state.GraphState;
import org.apache.geaflow.state.pushdown.filter.IFilter;
import org.apache.geaflow.state.pushdown.filter.InEdgeFilter;
import org.apache.geaflow.state.pushdown.filter.OutEdgeFilter;
import org.apache.geaflow.utils.keygroup.KeyGroup;

public class StaticEdgeQueryImpl<K, VV, EV> implements EdgeQuery<K, EV> {

    protected K vId;
    private final GraphState<K, VV, EV> graphState;
    protected KeyGroup keyGroup;

    public StaticEdgeQueryImpl(K vId, GraphState<K, VV, EV> graphState) {
        this.vId = vId;
        this.graphState = graphState;
    }

    public StaticEdgeQueryImpl(K vId, GraphState<K, VV, EV> graphState, KeyGroup keyGroup) {
        this.vId = vId;
        this.graphState = graphState;
        this.keyGroup = keyGroup;
    }

    @Override
    public List<IEdge<K, EV>> getEdges() {
        return graphState.staticGraph().E().query(vId).asList();
    }

    @Override
    public List<IEdge<K, EV>> getOutEdges() {
        return graphState.staticGraph().E().query(vId).by(OutEdgeFilter.getInstance()).asList();
    }


    @Override
    public List<IEdge<K, EV>> getInEdges() {
        return graphState.staticGraph().E().query(vId).by(InEdgeFilter.getInstance()).asList();
    }


    @Override
    public CloseableIterator<IEdge<K, EV>> getEdges(IFilter edgeFilter) {
        return (CloseableIterator) graphState.staticGraph().E().query(vId).by(edgeFilter).iterator();
    }
}
