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

package org.apache.geaflow.operator.impl.graph.algo.vc.context.dynamic;


import java.util.List;
import java.util.Map;
import org.apache.geaflow.api.graph.function.vc.IncVertexCentricTraversalFunction.TraversalGraphSnapShot;
import org.apache.geaflow.api.graph.function.vc.IncVertexCentricTraversalFunction.TraversalHistoricalGraph;
import org.apache.geaflow.api.graph.function.vc.base.IncVertexCentricFunction.HistoricalGraph;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.state.pushdown.filter.IVertexFilter;

public class TraversalIncHistoricalGraph<K, VV, EV> implements HistoricalGraph<K, VV, EV>
    , TraversalHistoricalGraph<K, VV, EV> {

    private final IncHistoricalGraph<K, VV, EV> historicalGraph;

    public TraversalIncHistoricalGraph(IncHistoricalGraph<K, VV, EV> historicalGraph) {
        this.historicalGraph = historicalGraph;
    }

    @Override
    public Long getLatestVersionId() {
        return historicalGraph.getLatestVersionId();
    }

    @Override
    public List<Long> getAllVersionIds() {
        return historicalGraph.getAllVersionIds();
    }

    @Override
    public Map<Long, IVertex<K, VV>> getAllVertex() {
        return historicalGraph.getAllVertex();
    }

    @Override
    public Map<Long, IVertex<K, VV>> getAllVertex(List<Long> versions) {
        return historicalGraph.getAllVertex(versions);
    }

    @Override
    public Map<Long, IVertex<K, VV>> getAllVertex(List<Long> versions, IVertexFilter<K, VV> vertexFilter) {
        return historicalGraph.getAllVertex(versions, vertexFilter);
    }

    @Override
    public TraversalGraphSnapShot<K, VV, EV> getSnapShot(long version) {
        return new TraversalIncGraphSnapShot<>(historicalGraph.vertexId, version, historicalGraph.graphState);
    }
}
