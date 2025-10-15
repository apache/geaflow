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

package org.apache.geaflow.state.graph;

import java.util.List;
import java.util.Map;
import org.apache.geaflow.common.iterator.CloseableIterator;
import org.apache.geaflow.common.tuple.Tuple;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.state.data.OneDegreeGraph;
import org.apache.geaflow.state.pushdown.IStatePushDown;

/**
 * This interface describes the static graph traits.
 */
public interface StaticGraphTrait<K, VV, EV> {

    /**
     * Add the edge to the static graph.
     */
    void addEdge(IEdge<K, EV> edge);

    /**
     * Add the vertex to the static graph.
     */
    void addVertex(IVertex<K, VV> vertex);

    /**
     * Fetch the vertex according to the id and pushdown condition.
     */
    IVertex<K, VV> getVertex(K sid, IStatePushDown pushdown);

    /**
     * Fetch the edges according to the id and pushdown condition.
     */
    List<IEdge<K, EV>> getEdges(K sid, IStatePushDown pushdown);

    /**
     * Fetch the one degree graph according to the id and pushdown condition.
     */
    OneDegreeGraph<K, VV, EV> getOneDegreeGraph(K sid, IStatePushDown pushdown);

    /**
     * Fetch the iterator of the ids of all graph vertices.
     */
    CloseableIterator<K> vertexIDIterator();

    /**
     * Fetch the iterator of the ids of all graph vertices by pushdown condition.
     */
    CloseableIterator<K> vertexIDIterator(IStatePushDown pushDown);

    /**
     * Fetch the iterator of the graph vertices according to the pushdown condition.
     */
    CloseableIterator<IVertex<K, VV>> getVertexIterator(IStatePushDown pushdown);

    /**
     * Fetch the iterator of some vertices according to the ids and pushdown condition.
     */
    CloseableIterator<IVertex<K, VV>> getVertexIterator(List<K> keys, IStatePushDown pushdown);

    /**
     * Fetch the iterator of graph edges according to the pushdown condition.
     */
    CloseableIterator<IEdge<K, EV>> getEdgeIterator(IStatePushDown pushdown);

    /**
     * Fetch the iterator of the graph edges according to the ids and pushdown condition.
     */
    CloseableIterator<IEdge<K, EV>> getEdgeIterator(List<K> keys, IStatePushDown pushdown);

    /**
     * Fetch the iterator of one degree graph according to the pushdown condition.
     */
    CloseableIterator<OneDegreeGraph<K, VV, EV>> getOneDegreeGraphIterator(IStatePushDown pushdown);

    /**
     * Fetch the iterator of one degree graph according to the ids and pushdown condition.
     */
    CloseableIterator<OneDegreeGraph<K, VV, EV>> getOneDegreeGraphIterator(List<K> keys,
                                                                           IStatePushDown pushdown);


    /**
     * Fetch the project result of edges according to the pushdown condition.
     */
    <R> CloseableIterator<Tuple<K, R>> getEdgeProjectIterator(
        IStatePushDown<K, IEdge<K, EV>, R> pushdown);

    /**
     * Fetch the project result of edges according to the ids and pushdown condition.
     */
    <R> CloseableIterator<Tuple<K, R>> getEdgeProjectIterator(List<K> keys,
                                                              IStatePushDown<K, IEdge<K, EV>, R> pushdown);

    /**
     * Fetch the aggregated results according to the pushdown condition.
     */
    Map<K, Long> getAggResult(IStatePushDown pushdown);

    /**
     * Fetch the aggregated results according to the ids and pushdown condition.
     */
    Map<K, Long> getAggResult(List<K> keys, IStatePushDown pushdown);
}
