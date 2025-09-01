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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.geaflow.common.iterator.CloseableIterator;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.state.data.DataType;
import org.apache.geaflow.state.data.OneDegreeGraph;
import org.apache.geaflow.state.pushdown.IStatePushDown;

/**
 * This interface describes the dynamic graph traits.
 */
public interface DynamicGraphTrait<K, VV, EV> {

    /**
     * Add the edge and its corresponding version.
     */
    void addEdge(long version, IEdge<K, EV> edge);

    /**
     * Add the vertex and its corresponding version.
     */
    void addVertex(long version, IVertex<K, VV> vertex);

    /**
     * Fetch the vertex according to the version, id and pushdown condition.
     */
    IVertex<K, VV> getVertex(long version, K sid, IStatePushDown pushdown);

    /**
     * Fetch the edge list according to the version, id and pushdown condition.
     */
    List<IEdge<K, EV>> getEdges(long version, K sid, IStatePushDown pushdown);

    /**
     * Fetch the one degree graph according to the version, id and pushdown condition.
     */
    OneDegreeGraph<K, VV, EV> getOneDegreeGraph(long version, K sid, IStatePushDown pushdown);

    /**
     * Fetch the iterator of the ids of all graph vertices.
     */
    CloseableIterator<K> vertexIDIterator();

    /**
     * Fetch the iterator of the ids of all graph vertices by pushdown condition.
     */
    CloseableIterator<K> vertexIDIterator(long version, IStatePushDown pushdown);

    /**
     * Fetch the iterator of the graph vertices according to the version and pushdown condition.
     */
    CloseableIterator<IVertex<K, VV>> getVertexIterator(long version, IStatePushDown pushdown);

    /**
     * Fetch the iterator of some vertices according to the version, ids and pushdown condition.
     */
    CloseableIterator<IVertex<K, VV>> getVertexIterator(long version, List<K> keys, IStatePushDown pushdown);

    /**
     * Fetch the iterator of graph edges according to the version and pushdown condition.
     */
    CloseableIterator<IEdge<K, EV>> getEdgeIterator(long version, IStatePushDown pushdown);

    /**
     * Fetch the iterator of some edges according to the version, ids and pushdown condition.
     */
    CloseableIterator<IEdge<K, EV>> getEdgeIterator(long version, List<K> keys, IStatePushDown pushdown);

    /**
     * Fetch the iterator of one degree graph according to the version and pushdown condition.
     */
    CloseableIterator<OneDegreeGraph<K, VV, EV>> getOneDegreeGraphIterator(long version, IStatePushDown pushdown);

    /**
     * Fetch the iterator of one degree graph according to the version, ids and pushdown condition.
     */
    CloseableIterator<OneDegreeGraph<K, VV, EV>> getOneDegreeGraphIterator(long version, List<K> keys, IStatePushDown pushdown);

    /**
     * Fetch the versions of some id.
     */
    List<Long> getAllVersions(K id, DataType dataType);

    /**
     * Fetch the latest version of some id.
     */
    long getLatestVersion(K id, DataType dataType);

    /**
     * Fetch all versioned data by id and pushdown condition.
     */
    Map<Long, IVertex<K, VV>> getAllVersionData(K id, IStatePushDown pushdown, DataType dataType);

    /**
     * Fetch some specific versioned data by id and pushdown condition.
     */
    Map<Long, IVertex<K, VV>> getVersionData(K id, Collection<Long> versions,
                                             IStatePushDown pushdown, DataType dataType);
}
