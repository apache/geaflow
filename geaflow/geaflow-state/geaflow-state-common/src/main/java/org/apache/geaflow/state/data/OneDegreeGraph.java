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

package org.apache.geaflow.state.data;

import java.io.Serializable;
import org.apache.geaflow.common.iterator.CloseableIterator;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.vertex.IVertex;

public class OneDegreeGraph<K, VV, EV> implements Serializable {

    private IVertex<K, VV> vertex;
    protected CloseableIterator<IEdge<K, EV>> edgeIterator;
    protected K key;

    public OneDegreeGraph(K key, IVertex<K, VV> vertex, CloseableIterator<IEdge<K, EV>> edgeIterator) {
        this.key = key;
        this.vertex = vertex;
        this.edgeIterator = edgeIterator;
    }

    public K getKey() {
        return key;
    }

    public IVertex<K, VV> getVertex() {
        return vertex;
    }

    public CloseableIterator<IEdge<K, EV>> getEdgeIterator() {
        return edgeIterator;
    }
}

