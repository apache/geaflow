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

package org.apache.geaflow.store.data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.vertex.IVertex;

public class GraphWriteMultiVersionedBuffer<K, VV, EV> implements Serializable {

    private final int thresholdSize;
    private volatile int size;
    private volatile boolean isFlushing;
    private Map<K, Map<Long, List<IEdge<K, EV>>>> vertexId2Edges;
    private Map<K, Map<Long, IVertex<K, VV>>> vertexId2Vertex;

    public GraphWriteMultiVersionedBuffer(int thresholdSize) {
        this.size = 0;
        this.isFlushing = false;
        this.thresholdSize = thresholdSize;
        this.vertexId2Edges = new ConcurrentHashMap<>();
        this.vertexId2Vertex = new ConcurrentHashMap<>();
    }

    public void addEdge(long version, IEdge<K, EV> edge) {
        K srcId = edge.getSrcId();
        Map<Long, List<IEdge<K, EV>>> map = vertexId2Edges.computeIfAbsent(srcId,
            k -> new HashMap<>());
        List<IEdge<K, EV>> edges = map.computeIfAbsent(version, k -> new ArrayList<>());
        edges.add(edge);
        this.size++;
    }

    public void addVertex(long version, IVertex<K, VV> vertex) {
        K id = vertex.getId();
        Map<Long, IVertex<K, VV>> map = vertexId2Vertex.computeIfAbsent(id, k -> new HashMap<>());
        map.put(version, vertex);
        this.size++;
    }

    public IVertex<K, VV> getVertex(long version, K sid) {
        Map<Long, IVertex<K, VV>> map = vertexId2Vertex.get(sid);
        IVertex<K, VV> vertex = null;
        if (map != null) {
            vertex = map.get(version);
        }
        return vertex;
    }

    public List<IEdge<K, EV>> getEdges(long version, K sid) {
        Map<Long, List<IEdge<K, EV>>> map = vertexId2Edges.get(sid);
        List<IEdge<K, EV>> list = new ArrayList<>();
        if (map != null) {
            list = map.getOrDefault(version, new ArrayList<>());
        }
        return list;
    }

    public Map<K, Map<Long, List<IEdge<K, EV>>>> getVertexId2Edges() {
        return vertexId2Edges;
    }

    public Map<K, Map<Long, IVertex<K, VV>>> getVertexId2Vertex() {
        return vertexId2Vertex;
    }

    public void setFlushing() {
        isFlushing = true;
    }

    public boolean isFlushing() {
        return isFlushing;
    }

    public boolean needFlush() {
        return size > thresholdSize;
    }

    public int getSize() {
        return size;
    }

    public void clear() {
        this.vertexId2Vertex.clear();
        this.vertexId2Edges.clear();
        this.size = 0;
        this.isFlushing = false;
    }
}
