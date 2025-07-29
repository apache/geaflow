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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.vertex.IVertex;

public class GraphWriteBuffer<K, VV, EV> implements Serializable {

    private final int thresholdSize;
    private volatile int size;
    private volatile boolean isFlushing;
    private Map<K, List<IEdge<K, EV>>> vertexId2Edges;
    private Map<K, IVertex<K, VV>> vertexId2Vertex;

    public GraphWriteBuffer(int thresholdSize) {
        this.size = 0;
        this.isFlushing = false;
        this.thresholdSize = thresholdSize;
        this.vertexId2Edges = new ConcurrentHashMap<>();
        this.vertexId2Vertex = new ConcurrentHashMap<>();
    }

    public void addVertex(IVertex<K, VV> vertex) {
        vertexId2Vertex.put(vertex.getId(), vertex);
        this.size++;
    }

    public void addEdge(IEdge<K, EV> edge) {
        List<IEdge<K, EV>> list = vertexId2Edges.computeIfAbsent(edge.getSrcId(),
            k -> new ArrayList<>());
        list.add(edge);
        this.size++;
    }

    public void addEdges(List<IEdge<K, EV>> edges) {
        if (edges == null || edges.size() == 0) {
            return;
        }
        List<IEdge<K, EV>> list =
            vertexId2Edges.computeIfAbsent(edges.get(0).getSrcId(), k -> new ArrayList<>());
        list.addAll(edges);
        this.size += edges.size();
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

    public Map<K, List<IEdge<K, EV>>> getVertexId2Edges() {
        return vertexId2Edges;
    }

    public Map<K, IVertex<K, VV>> getVertexId2Vertex() {
        return vertexId2Vertex;
    }

    public void clear() {
        this.vertexId2Vertex.clear();
        this.vertexId2Edges.clear();
        this.size = 0;
        this.isFlushing = false;
    }
}
