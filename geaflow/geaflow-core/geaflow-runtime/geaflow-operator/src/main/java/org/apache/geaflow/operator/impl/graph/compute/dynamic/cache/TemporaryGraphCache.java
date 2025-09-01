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

package org.apache.geaflow.operator.impl.graph.compute.dynamic.cache;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TemporaryGraphCache<K, VV, EV> {

    private static final Logger LOGGER = LoggerFactory.getLogger(TemporaryGraphCache.class);

    private final Set<K> vertexIds;
    private final Map<K, IVertex<K, VV>> vertices;
    private final Map<K, List<IEdge<K, EV>>> vertexEdges;

    public TemporaryGraphCache() {
        this.vertexIds = new HashSet<>();
        this.vertices = new HashMap<>();
        this.vertexEdges = new HashMap<>();
    }

    public void addVertex(IVertex<K, VV> vertex) {
        this.vertexIds.add(vertex.getId());
        this.vertices.put(vertex.getId(), vertex);
    }

    public IVertex<K, VV> getVertex(K vId) {
        return this.vertices.get(vId);
    }

    public void addEdge(IEdge<K, EV> edge) {
        this.vertexIds.add(edge.getSrcId());
        List<IEdge<K, EV>> edges = this.vertexEdges.getOrDefault(edge.getSrcId(),
            new ArrayList<>());
        edges.add(edge);
        this.vertexEdges.put(edge.getSrcId(), edges);
    }

    public List<IEdge<K, EV>> getEdges(K vId) {
        return this.vertexEdges.get(vId);
    }

    public Set<K> getAllEvolveVId() {
        return this.vertexIds;
    }

    public void clear() {
        this.vertexIds.clear();
        this.vertices.clear();
        this.vertexEdges.clear();
    }
}
