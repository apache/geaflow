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

package org.apache.geaflow.ai.graph;

import org.apache.geaflow.ai.graph.io.Edge;
import org.apache.geaflow.ai.graph.io.MemoryGraph;
import org.apache.geaflow.ai.graph.io.Vertex;

public class MemoryMutableGraph implements MutableGraph {

    private final MemoryGraph graph;

    public MemoryMutableGraph(MemoryGraph graph) {
        this.graph = graph;
    }

    @Override
    public int removeVertex(String label, String id) {
        return this.graph.removeVertex(label, id);
    }

    @Override
    public int updateVertex(Vertex newVertex) {
        return this.graph.updateVertex(newVertex);
    }

    @Override
    public int addVertex(Vertex newVertex) {
        return this.graph.addVertex(newVertex);
    }

    @Override
    public int removeEdge(Edge edge) {
        return this.graph.removeEdge(edge);
    }

    @Override
    public int addEdge(Edge newEdge) {
        return this.graph.addEdge(newEdge);
    }
}
