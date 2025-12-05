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
import org.apache.geaflow.ai.graph.io.GraphSchema;
import org.apache.geaflow.ai.graph.io.Vertex;

import java.util.ArrayList;
import java.util.Iterator;

public class EmptyGraphAccessor implements GraphAccessor {

    @Override
    public GraphSchema getGraphSchema() {
        return null;
    }

    @Override
    public GraphVertex getVertex(String label, String id) {
        return null;
    }

    @Override
    public GraphEdge getEdge(String label, String src, String dst) {
        return null;
    }

    @Override
    public Iterator<GraphVertex> scanVertex() {
        return new ArrayList<GraphVertex>().iterator();
    }

    @Override
    public Iterator<GraphEdge> scanEdge(GraphVertex vertex) {
        return new ArrayList<GraphEdge>().iterator();
    }

    @Override
    public GraphAccessor copy() {
        return new EmptyGraphAccessor();
    }

    @Override
    public String getType() {
        return EmptyGraphAccessor.class.getSimpleName();
    }
}
