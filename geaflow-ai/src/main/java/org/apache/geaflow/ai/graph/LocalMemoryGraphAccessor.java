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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.geaflow.ai.graph.io.*;

public class LocalMemoryGraphAccessor implements GraphAccessor {

    private final MemoryGraph graph;

    public LocalMemoryGraphAccessor(ClassLoader classLoader, String resourcePath, Long limit,
                                    Function<Vertex, Vertex> vertexMapper,
                                    Function<Edge, Edge> edgeMapper) {
        try {
            this.graph = GraphFileReader.getGraph(classLoader, resourcePath, limit,
                    vertexMapper, edgeMapper);
        } catch (Throwable e) {
            throw new RuntimeException("Init local graph error", e);
        }
    }

    public LocalMemoryGraphAccessor(MemoryGraph memoryGraph) {
        this.graph = memoryGraph;
    }

    @Override
    public GraphSchema getGraphSchema() {
        return graph.getGraphSchema();
    }

    @Override
    public GraphVertex getVertex(String label, String id) {
        Vertex innerVertex = graph.getVertex(label, id);
        if (innerVertex == null) {
            return null;
        }
        return new GraphVertex(innerVertex);
    }

    @Override
    public List<GraphEdge> getEdge(String label, String src, String dst) {
        List<Edge> innerEdges = graph.getEdge(label, src, dst);
        if (innerEdges == null) {
            return Collections.emptyList();
        }
        return innerEdges.stream().map(GraphEdge::new).collect(Collectors.toList());
    }

    @Override
    public Iterator<GraphVertex> scanVertex() {
        return new GraphVertexIterator(graph.scanVertex());
    }

    @Override
    public Iterator<GraphEdge> scanEdge(GraphVertex vertex) {
        return new GraphEdgeIterator(graph.scanEdge(vertex.getVertex()));
    }

    @Override
    public List<GraphEntity> expand(GraphEntity entity) {
        List<GraphEntity> results = new ArrayList<>();
        if (entity instanceof GraphVertex) {
            Iterator<Edge> iterator = graph.scanEdge(((GraphVertex) entity).getVertex());
            while (iterator.hasNext()) {
                results.add(new GraphEdge(iterator.next()));
            }
        } else if (entity instanceof GraphEdge) {
            GraphEdge graphEdge = (GraphEdge) entity;
            Vertex srcVertex = graph.getVertex(null, graphEdge.getEdge().getSrcId());
            Vertex dstVertex = graph.getVertex(null, graphEdge.getEdge().getDstId());
            if (srcVertex != null) {
                results.add(new GraphVertex(srcVertex));
            }
            if (dstVertex != null) {
                results.add(new GraphVertex(dstVertex));
            }
        }
        return results;
    }

    public MemoryMutableGraph getMutableGraph() {
        return new MemoryMutableGraph(graph);
    }

    @Override
    public GraphAccessor copy() {
        return this;
    }

    @Override
    public String getType() {
        return this.getClass().getSimpleName();
    }


    private static class GraphVertexIterator implements Iterator<GraphVertex> {

        private final Iterator<Vertex> vertexIterator;

        public GraphVertexIterator(Iterator<Vertex> vertexIterator) {
            this.vertexIterator = vertexIterator;
        }

        @Override
        public boolean hasNext() {
            return vertexIterator.hasNext();
        }

        @Override
        public GraphVertex next() {
            Vertex nextVertex = vertexIterator.next();
            return new GraphVertex(nextVertex);
        }
    }

    private static class GraphEdgeIterator implements Iterator<GraphEdge> {
        private final Iterator<Edge> delegate;

        public GraphEdgeIterator(Iterator<Edge> delegate) {
            this.delegate = delegate;
        }

        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        @Override
        public GraphEdge next() {
            Edge edge = delegate.next();
            return new GraphEdge(edge);
        }
    }

}
