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

package org.apache.geaflow.ai.graph.io;

import java.util.*;
import org.apache.geaflow.ai.common.ErrorCode;
import org.apache.geaflow.ai.graph.Graph;

public class MemoryGraph implements Graph {

    public GraphSchema graphSchema;
    public Map<String, EntityGroup> entities;

    public MemoryGraph(GraphSchema graphSchema, Map<String, EntityGroup> entities) {
        this.graphSchema = graphSchema;
        this.entities = entities;
    }

    @Override
    public GraphSchema getGraphSchema() {
        return graphSchema;
    }

    public void setGraphSchema(GraphSchema graphSchema) {
        this.graphSchema = graphSchema;
    }

    private EntityGroup getEntity(String entityName) {
        return entities.get(entityName);
    }

    @Override
    public Vertex getVertex(String label, String id) {
        if (label == null) {
            for (VertexSchema schema : getGraphSchema().getVertexSchemaList()) {
                Vertex res = getVertex(schema.getLabel(), id);
                if (res != null) {
                    return res;
                }
            }
        } else {
            VertexGroup vg = (VertexGroup) getEntity(label);
            return vg.getVertex(id);
        }
        return null;
    }

    @Override
    public int removeVertex(String label, String id) {
        EntityGroup vg = entities.get(label);
        if (vg == null) {
            return ErrorCode.GRAPH_ENTITY_GROUP_NOT_EXISTS;
        }
        if (!(vg instanceof VertexGroup)) {
            return ErrorCode.GRAPH_ENTITY_GROUP_NOT_MATCH;
        }
        VertexGroup vertexGroup = (VertexGroup) vg;
        return vertexGroup.removeVertex(id);
    }

    @Override
    public int updateVertex(Vertex newVertex) {
        String label = newVertex.getLabel();
        EntityGroup vg = entities.get(label);
        if (vg == null) {
            return ErrorCode.GRAPH_ENTITY_GROUP_NOT_EXISTS;
        }
        if (!(vg instanceof VertexGroup)) {
            return ErrorCode.GRAPH_ENTITY_GROUP_NOT_MATCH;
        }
        VertexGroup vertexGroup = (VertexGroup) vg;
        return vertexGroup.updateVertex(newVertex);
    }

    @Override
    public int addVertex(Vertex newVertex) {
        String label = newVertex.getLabel();
        EntityGroup vg = entities.get(label);
        if (vg == null) {
            return ErrorCode.GRAPH_ENTITY_GROUP_NOT_EXISTS;
        }
        if (!(vg instanceof VertexGroup)) {
            return ErrorCode.GRAPH_ENTITY_GROUP_NOT_MATCH;
        }
        VertexGroup vertexGroup = (VertexGroup) vg;
        return vertexGroup.addVertex(newVertex);
    }

    @Override
    public List<Edge> getEdge(String label, String src, String dst) {
        EdgeGroup eg = (EdgeGroup) getEntity(label);
        return eg.getEdge(src, dst);
    }

    @Override
    public int removeEdge(Edge edge) {
        String label = edge.getLabel();
        EntityGroup vg = entities.get(label);
        if (vg == null) {
            return ErrorCode.GRAPH_ENTITY_GROUP_NOT_EXISTS;
        }
        if (!(vg instanceof EdgeGroup)) {
            return ErrorCode.GRAPH_ENTITY_GROUP_NOT_MATCH;
        }
        EdgeGroup edgeGroup = (EdgeGroup) vg;
        return edgeGroup.removeEdge(edge);
    }

    @Override
    public int addEdge(Edge newEdge) {
        String label = newEdge.getLabel();
        EntityGroup vg = entities.get(label);
        if (vg == null) {
            return ErrorCode.GRAPH_ENTITY_GROUP_NOT_EXISTS;
        }
        if (!(vg instanceof EdgeGroup)) {
            return ErrorCode.GRAPH_ENTITY_GROUP_NOT_MATCH;
        }
        EdgeGroup edgeGroup = (EdgeGroup) vg;
        return edgeGroup.addEdge(newEdge);
    }

    @Override
    public Iterator<Edge> scanEdge(Vertex vertex) {
        List<Iterator<Edge>> iterators = new ArrayList<>();
        for (EntityGroup entityGroup : this.entities.values()) {
            if (entityGroup instanceof EdgeGroup) {
                iterators.add(((EdgeGroup) entityGroup).getOutEdges(vertex.getId()).iterator());
                iterators.add(((EdgeGroup) entityGroup).getInEdges(vertex.getId()).iterator());
            }
        }
        return new CompositeIterator<>(iterators);
    }

    @Override
    public Iterator<Vertex> scanVertex() {
        List<Iterator<Vertex>> iterators = new ArrayList<>();
        for (EntityGroup entityGroup : this.entities.values()) {
            if (entityGroup instanceof VertexGroup) {
                iterators.add(((VertexGroup) entityGroup).getVertices().iterator());
            }
        }
        return new CompositeIterator<>(iterators);
    }

    static class CompositeIterator<T> implements Iterator<T> {

        private final List<Iterator<T>> iterators;
        private int currentIndex = 0;

        public CompositeIterator(List<Iterator<T>> iterators) {
            this.iterators = iterators;
        }

        @Override
        public boolean hasNext() {
            while (currentIndex < iterators.size()) {
                if (iterators.get(currentIndex).hasNext()) {
                    return true;
                }
                currentIndex++;
            }
            return false;
        }

        @Override
        public T next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return iterators.get(currentIndex).next();
        }
    }
}
