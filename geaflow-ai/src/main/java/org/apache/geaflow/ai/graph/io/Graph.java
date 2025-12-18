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
import org.apache.geaflow.ai.graph.GraphVertex;

public class Graph {

    public GraphSchema graphSchema;
    public Map<String, EntityGroup> entities;

    public Graph(GraphSchema graphSchema, Map<String, EntityGroup> entities) {
        this.graphSchema = graphSchema;
        this.entities = entities;
    }

    public GraphSchema getGraphSchema() {
        return graphSchema;
    }

    public void setGraphSchema(GraphSchema graphSchema) {
        this.graphSchema = graphSchema;
    }

    public EntityGroup getEntity(String entityName) {
        return entities.get(entityName);
    }

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

    public Edge getEdge(String label, String src, String dst) {
        EdgeGroup eg = (EdgeGroup) getEntity(label);
        return eg.getEdge(src, dst);
    }

    public Iterator<Edge> scanEdge(GraphVertex vertex) {
        List<Iterator<Edge>> iterators = new ArrayList<>();
        for (EntityGroup entityGroup : this.entities.values()) {
            if (entityGroup instanceof EdgeGroup) {
                iterators.add(((EdgeGroup) entityGroup).getOutEdges(vertex.getVertex().getId()).iterator());
                iterators.add(((EdgeGroup) entityGroup).getInEdges(vertex.getVertex().getId()).iterator());
            }
        }
        return new CompositeIterator<>(iterators);
    }

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
