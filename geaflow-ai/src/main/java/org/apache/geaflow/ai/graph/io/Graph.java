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

import org.apache.geaflow.ai.graph.GraphVertex;

import java.util.*;

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
        VertexGroup vg = (VertexGroup) getEntity(label);
        return vg.getVertex(id);
    }

    public Edge getEdge(String label, String src, String dst) {
        EdgeGroup eg = (EdgeGroup) getEntity(label);
        return eg.getEdge(src, dst);
    }

    public Iterator<Edge> scanEdge(GraphVertex vertex) {
        // 创建一个列表来保存所有的 VertexGroup
        List<Iterator<Edge>> iterators = new ArrayList<>();
        // 遍历 entities，找到所有的 VertexGroup，并获取它们的 Vertex 迭代器
        for (EntityGroup entityGroup : this.entities.values()) {
            if (entityGroup instanceof EdgeGroup) {
                iterators.add(((EdgeGroup) entityGroup).getOutEdges(vertex.getVertex().getId()).iterator());
                iterators.add(((EdgeGroup) entityGroup).getInEdges(vertex.getVertex().getId()).iterator());
            }
        }
        // 返回一个组合的迭代器
        return new CompositeIterator<>(iterators);
    }

    public Iterator<Vertex> scanVertex() {
        // 创建一个列表来保存所有的 VertexGroup
        List<Iterator<Vertex>> iterators = new ArrayList<>();
        // 遍历 entities，找到所有的 VertexGroup，并获取它们的 Vertex 迭代器
        for (EntityGroup entityGroup : this.entities.values()) {
            if (entityGroup instanceof VertexGroup) {
                iterators.add(((VertexGroup) entityGroup).getVertices().iterator());
            }
        }
        // 返回一个组合的迭代器
        return new CompositeIterator<>(iterators);
    }

    // 自定义组合迭代器
    static class CompositeIterator<T> implements Iterator<T> {

        private final List<Iterator<T>> iterators;
        private int currentIndex = 0;

        public CompositeIterator(List<Iterator<T>> iterators) {
            this.iterators = iterators;
        }

        @Override
        public boolean hasNext() {
            // 跳过已经遍历完的迭代器
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
