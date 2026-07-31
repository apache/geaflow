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
import java.util.concurrent.atomic.AtomicLong;
import org.apache.geaflow.ai.common.ErrorCode;
import org.apache.geaflow.ai.graph.Graph;

public class MemoryGraph implements Graph {

    public GraphSchema graphSchema;
    public Map<String, EntityGroup> entities;

    /**
     * Bumped on every content or schema change so that derived structures such as verbalization
     * caches and keyword indexes can detect that they went stale, even when the graph is mutated
     * directly instead of through a server API.
     */
    private final AtomicLong version = new AtomicLong();

    /**
     * Bumped only by vertex and schema changes. Structures that depend on vertices alone, such as
     * the global keyword index, can watch this instead of {@link #version} and stay valid across
     * edge writes.
     */
    private final AtomicLong vertexVersion = new AtomicLong();

    public MemoryGraph(GraphSchema graphSchema, Map<String, EntityGroup> entities) {
        this.graphSchema = graphSchema;
        this.entities = entities;
    }

    public long getVersion() {
        return version.get();
    }

    public long getVertexVersion() {
        return vertexVersion.get();
    }

    /**
     * Registers a vertex schema and its empty entity group. Callers are expected to have validated
     * the label first. Advances the vertex version, since vertex verbalization is schema driven.
     */
    public void registerVertexSchema(VertexSchema vertexSchema) {
        graphSchema.addVertex(vertexSchema);
        entities.put(vertexSchema.getLabel(), new VertexGroup(vertexSchema, new ArrayList<>()));
        bumpVersion();
    }

    /**
     * Registers an edge schema and its empty entity group. Only the general version is advanced: an
     * edge schema cannot change how an existing vertex is verbalized, so vertex only derived
     * structures stay valid.
     */
    public void registerEdgeSchema(EdgeSchema edgeSchema) {
        graphSchema.addEdge(edgeSchema);
        entities.put(edgeSchema.getLabel(), new EdgeGroup(edgeSchema, new ArrayList<>()));
        bumpEdgeVersion();
    }

    private void bumpVersion() {
        version.incrementAndGet();
        vertexVersion.incrementAndGet();
    }

    private void bumpEdgeVersion() {
        version.incrementAndGet();
    }

    @Override
    public GraphSchema getGraphSchema() {
        return graphSchema;
    }

    public void setGraphSchema(GraphSchema graphSchema) {
        this.graphSchema = graphSchema;
        bumpVersion();
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
            if (vg == null) {
                return null;
            }
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
        return bumped(vertexGroup.removeVertex(id));
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
        return bumped(vertexGroup.updateVertex(newVertex));
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
        return bumped(vertexGroup.addVertex(newVertex));
    }

    @Override
    public List<Edge> getEdge(String label, String src, String dst) {
        EdgeGroup eg = (EdgeGroup) getEntity(label);
        if (eg == null) {
            return Collections.emptyList();
        }
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
        return edgeBumped(edgeGroup.removeEdge(edge));
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
        return edgeBumped(edgeGroup.addEdge(newEdge));
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

    /**
     * Marks the graph as changed and passes the mutation result through. The version is bumped even
     * for failed mutations: over invalidation is cheap, a missed invalidation is a correctness bug.
     */
    private int bumped(int mutationResult) {
        bumpVersion();
        return mutationResult;
    }

    /**
     * Same as {@link #bumped} but only advances the general version, leaving vertex only derived
     * structures valid.
     */
    private int edgeBumped(int mutationResult) {
        bumpEdgeVersion();
        return mutationResult;
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
