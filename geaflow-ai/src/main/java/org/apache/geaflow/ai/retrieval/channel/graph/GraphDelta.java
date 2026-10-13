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

package org.apache.geaflow.ai.retrieval.channel.graph;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.geaflow.ai.retrieval.codec.RetrievalModelJson;
import org.apache.geaflow.ai.retrieval.model.document.TextChunk;
import org.apache.geaflow.ai.retrieval.model.graph.EntityRef;
import org.apache.geaflow.ai.retrieval.model.graph.GraphEdgeRef;
import org.apache.geaflow.ai.retrieval.model.graph.GraphVertexRef;
import org.apache.geaflow.ai.retrieval.model.version.GraphVersion;

/** Immutable local graph changes applied on top of one parent version. */
public final class GraphDelta {

    private final GraphVersion graphVersion;
    private final GraphVersion parentVersion;
    private final List<TextChunk> chunks;
    private final List<EntityRef> entities;
    private final List<GraphVertexRef> vertices;
    private final List<GraphEdgeRef> edges;
    private final Map<String, Set<String>> aliases;
    private final GraphTombstones tombstones;
    private final String digest;

    private GraphDelta(Builder builder) {
        if (builder.graphVersion == null || builder.parentVersion == null) {
            throw new GraphViewException(GraphViewException.Code.INVALID_VIEW,
                "delta graph and parent versions are required");
        }
        if (builder.graphVersion.equals(builder.parentVersion)) {
            throw new GraphViewException(GraphViewException.Code.VERSION_CONFLICT,
                "delta version must differ from parent version");
        }
        if (!builder.graphVersion.getGraphName().equals(builder.parentVersion.getGraphName())) {
            throw new GraphViewException(GraphViewException.Code.VERSION_CONFLICT,
                "delta and parent graph names must match");
        }
        this.graphVersion = builder.graphVersion;
        this.parentVersion = builder.parentVersion;
        this.chunks = immutableValues(builder.chunks);
        this.entities = immutableValues(builder.entities);
        this.vertices = immutableValues(builder.vertices);
        this.edges = immutableValues(builder.edges);
        this.aliases = immutableAliases(builder.aliases);
        this.tombstones = new GraphTombstones(builder.deletedChunks, builder.deletedEntities,
            builder.deletedEdges, builder.deletedAliases);
        rejectConflicts();
        this.digest = digest();
    }

    private static <T> List<T> immutableValues(Map<String, T> values) {
        List<T> result = new ArrayList<>(values.values());
        result.sort((left, right) -> key(left).compareTo(key(right)));
        return Collections.unmodifiableList(result);
    }

    private static String key(Object value) {
        if (value instanceof TextChunk) {
            return ((TextChunk) value).getChunkId();
        }
        if (value instanceof EntityRef) {
            return ((EntityRef) value).getEntityId();
        }
        if (value instanceof GraphVertexRef) {
            return ((GraphVertexRef) value).getVertexId();
        }
        return ((GraphEdgeRef) value).getEdgeId();
    }

    private static Map<String, Set<String>> immutableAliases(Map<String, Set<String>> values) {
        Map<String, Set<String>> result = new TreeMap<>();
        for (Map.Entry<String, Set<String>> entry : values.entrySet()) {
            result.put(entry.getKey(), Collections.unmodifiableSet(new TreeSet<>(entry.getValue())));
        }
        return Collections.unmodifiableMap(result);
    }

    private void rejectConflicts() {
        if (overlap(tombstones.getChunks(), chunks, TextChunk::getChunkId)
            || overlap(tombstones.getEntities(), entities, EntityRef::getEntityId)
            || overlap(tombstones.getEdges(), edges, GraphEdgeRef::getEdgeId)) {
            throw new GraphViewException(GraphViewException.Code.DELTA_CONFLICT,
                "delta cannot update and delete the same record");
        }
        for (Map.Entry<String, Set<String>> entry : aliases.entrySet()) {
            for (String alias : entry.getValue()) {
                if (tombstones.deletesAlias(entry.getKey(), alias)) {
                    throw new GraphViewException(GraphViewException.Code.DELTA_CONFLICT,
                        "delta cannot update and delete alias " + alias);
                }
            }
        }
        for (EntityRef entity : entities) {
            for (String alias : entity.getAliases()) {
                if (tombstones.deletesAlias(entity.getEntityId(), alias)) {
                    throw new GraphViewException(GraphViewException.Code.DELTA_CONFLICT,
                        "entity update cannot delete alias " + alias);
                }
            }
        }
        for (EntityRef entity : entities) {
            for (String alias : entity.getAliases()) {
                if (tombstones.deletesAlias(entity.getEntityId(), alias)) {
                    throw new GraphViewException(GraphViewException.Code.DELTA_CONFLICT,
                        "entity update conflicts with alias deletion " + alias);
                }
            }
        }
        Set<String> seenVertexEntities = new LinkedHashSet<>();
        for (GraphVertexRef vertex : vertices) {
            if (!seenVertexEntities.add(vertex.getEntityId())) {
                throw new GraphViewException(GraphViewException.Code.DELTA_CONFLICT,
                    "multiple vertices map to entity " + vertex.getEntityId());
            }
        }
    }

    private static <T> boolean overlap(Set<String> deleted, List<T> values,
                                       java.util.function.Function<T, String> key) {
        for (T value : values) {
            if (deleted.contains(key.apply(value))) {
                return true;
            }
        }
        return false;
    }

    private String digest() {
        StringBuilder value = new StringBuilder();
        value.append(graphVersion.getGraphName()).append('\n').append(graphVersion.getVersion()).append('\n')
            .append(parentVersion.getVersion()).append('\n');
        for (TextChunk chunk : chunks) {
            value.append("c\u001f").append(RetrievalModelJson.toJson(chunk)).append('\n');
        }
        for (EntityRef entity : entities) {
            value.append("e\u001f").append(RetrievalModelJson.toJson(entity)).append('\n');
        }
        for (GraphVertexRef vertex : vertices) {
            value.append("v\u001f").append(RetrievalModelJson.toJson(vertex)).append('\n');
        }
        for (GraphEdgeRef edge : edges) {
            value.append("r\u001f").append(RetrievalModelJson.toJson(edge)).append('\n');
        }
        for (Map.Entry<String, Set<String>> entry : aliases.entrySet()) {
            for (String alias : entry.getValue()) {
                value.append("a\u001f").append(entry.getKey()).append('\u001f').append(alias).append('\n');
            }
        }
        appendSet(value, "dc", tombstones.getChunks());
        appendSet(value, "de", tombstones.getEntities());
        appendSet(value, "dr", tombstones.getEdges());
        appendSet(value, "da", tombstones.getAliases());
        return GraphSnapshot.sha256(value.toString());
    }

    private static void appendSet(StringBuilder value, String prefix, Set<String> values) {
        for (String item : values) {
            value.append(prefix).append("\u001f").append(item).append('\n');
        }
    }

    /** Applies this delta to exactly its parent snapshot. */
    public GraphSnapshot applyTo(GraphSnapshot parent) {
        if (parent == null || !parent.getGraphVersion().equals(parentVersion)) {
            throw new GraphViewException(GraphViewException.Code.MISSING_PARENT,
                "delta parent is not available: " + parentVersion.getVersion());
        }
        Map<String, TextChunk> chunksById = mapChunks(parent.getChunks());
        Map<String, EntityRef> entitiesById = mapEntities(parent.getEntities());
        Map<String, GraphVertexRef> verticesById = mapVertices(parent.getVertices());
        Map<String, GraphEdgeRef> edgesById = mapEdges(parent.getEdges());
        validateTombstones(chunksById, entitiesById, edgesById);
        for (String id : tombstones.getChunks()) {
            chunksById.remove(id);
        }
        for (String id : tombstones.getEdges()) {
            edgesById.remove(id);
        }
        for (String id : tombstones.getEntities()) {
            entitiesById.remove(id);
            verticesById.values().removeIf(vertex -> vertex.getEntityId().equals(id));
            edgesById.values().removeIf(edge -> edge.getSourceEntityId().equals(id)
                || edge.getTargetEntityId().equals(id));
        }
        for (EntityRef entity : entities) {
            entitiesById.put(entity.getEntityId(), entity);
        }
        for (Map.Entry<String, Set<String>> entry : aliases.entrySet()) {
            EntityRef entity = entitiesById.get(entry.getKey());
            if (entity == null) {
                throw new GraphViewException(GraphViewException.Code.BAD_REFERENCE,
                    "alias references unknown entity " + entry.getKey());
            }
            entitiesById.put(entry.getKey(), withAliases(entity, union(entity.getAliases(), entry.getValue())));
        }
        for (String aliasKey : tombstones.getAliases()) {
            int separator = aliasKey.indexOf('\u001f');
            if (separator <= 0) {
                throw new GraphViewException(GraphViewException.Code.BAD_REFERENCE, "invalid alias tombstone");
            }
            String entityId = aliasKey.substring(0, separator);
            EntityRef entity = entitiesById.get(entityId);
            if (entity != null) {
                String alias = aliasKey.substring(separator + 1);
                List<String> retained = new ArrayList<>(entity.getAliases());
                retained.remove(alias);
                entitiesById.put(entityId, withAliases(entity, retained));
            }
        }
        for (TextChunk chunk : chunks) {
            chunksById.put(chunk.getChunkId(), chunk);
        }
        for (GraphVertexRef vertex : vertices) {
            verticesById.values().removeIf(existing -> existing.getEntityId().equals(vertex.getEntityId()));
            verticesById.put(vertex.getVertexId(), vertex);
        }
        for (GraphEdgeRef edge : edges) {
            edgesById.put(edge.getEdgeId(), edge);
        }
        return new GraphSnapshot(graphVersion, new ArrayList<>(chunksById.values()),
            new ArrayList<>(entitiesById.values()), new ArrayList<>(verticesById.values()),
            new ArrayList<>(edgesById.values()));
    }

    private void validateTombstones(Map<String, TextChunk> chunksById,
                                    Map<String, EntityRef> entitiesById,
                                    Map<String, GraphEdgeRef> edgesById) {
        for (String id : tombstones.getChunks()) {
            if (!chunksById.containsKey(id)) {
                throw new GraphViewException(GraphViewException.Code.BAD_REFERENCE,
                    "delta deletes unknown chunk " + id);
            }
        }
        for (String id : tombstones.getEntities()) {
            if (!entitiesById.containsKey(id)) {
                throw new GraphViewException(GraphViewException.Code.BAD_REFERENCE,
                    "delta deletes unknown entity " + id);
            }
        }
        for (String id : tombstones.getEdges()) {
            if (!edgesById.containsKey(id)) {
                throw new GraphViewException(GraphViewException.Code.BAD_REFERENCE,
                    "delta deletes unknown edge " + id);
            }
        }
        for (String aliasKey : tombstones.getAliases()) {
            int separator = aliasKey.indexOf('\u001f');
            if (separator <= 0 || separator == aliasKey.length() - 1) {
                throw new GraphViewException(GraphViewException.Code.BAD_REFERENCE,
                    "invalid alias tombstone");
            }
            String entityId = aliasKey.substring(0, separator);
            String alias = aliasKey.substring(separator + 1);
            EntityRef entity = entitiesById.get(entityId);
            if (entity == null || !entity.getAliases().contains(alias)) {
                throw new GraphViewException(GraphViewException.Code.BAD_REFERENCE,
                    "delta deletes unknown alias " + alias);
            }
        }
    }

    private static Map<String, TextChunk> mapChunks(List<TextChunk> values) {
        Map<String, TextChunk> result = new LinkedHashMap<>();
        for (TextChunk value : values) {
            result.put(value.getChunkId(), value);
        }
        return result;
    }

    private static Map<String, EntityRef> mapEntities(List<EntityRef> values) {
        Map<String, EntityRef> result = new LinkedHashMap<>();
        for (EntityRef value : values) {
            result.put(value.getEntityId(), value);
        }
        return result;
    }

    private static Map<String, GraphVertexRef> mapVertices(List<GraphVertexRef> values) {
        Map<String, GraphVertexRef> result = new LinkedHashMap<>();
        for (GraphVertexRef value : values) {
            result.put(value.getVertexId(), value);
        }
        return result;
    }

    private static Map<String, GraphEdgeRef> mapEdges(List<GraphEdgeRef> values) {
        Map<String, GraphEdgeRef> result = new LinkedHashMap<>();
        for (GraphEdgeRef value : values) {
            result.put(value.getEdgeId(), value);
        }
        return result;
    }

    private static Set<String> union(List<String> existing, Set<String> additions) {
        Set<String> result = new TreeSet<>(existing);
        result.addAll(additions);
        return result;
    }

    private static EntityRef withAliases(EntityRef entity, Iterable<String> aliases) {
        List<String> values = new ArrayList<>();
        for (String alias : aliases) {
            values.add(alias);
        }
        return new EntityRef(entity.getEntityId(), entity.getCanonicalName(), values, entity.getType(),
            entity.getSourceChunkIds(), entity.getDataset(), entity.getDatasetVersion(), entity.getSplit(),
            entity.getSourceHash());
    }

    public GraphVersion getGraphVersion() {
        return graphVersion;
    }

    public GraphVersion getParentVersion() {
        return parentVersion;
    }

    public List<TextChunk> getChunks() {
        return chunks;
    }

    public List<EntityRef> getEntities() {
        return entities;
    }

    public List<GraphVertexRef> getVertices() {
        return vertices;
    }

    public List<GraphEdgeRef> getEdges() {
        return edges;
    }

    public Map<String, Set<String>> getAliases() {
        return aliases;
    }

    public GraphTombstones getTombstones() {
        return tombstones;
    }

    public String getDigest() {
        return digest;
    }

    public static Builder builder(GraphVersion graphVersion, GraphVersion parentVersion) {
        return new Builder(graphVersion, parentVersion);
    }

    /** Builder rejects duplicate operations so a release cannot depend on input iteration order. */
    public static final class Builder {
        private final GraphVersion graphVersion;
        private final GraphVersion parentVersion;
        private final Map<String, TextChunk> chunks = new TreeMap<>();
        private final Map<String, EntityRef> entities = new TreeMap<>();
        private final Map<String, GraphVertexRef> vertices = new TreeMap<>();
        private final Map<String, GraphEdgeRef> edges = new TreeMap<>();
        private final Map<String, Set<String>> aliases = new TreeMap<>();
        private final Set<String> deletedChunks = new TreeSet<>();
        private final Set<String> deletedEntities = new TreeSet<>();
        private final Set<String> deletedEdges = new TreeSet<>();
        private final Set<String> deletedAliases = new TreeSet<>();

        private Builder(GraphVersion graphVersion, GraphVersion parentVersion) {
            this.graphVersion = graphVersion;
            this.parentVersion = parentVersion;
        }

        public Builder upsertChunk(TextChunk chunk) {
            put(chunks, chunk == null ? null : chunk.getChunkId(), chunk, "chunk");
            return this;
        }

        public Builder upsertEntity(EntityRef entity) {
            put(entities, entity == null ? null : entity.getEntityId(), entity, "entity");
            return this;
        }

        public Builder upsertVertex(GraphVertexRef vertex) {
            put(vertices, vertex == null ? null : vertex.getVertexId(), vertex, "vertex");
            return this;
        }

        public Builder upsertEdge(GraphEdgeRef edge) {
            put(edges, edge == null ? null : edge.getEdgeId(), edge, "edge");
            return this;
        }

        public Builder upsertAlias(String entityId, String alias) {
            if (entityId == null || alias == null || entityId.isEmpty() || alias.isEmpty()) {
                throw new GraphViewException(GraphViewException.Code.INVALID_VIEW, "alias identity is required");
            }
            if (deletedAliases.contains(GraphTombstones.aliasKey(entityId, alias))) {
                throw new GraphViewException(GraphViewException.Code.DELTA_CONFLICT,
                    "alias is both updated and deleted");
            }
            if (!aliases.computeIfAbsent(entityId, ignored -> new TreeSet<>()).add(alias)) {
                throw new GraphViewException(GraphViewException.Code.DELTA_CONFLICT,
                    "duplicate alias update " + alias);
            }
            return this;
        }

        public Builder deleteChunk(String chunkId) {
            delete(deletedChunks, chunkId, chunks, "chunk");
            return this;
        }

        public Builder deleteEntity(String entityId) {
            delete(deletedEntities, entityId, entities, "entity");
            return this;
        }

        public Builder deleteEdge(String edgeId) {
            delete(deletedEdges, edgeId, edges, "edge");
            return this;
        }

        public Builder deleteAlias(String entityId, String alias) {
            if (entityId == null || alias == null || entityId.isEmpty() || alias.isEmpty()) {
                throw new GraphViewException(GraphViewException.Code.INVALID_VIEW, "alias identity is required");
            }
            String key = GraphTombstones.aliasKey(entityId, alias);
            if (aliases.getOrDefault(entityId, Collections.emptySet()).contains(alias)) {
                throw new GraphViewException(GraphViewException.Code.DELTA_CONFLICT,
                    "alias is both updated and deleted");
            }
            if (!deletedAliases.add(key)) {
                throw new GraphViewException(GraphViewException.Code.DELTA_CONFLICT,
                    "duplicate alias delete " + alias);
            }
            return this;
        }

        private static <T> void put(Map<String, T> values, String key, T value, String type) {
            if (value == null || key == null) {
                throw new GraphViewException(GraphViewException.Code.INVALID_VIEW, type + " is required");
            }
            if (values.put(key, value) != null) {
                throw new GraphViewException(GraphViewException.Code.DELTA_CONFLICT,
                    "duplicate " + type + " " + key);
            }
        }

        private static <T> void delete(Set<String> deleted, String key, Map<String, T> updates, String type) {
            if (key == null || key.isEmpty()) {
                throw new GraphViewException(GraphViewException.Code.INVALID_VIEW, type + " id is required");
            }
            if (updates.containsKey(key) || !deleted.add(key)) {
                throw new GraphViewException(GraphViewException.Code.DELTA_CONFLICT,
                    "duplicate or conflicting " + type + " operation " + key);
            }
        }

        public GraphDelta build() {
            return new GraphDelta(this);
        }
    }
}
