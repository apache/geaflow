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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.geaflow.ai.retrieval.codec.RetrievalModelJson;
import org.apache.geaflow.ai.retrieval.model.document.TextChunk;
import org.apache.geaflow.ai.retrieval.model.graph.EntityRef;
import org.apache.geaflow.ai.retrieval.model.graph.GraphEdgeRef;
import org.apache.geaflow.ai.retrieval.model.graph.GraphVertexRef;
import org.apache.geaflow.ai.retrieval.model.version.GraphVersion;

/** Immutable, fully validated graph snapshot used as the base of logical views. */
public final class GraphSnapshot {

    private static final Comparator<EntityRef> ENTITY_ORDER = Comparator.comparing(EntityRef::getEntityId);
    private static final Comparator<GraphEdgeRef> EDGE_ORDER = Comparator.comparing(GraphEdgeRef::getEdgeId);
    private static final Comparator<GraphVertexRef> VERTEX_ORDER = Comparator.comparing(GraphVertexRef::getVertexId);
    private static final Comparator<TextChunk> CHUNK_ORDER = Comparator.comparing(TextChunk::getChunkId);

    private final GraphVersion graphVersion;
    private final List<TextChunk> chunks;
    private final List<EntityRef> entities;
    private final List<GraphVertexRef> vertices;
    private final List<GraphEdgeRef> edges;
    private final String fingerprint;
    private final GraphStatistics statistics;

    public GraphSnapshot(GraphVersion graphVersion, List<EntityRef> entities, List<GraphEdgeRef> edges) {
        this(graphVersion, Collections.emptyList(), entities, Collections.emptyList(), edges);
    }

    /** Four-argument compatibility constructor accepting either chunks/entities or entities/vertices. */
    public GraphSnapshot(GraphVersion graphVersion, List<?> first, List<?> second,
                         List<GraphEdgeRef> edges) {
        this(graphVersion, classifyChunks(first, second), classifyEntities(first, second),
            classifyVertices(first, second), edges);
    }

    private static List<TextChunk> classifyChunks(List<?> first, List<?> second) {
        return first != null && !first.isEmpty() && first.get(0) instanceof TextChunk
            ? castChunks(first) : Collections.emptyList();
    }

    private static List<EntityRef> classifyEntities(List<?> first, List<?> second) {
        List<?> values = first != null && !first.isEmpty() && first.get(0) instanceof EntityRef ? first : second;
        return values != null && !values.isEmpty() && values.get(0) instanceof EntityRef
            ? castEntities(values) : Collections.emptyList();
    }

    private static List<GraphVertexRef> classifyVertices(List<?> first, List<?> second) {
        List<?> values = first != null && !first.isEmpty() && first.get(0) instanceof GraphVertexRef ? first : second;
        return values != null && !values.isEmpty() && values.get(0) instanceof GraphVertexRef
            ? castVertices(values) : Collections.emptyList();
    }

    private static List<TextChunk> castChunks(List<?> values) {
        List<TextChunk> result = new ArrayList<>();
        for (Object value : values) {
            result.add((TextChunk) value);
        }
        return result;
    }

    private static List<EntityRef> castEntities(List<?> values) {
        List<EntityRef> result = new ArrayList<>();
        for (Object value : values) {
            result.add((EntityRef) value);
        }
        return result;
    }

    private static List<GraphVertexRef> castVertices(List<?> values) {
        List<GraphVertexRef> result = new ArrayList<>();
        for (Object value : values) {
            result.add((GraphVertexRef) value);
        }
        return result;
    }

    public GraphSnapshot(GraphVersion graphVersion, List<TextChunk> chunks, List<EntityRef> entities,
                         List<GraphVertexRef> vertices, List<GraphEdgeRef> edges) {
        if (graphVersion == null) {
            throw new GraphViewException(GraphViewException.Code.INVALID_VIEW, "graphVersion is required");
        }
        this.graphVersion = graphVersion;
        this.chunks = sortedChunks(chunks);
        this.entities = sortedEntities(entities);
        this.vertices = sortedVertices(vertices, this.entities);
        this.edges = sortedEdges(edges, this.entities);
        validateAliases(this.entities);
        validateChunkReferences(this.chunks, this.entities, this.edges);
        this.statistics = GraphStatistics.from(this.edges);
        this.fingerprint = fingerprint();
    }

    private static List<TextChunk> sortedChunks(List<TextChunk> values) {
        List<TextChunk> result = copy(values);
        result.sort(CHUNK_ORDER);
        ensureUnique(result, TextChunk::getChunkId, "chunk");
        return Collections.unmodifiableList(result);
    }

    private static List<EntityRef> sortedEntities(List<EntityRef> values) {
        List<EntityRef> result = copy(values);
        result.sort(ENTITY_ORDER);
        ensureUnique(result, EntityRef::getEntityId, "entity");
        return Collections.unmodifiableList(result);
    }

    private static List<GraphVertexRef> sortedVertices(List<GraphVertexRef> values,
                                                       List<EntityRef> entities) {
        List<GraphVertexRef> result = copy(values);
        result.sort(VERTEX_ORDER);
        ensureUnique(result, GraphVertexRef::getVertexId, "vertex");
        Set<String> entityIds = new HashSet<>();
        for (GraphVertexRef vertex : result) {
            if (!entityIds.add(vertex.getEntityId())) {
                throw new GraphViewException(GraphViewException.Code.DELTA_CONFLICT,
                    "multiple vertices map to entity " + vertex.getEntityId());
            }
        }
        Map<String, EntityRef> byId = byEntityId(entities);
        for (GraphVertexRef vertex : result) {
            if (!byId.containsKey(vertex.getEntityId())) {
                throw new GraphViewException(GraphViewException.Code.BAD_REFERENCE,
                    "vertex references unknown entity " + vertex.getEntityId());
            }
        }
        return Collections.unmodifiableList(result);
    }

    private static List<GraphEdgeRef> sortedEdges(List<GraphEdgeRef> values, List<EntityRef> entities) {
        List<GraphEdgeRef> result = copy(values);
        result.sort(EDGE_ORDER);
        ensureUnique(result, GraphEdgeRef::getEdgeId, "edge");
        Set<String> entityIds = byEntityId(entities).keySet();
        for (GraphEdgeRef edge : result) {
            if (!entityIds.contains(edge.getSourceEntityId()) || !entityIds.contains(edge.getTargetEntityId())) {
                throw new GraphViewException(GraphViewException.Code.BAD_REFERENCE,
                    "edge references unknown entity: " + edge.getEdgeId());
            }
        }
        return Collections.unmodifiableList(result);
    }

    private static void validateAliases(List<EntityRef> entities) {
        Map<String, String> owners = new HashMap<>();
        for (EntityRef entity : entities) {
            for (String alias : entity.getAliases()) {
                String previous = owners.putIfAbsent(alias, entity.getEntityId());
                if (previous != null && !previous.equals(entity.getEntityId())) {
                    throw new GraphViewException(GraphViewException.Code.DELTA_CONFLICT,
                        "alias is assigned to multiple entities: " + alias);
                }
            }
        }
    }

    private static void validateChunkReferences(List<TextChunk> chunks, List<EntityRef> entities,
                                                List<GraphEdgeRef> edges) {
        if (chunks.isEmpty()) {
            return;
        }
        Set<String> chunkIds = new HashSet<>();
        Set<String> positions = new HashSet<>();
        for (TextChunk chunk : chunks) {
            chunkIds.add(chunk.getChunkId());
            String position = chunk.getDocumentId() + "\u001f" + chunk.getChunkIndex();
            if (!positions.add(position)) {
                throw new GraphViewException(GraphViewException.Code.DELTA_CONFLICT,
                    "duplicate chunk position " + position);
            }
        }
        for (EntityRef entity : entities) {
            for (String chunkId : entity.getSourceChunkIds()) {
                requireChunk(chunkIds, chunkId, "entity " + entity.getEntityId());
            }
        }
        for (GraphEdgeRef edge : edges) {
            for (String chunkId : edge.getSourceChunkIds()) {
                requireChunk(chunkIds, chunkId, "edge " + edge.getEdgeId());
            }
        }
    }

    private static void requireChunk(Set<String> chunkIds, String chunkId, String owner) {
        if (!chunkIds.contains(chunkId)) {
            throw new GraphViewException(GraphViewException.Code.BAD_REFERENCE,
                owner + " references unknown chunk " + chunkId);
        }
    }

    private static <T> List<T> copy(List<T> values) {
        List<T> result = new ArrayList<>();
        if (values != null) {
            for (T value : values) {
                if (value == null) {
                    throw new GraphViewException(GraphViewException.Code.INVALID_VIEW, "graph records must not be null");
                }
                result.add(value);
            }
        }
        return result;
    }

    private static <T> void ensureUnique(List<T> values, java.util.function.Function<T, String> key,
                                         String type) {
        Set<String> ids = new HashSet<>();
        for (T value : values) {
            if (!ids.add(key.apply(value))) {
                throw new GraphViewException(GraphViewException.Code.DELTA_CONFLICT,
                    "duplicate " + type + " " + key.apply(value));
            }
        }
    }

    private static Map<String, EntityRef> byEntityId(List<EntityRef> values) {
        Map<String, EntityRef> result = new HashMap<>();
        for (EntityRef entity : values) {
            result.put(entity.getEntityId(), entity);
        }
        return result;
    }

    private String fingerprint() {
        StringBuilder value = new StringBuilder();
        value.append(graphVersion.getGraphName()).append('\n').append(graphVersion.getVersion()).append('\n');
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
        return sha256(value.toString());
    }

    static String sha256(String value) {
        try {
            byte[] bytes = MessageDigest.getInstance("SHA-256")
                .digest(value.getBytes(StandardCharsets.UTF_8));
            StringBuilder result = new StringBuilder(bytes.length * 2);
            for (byte item : bytes) {
                result.append(String.format("%02x", item & 0xff));
            }
            return result.toString();
        } catch (NoSuchAlgorithmException error) {
            throw new IllegalStateException("SHA-256 is unavailable", error);
        }
    }

    public GraphVersion getGraphVersion() {
        return graphVersion;
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

    public String getFingerprint() {
        return fingerprint;
    }

    public GraphStatistics getStatistics() {
        return statistics;
    }
}
