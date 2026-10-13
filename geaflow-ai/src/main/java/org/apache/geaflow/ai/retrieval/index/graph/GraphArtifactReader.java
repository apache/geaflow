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

package org.apache.geaflow.ai.retrieval.index.graph;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalErrorCode;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalException;
import org.apache.geaflow.ai.retrieval.channel.graph.GraphNeighborProvider;
import org.apache.geaflow.ai.retrieval.channel.graph.InMemoryGraphNeighborProvider;
import org.apache.geaflow.ai.retrieval.index.ArtifactIdentity;
import org.apache.geaflow.ai.retrieval.model.document.RetrievalIds;
import org.apache.geaflow.ai.retrieval.model.document.TextChunk;
import org.apache.geaflow.ai.retrieval.model.graph.EntityRef;
import org.apache.geaflow.ai.retrieval.model.graph.GraphEdgeRef;
import org.apache.geaflow.ai.retrieval.model.graph.GraphVertexRef;

/** Reader for current and legacy deterministic graph artifacts. */
public final class GraphArtifactReader implements Closeable {
    private static final String MAGIC = "GEAFLOW-GRAPH-2";
    private static final String LEGACY_MAGIC = "GEAFLOW-GRAPH-1";
    private final ArtifactIdentity identity;
    private final List<EntityRef> entities;
    private final List<GraphEdgeRef> edges;
    private final List<GraphVertexRef> vertices;
    private final GraphNeighborProvider neighbors;

    public GraphArtifactReader(Path artifact, String expectedGraphVersion, String expectedIndexVersion) {
        if (artifact == null || !Files.isRegularFile(artifact)) {
            throw notReady("missing graph artifact: " + artifact);
        }
        try (DataInputStream input = new DataInputStream(Files.newInputStream(artifact))) {
            String magic = input.readUTF();
            boolean currentFormat = MAGIC.equals(magic);
            if (!currentFormat && !LEGACY_MAGIC.equals(magic)) {
                throw new IOException("invalid graph artifact magic");
            }
            identity = new ArtifactIdentity(input.readUTF(), input.readUTF(), input.readUTF(), input.readUTF());
            identity.validateVersions(expectedGraphVersion, expectedIndexVersion);
            int entityCount = input.readInt();
            int edgeCount = input.readInt();
            int vertexCount = currentFormat ? input.readInt() : -1;
            if (entityCount < 0 || edgeCount < 0 || (currentFormat && vertexCount < 0)) {
                throw new IOException("invalid graph record counts");
            }
            long minimumRecordBytes = (long) entityCount * 14L + (long) edgeCount * 12L
                + (long) Math.max(0, vertexCount) * 7L;
            if (minimumRecordBytes > input.available()) {
                throw new IOException("graph record counts exceed artifact size");
            }
            entities = new ArrayList<>(entityCount);
            Set<String> entityIds = new HashSet<>();
            String previousEntityId = null;
            for (int index = 0; index < entityCount; index++) {
                String entityId = input.readUTF();
                final String canonical = input.readUTF();
                final String type = input.readUTF();
                int aliasCount = input.readInt();
                if (aliasCount < 0 || !entityIds.add(entityId)
                    || previousEntityId != null && previousEntityId.compareTo(entityId) >= 0) {
                    throw new IOException("invalid or duplicate graph entity");
                }
                validateStringCount(input, aliasCount);
                previousEntityId = entityId;
                List<String> aliases = readStrings(input, aliasCount);
                int sourceCount = input.readInt();
                if (sourceCount < 0) {
                    throw new IOException("invalid graph entity provenance count");
                }
                validateStringCount(input, sourceCount);
                List<String> sources = readStrings(input, sourceCount);
                entities.add(currentFormat
                    ? readEntityWithProvenance(input, entityId, canonical, aliases, type, sources)
                    : new EntityRef(entityId, canonical, aliases, type, sources));
            }
            edges = new ArrayList<>(edgeCount);
            Set<String> edgeIds = new HashSet<>();
            String previousEdgeId = null;
            for (int index = 0; index < edgeCount; index++) {
                String edgeId = input.readUTF();
                final String label = input.readUTF();
                String source = input.readUTF();
                String target = input.readUTF();
                int sourceCount = input.readInt();
                if (sourceCount < 0 || !edgeIds.add(edgeId)
                    || !entityIds.contains(source) || !entityIds.contains(target)
                    || previousEdgeId != null && previousEdgeId.compareTo(edgeId) >= 0) {
                    throw new IOException("invalid or duplicate graph edge");
                }
                validateStringCount(input, sourceCount);
                previousEdgeId = edgeId;
                List<String> sourceChunks = readStrings(input, sourceCount);
                edges.add(currentFormat
                    ? readEdgeWithProvenance(input, edgeId, label, source, target, sourceChunks)
                    : new GraphEdgeRef(edgeId, label, source, target, sourceChunks));
            }
            for (GraphEdgeRef edge : edges) {
                if (!entityIds.contains(edge.getSourceEntityId())
                    || !entityIds.contains(edge.getTargetEntityId())) {
                    throw new IOException("graph edge references unknown entity");
                }
            }
            vertices = currentFormat ? readVertices(input, vertexCount, entities)
                : defaultVertices(entities);
            if (input.read() != -1) {
                throw new IOException("trailing graph artifact data");
            }
            neighbors = new InMemoryGraphNeighborProvider(edges, vertices);
        } catch (RetrievalException error) {
            throw error;
        } catch (Exception error) {
            throw new RetrievalException(RetrievalErrorCode.INDEX_NOT_READY,
                "unable to read graph artifact", error);
        }
    }

    private static EntityRef readEntityWithProvenance(DataInputStream input, String entityId,
                                                       String canonical, List<String> aliases,
                                                       String type, List<String> sourceChunks)
        throws IOException {
        String[] provenance = readProvenance(input);
        return new EntityRef(entityId, canonical, aliases, type, sourceChunks,
            provenance[0], provenance[1], provenance[2], provenance[3]);
    }

    private static GraphEdgeRef readEdgeWithProvenance(DataInputStream input, String edgeId,
                                                        String label, String source, String target,
                                                        List<String> sourceChunks) throws IOException {
        String[] provenance = readProvenance(input);
        return new GraphEdgeRef(edgeId, label, source, target, sourceChunks,
            provenance[0], provenance[1], provenance[2], provenance[3]);
    }

    private static String[] readProvenance(DataInputStream input) throws IOException {
        if (!input.readBoolean()) {
            return new String[] {null, null, null, null};
        }
        return new String[] {input.readUTF(), input.readUTF(), input.readUTF(), input.readUTF()};
    }

    private static List<GraphVertexRef> readVertices(DataInputStream input, int count,
                                                     List<EntityRef> entities) throws IOException {
        Map<String, EntityRef> entityById = entities.stream().collect(Collectors.toMap(
            EntityRef::getEntityId, Function.identity()));
        Set<String> vertexIds = new HashSet<>();
        Set<String> mappedEntities = new HashSet<>();
        List<GraphVertexRef> result = new ArrayList<>(count);
        String previousVertexId = null;
        for (int index = 0; index < count; index++) {
            final String label = input.readUTF();
            String vertexId = input.readUTF();
            String entityId = input.readUTF();
            String[] provenance = readProvenance(input);
            EntityRef entity = entityById.get(entityId);
            if (!vertexIds.add(vertexId) || !mappedEntities.add(entityId) || entity == null
                || (previousVertexId != null && previousVertexId.compareTo(vertexId) >= 0)) {
                throw new IOException("invalid or duplicate graph vertex mapping");
            }
            if (!sameProvenance(provenance, entity)) {
                throw new IOException("graph vertex provenance conflicts with entity");
            }
            previousVertexId = vertexId;
            result.add(new GraphVertexRef(label, vertexId, entityId, provenance[0], provenance[1],
                provenance[2], provenance[3]));
        }
        if (!mappedEntities.equals(entityById.keySet())) {
            throw new IOException("every graph entity must have exactly one vertex mapping");
        }
        return result;
    }

    private static boolean sameProvenance(String[] provenance, EntityRef entity) {
        return Objects.equals(provenance[0], entity.getDataset())
            && Objects.equals(provenance[1], entity.getDatasetVersion())
            && Objects.equals(provenance[2], entity.getSplit())
            && Objects.equals(provenance[3], entity.getSourceHash());
    }

    private static List<GraphVertexRef> defaultVertices(List<EntityRef> entities) {
        List<GraphVertexRef> result = new ArrayList<>(entities.size());
        for (EntityRef entity : entities) {
            result.add(new GraphVertexRef(entity.getType(), entity.getEntityId(), entity.getEntityId(),
                entity.getDataset(), entity.getDatasetVersion(), entity.getSplit(), entity.getSourceHash()));
        }
        return result;
    }

    private static List<String> readStrings(DataInputStream input, int count) throws IOException {
        validateStringCount(input, count);
        List<String> values = new ArrayList<>(count);
        Set<String> seen = new HashSet<>();
        for (int index = 0; index < count; index++) {
            String value = input.readUTF();
            if (!seen.add(value)) {
                throw new IOException("duplicate graph reference");
            }
            values.add(value);
        }
        return values;
    }

    private static void validateStringCount(DataInputStream input, int count) throws IOException {
        if (count < 0 || (long) count * 2L > input.available()) {
            throw new IOException("graph string count exceeds remaining artifact data");
        }
    }

    private static RetrievalException notReady(String message) {
        return new RetrievalException(RetrievalErrorCode.INDEX_NOT_READY, message);
    }

    public ArtifactIdentity getIdentity() {
        return identity;
    }

    public List<EntityRef> getEntities() {
        return java.util.Collections.unmodifiableList(entities);
    }

    public List<GraphEdgeRef> getEdges() {
        return java.util.Collections.unmodifiableList(edges);
    }

    public List<GraphVertexRef> getVertices() {
        return java.util.Collections.unmodifiableList(vertices);
    }

    public GraphNeighborProvider getNeighbors() {
        return neighbors;
    }

    /** Validates that all graph provenance references point at the supplied chunks. */
    public void validateReferences(List<TextChunk> chunks) {
        if (chunks == null) {
            throw notReady("graph fixture chunks are missing");
        }
        Map<String, TextChunk> chunksById = new java.util.HashMap<>();
        for (TextChunk chunk : chunks) {
            if (chunk == null || chunksById.put(chunk.getChunkId(), chunk) != null) {
                throw notReady("invalid or duplicate fixture chunk");
            }
        }
        Set<String> chunkIds = chunksById.keySet();
        for (EntityRef entity : entities) {
            if (entity.getDataset() != null && entity.getSourceChunkIds().isEmpty()) {
                throw notReady("entity " + entity.getEntityId()
                    + " requires source chunks for scoped provenance");
            }
            validateChunkReferences(entity.getSourceChunkIds(), chunkIds, "entity " + entity.getEntityId());
            validateProvenance(entity.getDataset(), entity.getDatasetVersion(), entity.getSplit(),
                entity.getSourceHash(), entity.getSourceChunkIds(), chunksById,
                "entity " + entity.getEntityId());
        }
        for (GraphEdgeRef edge : edges) {
            validateChunkReferences(edge.getSourceChunkIds(), chunkIds, "edge " + edge.getEdgeId());
            validateProvenance(edge.getDataset(), edge.getDatasetVersion(), edge.getSplit(),
                edge.getSourceHash(), edge.getSourceChunkIds(), chunksById,
                "edge " + edge.getEdgeId());
        }
    }

    private static void validateChunkReferences(List<String> references, Set<String> chunkIds, String owner) {
        for (String chunkId : references) {
            if (!chunkIds.contains(chunkId)) {
                throw notReady(owner + " references unknown chunk " + chunkId);
            }
        }
    }

    private static void validateProvenance(String dataset, String datasetVersion, String split,
                                           String sourceHash, List<String> sourceChunkIds,
                                           Map<String, TextChunk> chunks, String owner) {
        if (sourceChunkIds.isEmpty()) {
            if (dataset == null && chunks.values().stream()
                .anyMatch(chunk -> chunk.getDataset() != null)) {
                throw notReady(owner + " is missing provenance for scoped chunks");
            }
            return;
        }
        Set<String> sourceHashes = new java.util.TreeSet<>();
        boolean hasScopedChunk = false;
        boolean hasUnscopedChunk = false;
        for (String chunkId : sourceChunkIds) {
            TextChunk chunk = chunks.get(chunkId);
            if (chunk == null) {
                throw notReady(owner + " references an unknown chunk " + chunkId);
            }
            if (chunk.getDataset() == null) {
                hasUnscopedChunk = true;
            } else {
                hasScopedChunk = true;
                sourceHashes.add(chunk.getSourceHash());
            }
        }
        if (dataset == null) {
            if (hasScopedChunk) {
                throw notReady(owner + " is missing provenance for scoped chunks");
            }
            return;
        }
        if (hasUnscopedChunk) {
            throw notReady(owner + " provenance references an unscoped chunk");
        }
        for (String chunkId : sourceChunkIds) {
            TextChunk chunk = chunks.get(chunkId);
            if (!Objects.equals(dataset, chunk.getDataset())
                || !Objects.equals(datasetVersion, chunk.getDatasetVersion())
                || !Objects.equals(split, chunk.getSplit())) {
                throw notReady(owner + " provenance does not match referenced chunk");
            }
        }
        if (!Objects.equals(sourceHash, RetrievalIds.aggregateSourceHash(
            new ArrayList<>(sourceHashes)))) {
            throw notReady(owner + " sourceHash does not match referenced chunks");
        }
    }

    @Override
    public void close() {
        // The artifact is loaded into immutable memory; no resources remain open.
    }
}
