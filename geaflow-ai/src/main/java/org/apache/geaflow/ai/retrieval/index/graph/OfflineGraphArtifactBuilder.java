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

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.geaflow.ai.retrieval.index.ArtifactIdentity;
import org.apache.geaflow.ai.retrieval.index.ArtifactPublisher;
import org.apache.geaflow.ai.retrieval.ingest.GraphArtifact;
import org.apache.geaflow.ai.retrieval.ingest.IngestionContext;
import org.apache.geaflow.ai.retrieval.metadata.GraphBuildMetadata;
import org.apache.geaflow.ai.retrieval.model.document.RetrievalIds;
import org.apache.geaflow.ai.retrieval.model.document.TextChunk;
import org.apache.geaflow.ai.retrieval.model.graph.EntityRef;
import org.apache.geaflow.ai.retrieval.model.graph.GraphEdgeRef;
import org.apache.geaflow.ai.retrieval.model.graph.GraphVertexRef;

/** Builds and atomically publishes a deterministic graph artifact. */
public final class OfflineGraphArtifactBuilder {
    private static final String MAGIC = "GEAFLOW-GRAPH-2";
    private final Path outputDirectory;

    public OfflineGraphArtifactBuilder(Path outputDirectory) {
        this.outputDirectory = outputDirectory;
    }

    public GraphArtifact build(IngestionContext context, List<TextChunk> chunks,
                               List<EntityRef> entities, List<GraphEdgeRef> edges) throws IOException {
        List<GraphVertexRef> vertices = new ArrayList<>();
        for (EntityRef entity : entities) {
            vertices.add(new GraphVertexRef(entity.getType(), entity.getEntityId(), entity.getEntityId(),
                entity.getDataset(), entity.getDatasetVersion(), entity.getSplit(), entity.getSourceHash()));
        }
        return build(context, chunks, entities, vertices, edges);
    }

    public GraphArtifact build(IngestionContext context, List<TextChunk> chunks,
                               List<EntityRef> entities, List<GraphVertexRef> vertices,
                               List<GraphEdgeRef> edges) throws IOException {
        String graphVersion = context.getGraphVersion().getVersion();
        String indexVersion = context.getIndexVersion();
        Path published = graphVersion.equals(indexVersion)
            ? ArtifactIdentity.resolveVersionedPath(outputDirectory, "graph-", graphVersion, ".bin")
            : ArtifactIdentity.resolveVersionedPath(
                ArtifactIdentity.resolveVersionedPath(outputDirectory, "graph-", graphVersion, ""),
                "index-", indexVersion, ".bin");
        Files.createDirectories(published.getParent());
        List<EntityRef> orderedEntities = new ArrayList<>(entities);
        orderedEntities.sort(Comparator.comparing(EntityRef::getEntityId));
        List<GraphVertexRef> orderedVertices = new ArrayList<>(vertices);
        orderedVertices.sort(Comparator.comparing(GraphVertexRef::getVertexId));
        List<GraphEdgeRef> orderedEdges = new ArrayList<>(edges);
        orderedEdges.sort(Comparator.comparing(GraphEdgeRef::getEdgeId));
        validate(orderedEntities, orderedVertices, orderedEdges, chunks);
        Path staging = Files.createTempFile(outputDirectory, "graph-", ".tmp");
        try {
            try (DataOutputStream output = new DataOutputStream(Files.newOutputStream(staging))) {
                output.writeUTF(MAGIC);
                output.writeUTF(context.getGraphVersion().getGraphName());
                output.writeUTF(graphVersion);
                output.writeUTF(indexVersion);
                output.writeUTF(ArtifactIdentity.fingerprint(chunks));
                output.writeInt(orderedEntities.size());
                output.writeInt(orderedEdges.size());
                output.writeInt(orderedVertices.size());
                for (EntityRef entity : orderedEntities) {
                    output.writeUTF(entity.getEntityId());
                    output.writeUTF(entity.getCanonicalName());
                    output.writeUTF(entity.getType());
                    writeStrings(output, entity.getAliases());
                    writeStrings(output, entity.getSourceChunkIds());
                    writeProvenance(output, entity.getDataset(), entity.getDatasetVersion(),
                        entity.getSplit(), entity.getSourceHash());
                }
                for (GraphEdgeRef edge : orderedEdges) {
                    output.writeUTF(edge.getEdgeId());
                    output.writeUTF(edge.getLabel());
                    output.writeUTF(edge.getSourceEntityId());
                    output.writeUTF(edge.getTargetEntityId());
                    writeStrings(output, edge.getSourceChunkIds());
                    writeProvenance(output, edge.getDataset(), edge.getDatasetVersion(),
                        edge.getSplit(), edge.getSourceHash());
                }
                for (GraphVertexRef vertex : orderedVertices) {
                    output.writeUTF(vertex.getLabel());
                    output.writeUTF(vertex.getVertexId());
                    output.writeUTF(vertex.getEntityId());
                    writeProvenance(output, vertex.getDataset(), vertex.getDatasetVersion(),
                        vertex.getSplit(), vertex.getSourceHash());
                }
            }
            ArtifactPublisher.publish(staging, published,
                existing -> validateExisting(existing, context, orderedEntities, orderedEdges,
                    orderedVertices, chunks));
            return new Artifact(new GraphBuildMetadata(context.getGraphVersion(), published.toString(),
                "offline-graph", "offline-graph-v2", true), published);
        } finally {
            Files.deleteIfExists(staging);
        }
    }

    private static void writeStrings(DataOutputStream output, List<String> values) throws IOException {
        output.writeInt(values.size());
        for (String value : values) {
            output.writeUTF(value);
        }
    }

    private static void writeProvenance(DataOutputStream output, String dataset,
                                        String datasetVersion, String split, String sourceHash)
        throws IOException {
        boolean present = dataset != null;
        if (present != (datasetVersion != null && split != null && sourceHash != null)) {
            throw new IOException("incomplete graph provenance");
        }
        output.writeBoolean(present);
        if (present) {
            output.writeUTF(dataset);
            output.writeUTF(datasetVersion);
            output.writeUTF(split);
            output.writeUTF(sourceHash);
        }
    }

    private static void validate(List<EntityRef> entities, List<GraphVertexRef> vertices,
                                 List<GraphEdgeRef> edges, List<TextChunk> chunks) throws IOException {
        Set<String> entityIds = new HashSet<>();
        Map<String, EntityRef> entityById = new java.util.HashMap<>();
        for (EntityRef entity : entities) {
            if (!entityIds.add(entity.getEntityId())) {
                throw new IOException("duplicate graph entity");
            }
            entityById.put(entity.getEntityId(), entity);
        }
        Set<String> vertexIds = new HashSet<>();
        Set<String> vertexEntities = new HashSet<>();
        for (GraphVertexRef vertex : vertices) {
            if (!vertexIds.add(vertex.getVertexId()) || !vertexEntities.add(vertex.getEntityId())
                || !entityIds.contains(vertex.getEntityId())) {
                throw new IOException("invalid or duplicate graph vertex mapping");
            }
            EntityRef entity = entityById.get(vertex.getEntityId());
            if (!sameProvenance(vertex.getDataset(), vertex.getDatasetVersion(), vertex.getSplit(),
                vertex.getSourceHash(), entity.getDataset(), entity.getDatasetVersion(),
                entity.getSplit(), entity.getSourceHash())) {
                throw new IOException("graph vertex provenance conflicts with entity");
            }
        }
        if (!vertexEntities.equals(entityIds)) {
            throw new IOException("every graph entity must have exactly one vertex mapping");
        }
        Map<String, TextChunk> chunksById = new HashMap<>();
        for (TextChunk chunk : chunks) {
            if (chunk == null || chunksById.put(chunk.getChunkId(), chunk) != null) {
                throw new IOException("invalid or duplicate chunk");
            }
        }
        Set<String> chunkIds = chunksById.keySet();
        for (EntityRef entity : entities) {
            if (entity.getDataset() != null && entity.getSourceChunkIds().isEmpty()) {
                throw new IOException("entity " + entity.getEntityId()
                    + " requires source chunks for scoped provenance");
            }
            for (String chunkId : entity.getSourceChunkIds()) {
                if (!chunkIds.contains(chunkId)) {
                    throw new IOException("graph entity references unknown chunk");
                }
            }
            validateProvenance(entity.getDataset(), entity.getDatasetVersion(), entity.getSplit(),
                entity.getSourceHash(), entity.getSourceChunkIds(), chunksById,
                "entity " + entity.getEntityId());
        }
        Set<String> edgeIds = new HashSet<>();
        for (GraphEdgeRef edge : edges) {
            if (!edgeIds.add(edge.getEdgeId()) || !entityIds.contains(edge.getSourceEntityId())
                || !entityIds.contains(edge.getTargetEntityId())) {
                throw new IOException("invalid graph edge");
            }
            for (String chunkId : edge.getSourceChunkIds()) {
                if (!chunkIds.contains(chunkId)) {
                    throw new IOException("graph edge references unknown chunk");
                }
            }
            validateProvenance(edge.getDataset(), edge.getDatasetVersion(), edge.getSplit(),
                edge.getSourceHash(), edge.getSourceChunkIds(), chunksById,
                "edge " + edge.getEdgeId());
        }
    }

    private static boolean sameProvenance(String firstDataset, String firstVersion, String firstSplit,
                                          String firstHash, String secondDataset, String secondVersion,
                                          String secondSplit, String secondHash) {
        return Objects.equals(firstDataset, secondDataset)
            && Objects.equals(firstVersion, secondVersion)
            && Objects.equals(firstSplit, secondSplit)
            && Objects.equals(firstHash, secondHash);
    }

    private static void validateProvenance(String dataset, String datasetVersion, String split,
                                           String sourceHash, List<String> sourceChunkIds,
                                           Map<String, TextChunk> chunks, String owner) throws IOException {
        if (sourceChunkIds.isEmpty()) {
            if (dataset == null && chunks.values().stream()
                .anyMatch(chunk -> chunk.getDataset() != null)) {
                throw new IOException(owner + " is missing provenance for scoped chunks");
            }
            return;
        }
        Set<String> sourceHashes = new java.util.TreeSet<>();
        boolean hasScopedChunk = false;
        boolean hasUnscopedChunk = false;
        for (String chunkId : sourceChunkIds) {
            TextChunk chunk = chunks.get(chunkId);
            if (chunk == null) {
                throw new IOException(owner + " references an unknown chunk");
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
                throw new IOException(owner + " is missing provenance for scoped chunks");
            }
            return;
        }
        if (hasUnscopedChunk) {
            throw new IOException(owner + " provenance references an unscoped chunk");
        }
        for (String chunkId : sourceChunkIds) {
            TextChunk chunk = chunks.get(chunkId);
            if (!Objects.equals(dataset, chunk.getDataset())
                || !Objects.equals(datasetVersion, chunk.getDatasetVersion())
                || !Objects.equals(split, chunk.getSplit())) {
                throw new IOException(owner + " provenance does not match referenced chunk");
            }
        }
        if (!Objects.equals(sourceHash, RetrievalIds.aggregateSourceHash(
            new ArrayList<>(sourceHashes)))) {
            throw new IOException(owner + " sourceHash does not match referenced chunks");
        }
    }

    private static void validateExisting(Path path, IngestionContext context,
                                         List<EntityRef> entities, List<GraphEdgeRef> edges,
                                         List<GraphVertexRef> vertices,
                                         List<TextChunk> chunks) throws IOException {
        String graphVersion = context.getGraphVersion().getVersion();
        String indexVersion = context.getIndexVersion();
        try (GraphArtifactReader reader = new GraphArtifactReader(path, graphVersion, indexVersion)) {
            reader.getIdentity().validate(context.getGraphVersion().getGraphName(), graphVersion,
                indexVersion, chunks);
            reader.validateReferences(chunks);
            if (!entities.equals(reader.getEntities()) || !edges.equals(reader.getEdges())) {
                throw new IOException("existing graph artifact content mismatch");
            }
            if (!vertices.equals(reader.getVertices())) {
                throw new IOException("existing graph vertex content mismatch");
            }
        } catch (org.apache.geaflow.ai.retrieval.api.model.RetrievalException error) {
            throw new IOException("existing graph artifact is invalid", error);
        }
    }

    private static final class Artifact implements GraphArtifact {
        private final GraphBuildMetadata metadata;
        private final Path path;

        private Artifact(GraphBuildMetadata metadata, Path path) {
            this.metadata = metadata;
            this.path = path;
        }

        @Override
        public GraphBuildMetadata getMetadata() {
            return metadata;
        }

        @Override
        public void close() throws IOException {
            if (!Files.isRegularFile(path) || Files.size(path) == 0) {
                throw new IOException("graph artifact is not readable: " + path);
            }
        }
    }
}
