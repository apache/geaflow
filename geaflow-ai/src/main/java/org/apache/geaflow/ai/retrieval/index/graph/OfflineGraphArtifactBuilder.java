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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.geaflow.ai.retrieval.index.ArtifactIdentity;
import org.apache.geaflow.ai.retrieval.index.ArtifactPublisher;
import org.apache.geaflow.ai.retrieval.ingest.GraphArtifact;
import org.apache.geaflow.ai.retrieval.ingest.IngestionContext;
import org.apache.geaflow.ai.retrieval.metadata.GraphBuildMetadata;
import org.apache.geaflow.ai.retrieval.model.document.TextChunk;
import org.apache.geaflow.ai.retrieval.model.graph.EntityRef;
import org.apache.geaflow.ai.retrieval.model.graph.GraphEdgeRef;

/** Builds and atomically publishes a deterministic graph artifact. */
public final class OfflineGraphArtifactBuilder {
    private static final String MAGIC = "GEAFLOW-GRAPH-1";
    private final Path outputDirectory;

    public OfflineGraphArtifactBuilder(Path outputDirectory) {
        this.outputDirectory = outputDirectory;
    }

    public GraphArtifact build(IngestionContext context, List<TextChunk> chunks,
                               List<EntityRef> entities, List<GraphEdgeRef> edges) throws IOException {
        Path published = ArtifactIdentity.resolveVersionedPath(outputDirectory, "graph-",
            context.getGraphVersion().getVersion(), ".bin");
        Files.createDirectories(outputDirectory);
        List<EntityRef> orderedEntities = new ArrayList<>(entities);
        orderedEntities.sort(Comparator.comparing(EntityRef::getEntityId));
        List<GraphEdgeRef> orderedEdges = new ArrayList<>(edges);
        orderedEdges.sort(Comparator.comparing(GraphEdgeRef::getEdgeId));
        validate(orderedEntities, orderedEdges, chunks);
        Path staging = Files.createTempFile(outputDirectory, "graph-", ".tmp");
        try {
            try (DataOutputStream output = new DataOutputStream(Files.newOutputStream(staging))) {
                output.writeUTF(MAGIC);
                output.writeUTF(context.getGraphVersion().getGraphName());
                output.writeUTF(context.getGraphVersion().getVersion());
                output.writeUTF(context.getGraphVersion().getVersion());
                output.writeUTF(ArtifactIdentity.fingerprint(chunks));
                output.writeInt(orderedEntities.size());
                output.writeInt(orderedEdges.size());
                for (EntityRef entity : orderedEntities) {
                    output.writeUTF(entity.getEntityId());
                    output.writeUTF(entity.getCanonicalName());
                    output.writeUTF(entity.getType());
                    writeStrings(output, entity.getAliases());
                    writeStrings(output, entity.getSourceChunkIds());
                }
                for (GraphEdgeRef edge : orderedEdges) {
                    output.writeUTF(edge.getEdgeId());
                    output.writeUTF(edge.getLabel());
                    output.writeUTF(edge.getSourceEntityId());
                    output.writeUTF(edge.getTargetEntityId());
                    writeStrings(output, edge.getSourceChunkIds());
                }
            }
            ArtifactPublisher.publish(staging, published,
                existing -> validateExisting(existing, context, orderedEntities, orderedEdges, chunks));
            return new Artifact(new GraphBuildMetadata(context.getGraphVersion(), published.toString(),
                "offline-graph", "offline-graph-v1", true), published);
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

    private static void validate(List<EntityRef> entities, List<GraphEdgeRef> edges, List<TextChunk> chunks)
        throws IOException {
        Set<String> entityIds = new HashSet<>();
        for (EntityRef entity : entities) {
            if (!entityIds.add(entity.getEntityId())) {
                throw new IOException("duplicate graph entity");
            }
        }
        Set<String> chunkIds = new HashSet<>();
        for (TextChunk chunk : chunks) {
            if (chunk == null || !chunkIds.add(chunk.getChunkId())) {
                throw new IOException("invalid or duplicate chunk");
            }
        }
        for (EntityRef entity : entities) {
            for (String chunkId : entity.getSourceChunkIds()) {
                if (!chunkIds.contains(chunkId)) {
                    throw new IOException("graph entity references unknown chunk");
                }
            }
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
        }
    }

    private static void validateExisting(Path path, IngestionContext context,
                                         List<EntityRef> entities, List<GraphEdgeRef> edges,
                                         List<TextChunk> chunks) throws IOException {
        String version = context.getGraphVersion().getVersion();
        try (GraphArtifactReader reader = new GraphArtifactReader(path, version, version)) {
            reader.getIdentity().validate(context.getGraphVersion().getGraphName(), version, version, chunks);
            reader.validateReferences(chunks);
            if (!entities.equals(reader.getEntities()) || !edges.equals(reader.getEdges())) {
                throw new IOException("existing graph artifact content mismatch");
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
