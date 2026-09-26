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

package org.apache.geaflow.ai.retrieval.index.vector;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.geaflow.ai.retrieval.index.IndexArtifact;
import org.apache.geaflow.ai.retrieval.index.VectorIndexBuilder;
import org.apache.geaflow.ai.retrieval.ingest.IngestionContext;
import org.apache.geaflow.ai.retrieval.metadata.IndexBuildMetadata;
import org.apache.geaflow.ai.retrieval.model.document.TextChunk;
import org.apache.geaflow.ai.retrieval.model.version.IndexVersion;

/** Builds a deterministic, service-independent vector artifact from precomputed vectors. */
public final class OfflineVectorIndexBuilder implements VectorIndexBuilder {

    private static final String INDEX_NAME = "vector";
    private static final String BUILDER_VERSION = "offline-vector-v1";
    private final Path outputDirectory;
    private final Map<String, float[]> vectors;
    private final String vectorSource;
    private final String vectorVersion;

    public OfflineVectorIndexBuilder(Path outputDirectory, Map<String, float[]> vectors,
                                     String vectorSource, String vectorVersion) {
        this.outputDirectory = Objects.requireNonNull(outputDirectory, "outputDirectory");
        this.vectors = Objects.requireNonNull(vectors, "vectors");
        this.vectorSource = requireText(vectorSource, "vectorSource");
        this.vectorVersion = requireText(vectorVersion, "vectorVersion");
    }

    @Override
    public IndexArtifact build(IngestionContext context, List<TextChunk> chunks) throws IOException {
        Objects.requireNonNull(context, "context");
        Objects.requireNonNull(chunks, "chunks");
        List<TextChunk> ordered = new ArrayList<>(chunks);
        ordered.sort(Comparator.comparing(TextChunk::getChunkId));
        int dimensions = validate(context, ordered);
        Files.createDirectories(outputDirectory);
        Path staging = Files.createTempFile(outputDirectory, INDEX_NAME + "-", ".tmp");
        Path published = outputDirectory.resolve(INDEX_NAME + "-" + context.getGraphVersion().getVersion() + ".bin");
        try (DataOutputStream output = new DataOutputStream(Files.newOutputStream(staging))) {
            output.writeUTF("GEAFLOW-VECTOR-1");
            output.writeUTF(vectorSource);
            output.writeUTF(vectorVersion);
            output.writeInt(dimensions);
            output.writeInt(ordered.size());
            for (TextChunk chunk : ordered) {
                output.writeUTF(chunk.getChunkId());
                output.writeUTF(chunk.getDocumentId());
                float[] vector = vectors.get(chunk.getChunkId());
                for (float value : vector) {
                    output.writeFloat(value);
                }
            }
        }
        if (Files.exists(published)) {
            Files.deleteIfExists(staging);
            return new VectorArtifact(new IndexBuildMetadata(context.getGraphVersion(),
                new IndexVersion(INDEX_NAME, context.getGraphVersion().getVersion(),
                    context.getGraphVersion().getVersion()), INDEX_NAME,
                BUILDER_VERSION + ":" + vectorSource + ":" + vectorVersion,
                published.toString(), true), published);
        }
        try {
            Files.move(staging, published, java.nio.file.StandardCopyOption.ATOMIC_MOVE);
        } catch (java.nio.file.AtomicMoveNotSupportedException unsupported) {
            Files.move(staging, published);
        }
        IndexVersion indexVersion = new IndexVersion(INDEX_NAME,
            context.getGraphVersion().getVersion(), context.getGraphVersion().getVersion());
        IndexBuildMetadata metadata = new IndexBuildMetadata(context.getGraphVersion(), indexVersion,
            INDEX_NAME, BUILDER_VERSION + ":" + vectorSource + ":" + vectorVersion,
            published.toString(), true);
        return new VectorArtifact(metadata, published);
    }

    private int validate(IngestionContext context, List<TextChunk> chunks) throws IOException {
        if (!vectorSource.equals(context.getManifest().getVectorSource())
            || !vectorVersion.equals(context.getManifest().getVectorVersion())) {
            throw new IOException("vector source/version does not match manifest");
        }
        if (chunks.isEmpty()) {
            throw new IOException("vector artifact cannot be empty");
        }
        int dimensions = -1;
        java.util.Set<String> chunkIds = new java.util.HashSet<>();
        for (TextChunk chunk : chunks) {
            if (!chunkIds.add(chunk.getChunkId())) {
                throw new IOException("duplicate chunk ID " + chunk.getChunkId());
            }
            float[] vector = vectors.get(chunk.getChunkId());
            if (vector == null) {
                throw new IOException("missing vector for chunk " + chunk.getChunkId());
            }
            if (dimensions < 0) {
                dimensions = vector.length;
            }
            if (vector.length != dimensions || vector.length == 0) {
                throw new IOException("vector dimensions do not align for chunk " + chunk.getChunkId());
            }
            for (float value : vector) {
                if (Float.isNaN(value) || Float.isInfinite(value)) {
                    throw new IOException("non-finite vector value for chunk " + chunk.getChunkId());
                }
            }
        }
        if (vectors.size() != chunks.size()) {
            throw new IOException("vector count does not match chunk count");
        }
        return dimensions;
    }

    private static String requireText(String value, String name) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }

    private static final class VectorArtifact implements IndexArtifact {
        private final IndexBuildMetadata metadata;
        private final Path path;

        private VectorArtifact(IndexBuildMetadata metadata, Path path) {
            this.metadata = metadata;
            this.path = path;
        }

        @Override
        public IndexBuildMetadata getMetadata() {
            return metadata;
        }

        @Override
        public void close() throws IOException {
            if (!Files.isRegularFile(path) || Files.size(path) == 0) {
                throw new IOException("vector artifact is not readable: " + path);
            }
            try (DataInputStream input = new DataInputStream(Files.newInputStream(path))) {
                if (!"GEAFLOW-VECTOR-1".equals(input.readUTF())) {
                    throw new IOException("invalid vector artifact header");
                }
                input.readUTF();
                input.readUTF();
                int dimensions = input.readInt();
                int count = input.readInt();
                if (dimensions <= 0 || count <= 0) {
                    throw new IOException("invalid vector artifact dimensions/count");
                }
            }
        }
    }
}
