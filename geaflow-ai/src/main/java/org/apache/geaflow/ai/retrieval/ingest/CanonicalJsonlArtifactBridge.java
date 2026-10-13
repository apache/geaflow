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

package org.apache.geaflow.ai.retrieval.ingest;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.geaflow.ai.retrieval.index.IndexArtifact;
import org.apache.geaflow.ai.retrieval.index.bm25.LuceneBm25IndexBuilder;
import org.apache.geaflow.ai.retrieval.index.graph.OfflineGraphArtifactBuilder;
import org.apache.geaflow.ai.retrieval.index.vector.OfflineVectorIndexBuilder;
import org.apache.geaflow.ai.retrieval.metadata.DatasetManifest;
import org.apache.geaflow.ai.retrieval.model.document.TextChunk;
import org.apache.geaflow.ai.retrieval.model.graph.EntityRef;
import org.apache.geaflow.ai.retrieval.model.graph.GraphEdgeRef;
import org.apache.geaflow.ai.retrieval.model.graph.GraphVertexRef;
import org.apache.geaflow.ai.retrieval.model.version.GraphVersion;

/** Converts a verified Python canonical JSONL release into native Java retrieval artifacts. */
public final class CanonicalJsonlArtifactBridge {

    private CanonicalJsonlArtifactBridge() {
    }

    /** Builds graph, BM25, and vector artifacts from a READY Python release. */
    public static void build(Path releaseDirectory, Path outputDirectory,
                             DatasetManifest manifest, GraphVersion graphVersion,
                             String indexVersion, Map<String, float[]> vectors,
                             String vectorSource, String vectorVersion) throws IOException {
        if (releaseDirectory == null || outputDirectory == null || manifest == null
            || graphVersion == null || vectors == null) {
            throw new IllegalArgumentException("release, output, manifest, graph, and vectors are required");
        }
        JsonObject ready = readObject(releaseDirectory.resolve("READY.json"));
        if (!"READY".equals(requiredString(ready, "state"))
            || !manifest.getDataset().equals(requiredString(ready, "dataset"))
            || !manifest.getDatasetRelease().equals(requiredString(ready, "dataset_release"))
            || !manifest.getSplit().equals(requiredString(ready, "split"))
            || !manifest.getSha256().equals(requiredString(ready, "sha256"))
            || !manifest.getPreprocessingVersion().equals(requiredString(ready, "preprocessing_version"))
            || !manifest.getChunking().getPolicyVersion().equals(requiredString(ready, "policy_version"))
            || !manifest.getGraphSchemaVersion().equals(requiredString(ready, "entity_schema_version"))
            || !Objects.equals(manifest.getVectorSource(), requiredString(ready, "vector_source"))
            || !Objects.equals(manifest.getVectorVersion(), requiredString(ready, "vector_version"))
            || manifest.getRandomSeed() != requiredLong(ready, "random_seed")) {
            throw new IOException("Python release metadata does not match the supplied dataset manifest");
        }
        if (!graphVersion.getVersion().equals(requiredString(ready, "version"))) {
            throw new IOException("supplied graph version does not match Python release version");
        }
        validateChecksums(releaseDirectory, ready.getAsJsonObject("artifact_checksums"));
        Path documentsPath = resolveReleasePath(releaseDirectory, "documents.jsonl");
        Path chunksPath = resolveReleasePath(releaseDirectory, "chunks.jsonl");
        if (countLines(documentsPath) != requiredInt(ready, "documents")) {
            throw new IOException("document count does not match Python READY metadata");
        }
        List<TextChunk> chunks = readChunks(chunksPath, ready);
        if (chunks.size() != requiredInt(ready, "chunks")) {
            throw new IOException("chunk count does not match Python READY metadata");
        }
        GraphRecords graph = readGraph(releaseDirectory, ready, chunks);
        IngestionContext context = new IngestionContext(manifest, graphVersion,
            requiredString(ready, "pipeline_version"), indexVersion);
        Files.createDirectories(outputDirectory);
        Path staging = Files.createTempDirectory(outputDirectory.toAbsolutePath().getParent(),
            outputDirectory.getFileName() + ".bridge-");
        try {
            try (org.apache.geaflow.ai.retrieval.ingest.GraphArtifact ignored =
                     new OfflineGraphArtifactBuilder(staging).build(context, chunks,
                         graph.entities, graph.vertices, graph.edges);
                 IndexArtifact bm25 = new LuceneBm25IndexBuilder(staging).build(context, chunks);
                 IndexArtifact vector = new OfflineVectorIndexBuilder(staging, vectors,
                     vectorSource, vectorVersion).build(context, chunks)) {
                if (!bm25.getMetadata().isReady() || !vector.getMetadata().isReady()) {
                    throw new IOException("Java artifact builders did not produce READY indexes");
                }
            }
            publishStaging(staging, outputDirectory);
        } finally {
            deleteTree(staging);
        }
    }

    private static List<TextChunk> readChunks(Path path, JsonObject ready) throws IOException {
        String preprocessingVersion = requiredString(ready, "preprocessing_version");
        String dataset = requiredString(ready, "dataset");
        String datasetRelease = requiredString(ready, "dataset_release");
        String split = requiredString(ready, "split");
        String policyVersion = requiredString(ready, "policy_version");
        List<TextChunk> chunks = new ArrayList<>();
        Set<String> ids = new HashSet<>();
        Set<String> positions = new HashSet<>();
        try (java.util.stream.Stream<String> lines = Files.lines(path, StandardCharsets.UTF_8)) {
            java.util.Iterator<String> iterator = lines.iterator();
            while (iterator.hasNext()) {
                JsonObject value = parseObject(iterator.next(), path);
                if (!preprocessingVersion.equals(requiredString(value, "normalization_version"))) {
                    throw new IOException("chunk normalization version does not match READY metadata");
                }
                if (!dataset.equals(requiredString(value, "dataset"))
                    || !datasetRelease.equals(requiredString(value, "dataset_release"))
                    || !split.equals(requiredString(value, "split"))
                    || !policyVersion.equals(requiredString(value, "policy_version"))) {
                    throw new IOException("chunk provenance or policy does not match READY metadata");
                }
                String text = requiredString(value, "text");
                String textHash = requiredString(value, "text_hash");
                if (!textHash.equals(sha256(text.getBytes(StandardCharsets.UTF_8)))) {
                    throw new IOException("chunk text hash does not match canonical text");
                }
                String documentId = requiredString(value, "document_id");
                int chunkIndex = requiredInt(value, "chunk_index");
                String expectedChunkId = sha256((documentId + "\0" + chunkIndex + "\0"
                    + textHash + "\0" + policyVersion).getBytes(StandardCharsets.UTF_8));
                if (!expectedChunkId.equals(requiredString(value, "chunk_id"))) {
                    throw new IOException("chunk ID does not match canonical content");
                }
                TextChunk chunk = new TextChunk(requiredString(value, "chunk_id"),
                    documentId, chunkIndex,
                    requiredInt(value, "start_offset"), requiredInt(value, "end_offset"),
                    requiredInt(value, "token_estimate"), text,
                    policyVersion, textHash,
                    requiredString(value, "source_uri"), requiredString(value, "dataset"),
                    requiredString(value, "dataset_release"), requiredString(value, "split"),
                    requiredString(value, "source_hash"));
                if (!ids.add(chunk.getChunkId())) {
                    throw new IOException("duplicate canonical chunk ID " + chunk.getChunkId());
                }
                String position = chunk.getDocumentId().length() + ":" + chunk.getDocumentId()
                    + chunk.getChunkIndex();
                if (!positions.add(position)) {
                    throw new IOException("duplicate canonical chunk position " + position);
                }
                chunks.add(chunk);
            }
        }
        if (chunks.isEmpty()) {
            throw new IOException("Python release has no chunks");
        }
        return chunks;
    }

    private static GraphRecords readGraph(Path root, JsonObject ready, List<TextChunk> chunks)
        throws IOException {
        Path graphManifestPath = resolveReleasePath(root, requiredString(ready, "graph_manifest"));
        JsonObject graphManifest = readObject(graphManifestPath);
        if (!requiredString(ready, "version").equals(requiredString(graphManifest, "graph_version"))
            || !requiredString(ready, "entity_schema_version")
            .equals(requiredString(graphManifest, "schema_version"))
            || !requiredString(ready, "entity_extractor_version")
            .equals(requiredString(graphManifest, "extractor_version"))) {
            throw new IOException("Python graph manifest identity does not match READY metadata");
        }
        Path graphDirectory = graphManifestPath.getParent();
        String graphRelativeDirectory = root.relativize(graphDirectory).toString();
        Map<String, TextChunk> chunksById = new HashMap<>();
        for (TextChunk chunk : chunks) {
            chunksById.put(chunk.getChunkId(), chunk);
        }
        List<EntityRef> entities = new ArrayList<>();
        List<GraphVertexRef> vertices = new ArrayList<>();
        Set<String> entityIds = new HashSet<>();
        Set<String> vertexIds = new HashSet<>();
        Map<String, String> vertexTypes = new HashMap<>();
        int vertexRecordCount = 0;
        Path verticesPath = resolveReleasePath(root,
            Paths.get(graphRelativeDirectory, "vertices.jsonl").toString());
        if (!requiredString(graphManifest, "vertices_sha256").equals(sha256(verticesPath))) {
            throw new IOException("graph vertices checksum does not match graph manifest");
        }
        try (java.util.stream.Stream<String> lines = Files.lines(verticesPath, StandardCharsets.UTF_8)) {
            java.util.Iterator<String> iterator = lines.iterator();
            while (iterator.hasNext()) {
                JsonObject value = parseObject(iterator.next(), verticesPath);
                vertexRecordCount++;
                String vertexId = requiredString(value, "vertex_id");
                String vertexType = requiredString(value, "vertex_type");
                if (!vertexIds.add(vertexId)) {
                    throw new IOException("duplicate canonical vertex ID " + vertexId);
                }
                vertexTypes.put(vertexId, vertexType);
                if (!"ENTITY".equals(vertexType)) {
                    continue;
                }
                String entityId = requiredString(value, "entity_id");
                List<String> sourceChunks = strings(value, "source_chunk_ids");
                EntityRef entity = new EntityRef(entityId, requiredString(value, "canonical_name"),
                    Collections.emptyList(), requiredString(value, "entity_type"), sourceChunks,
                    requiredString(value, "dataset"), requiredString(value, "dataset_release"),
                    requiredString(value, "split"), requiredString(value, "source_hash"));
                if (!entityIds.add(entityId)) {
                    throw new IOException("duplicate canonical entity ID " + entityId);
                }
                validateGraphSource(entity.getSourceChunkIds(), entity.getSourceHash(), chunksById,
                    "entity " + entityId);
                entities.add(entity);
                vertices.add(new GraphVertexRef(requiredString(value, "entity_type"),
                    vertexId, entityId, entity.getDataset(),
                    entity.getDatasetVersion(), entity.getSplit(), entity.getSourceHash()));
            }
        }
        List<GraphEdgeRef> edges = new ArrayList<>();
        Set<String> edgeIds = new HashSet<>();
        int edgeRecordCount = 0;
        int skippedMentionEdges = 0;
        Path edgesPath = resolveReleasePath(root,
            Paths.get(graphRelativeDirectory, "edges.jsonl").toString());
        if (!requiredString(graphManifest, "edges_sha256").equals(sha256(edgesPath))) {
            throw new IOException("graph edges checksum does not match graph manifest");
        }
        try (java.util.stream.Stream<String> lines = Files.lines(edgesPath, StandardCharsets.UTF_8)) {
            java.util.Iterator<String> iterator = lines.iterator();
            while (iterator.hasNext()) {
                JsonObject value = parseObject(iterator.next(), edgesPath);
                edgeRecordCount++;
                String edgeId = requiredString(value, "edge_id");
                if (!edgeIds.add(edgeId)) {
                    throw new IOException("duplicate canonical edge ID " + edgeId);
                }
                String source = requiredString(value, "source_vertex_id");
                String target = requiredString(value, "target_vertex_id");
                if (!entityIds.contains(source) || !entityIds.contains(target)) {
                    if (!"mentions".equals(requiredString(value, "relation_type"))
                        || !"CHUNK".equals(vertexTypes.get(source)) || !entityIds.contains(target)) {
                        throw new IOException("graph edge references a non-entity vertex");
                    }
                    validateGraphSource(strings(value, "source_chunk_ids"),
                        requiredString(value, "source_hash"), chunksById, "edge " + edgeId);
                    skippedMentionEdges++;
                    continue;
                }
                GraphEdgeRef edge = new GraphEdgeRef(edgeId,
                    requiredString(value, "relation_type"), source, target,
                    strings(value, "source_chunk_ids"), requiredString(value, "dataset"),
                    requiredString(value, "dataset_release"), requiredString(value, "split"),
                    requiredString(value, "source_hash"));
                validateGraphSource(edge.getSourceChunkIds(), edge.getSourceHash(), chunksById,
                    "edge " + edge.getEdgeId());
                edges.add(edge);
            }
        }
        if (entities.isEmpty()) {
            throw new IOException("Python graph release has no entity vertices");
        }
        if (vertexRecordCount != requiredInt(graphManifest, "vertex_count")
            || edgeRecordCount != requiredInt(graphManifest, "edge_count")
            || vertexRecordCount != requiredInt(ready, "vertices")
            || edgeRecordCount != requiredInt(ready, "edges")
            || edgeRecordCount != edges.size() + skippedMentionEdges) {
            throw new IOException("graph counts do not match graph manifest");
        }
        return new GraphRecords(entities, vertices, edges);
    }

    private static void validateGraphSource(List<String> sourceChunkIds, String sourceHash,
                                            Map<String, TextChunk> chunks, String owner)
        throws IOException {
        if (sourceChunkIds.isEmpty()) {
            throw new IOException(owner + " has no supporting source chunks");
        }
        List<String> hashes = new ArrayList<>();
        for (String chunkId : sourceChunkIds) {
            TextChunk chunk = chunks.get(chunkId);
            if (chunk == null) {
                throw new IOException(owner + " references missing chunk " + chunkId);
            }
            hashes.add(chunk.getSourceHash());
        }
        if (!sourceHash.equals(org.apache.geaflow.ai.retrieval.model.document.RetrievalIds
            .aggregateSourceHash(hashes))) {
            throw new IOException(owner + " source hash does not match supporting chunks");
        }
    }

    private static void validateChecksums(Path root, JsonObject checksums) throws IOException {
        if (checksums == null || checksums.entrySet().isEmpty()) {
            throw new IOException("Python READY metadata has no artifact checksums");
        }
        Set<String> expectedPaths = new HashSet<>();
        for (Map.Entry<String, JsonElement> entry : checksums.entrySet()) {
            Path path = resolveReleasePath(root, entry.getKey());
            String expectedDigest = entry.getValue().getAsString();
            if (!Files.isRegularFile(path) || !expectedDigest.equals(sha256(path))) {
                throw new IOException("Python release checksum mismatch: " + entry.getKey());
            }
            expectedPaths.add(entry.getKey());
        }
        Set<String> actual = new HashSet<>();
        try (java.util.stream.Stream<Path> paths = Files.walk(root)) {
            paths.filter(Files::isRegularFile).forEach(path -> {
                if (!path.equals(root.resolve("READY.json"))) {
                    actual.add(root.relativize(path).toString().replace(path.getFileSystem().getSeparator(), "/"));
                }
            });
        }
        if (!actual.equals(expectedPaths)) {
            throw new IOException("Python READY checksum manifest does not cover release files");
        }
    }

    private static Path resolveReleasePath(Path root, String relative) throws IOException {
        Path realRoot = root.toRealPath();
        Path resolved = root.resolve(relative).normalize();
        if (!resolved.startsWith(root.normalize()) || !Files.exists(resolved)
            || !resolved.toRealPath().startsWith(realRoot)) {
            throw new IOException("release path escapes its root: " + relative);
        }
        return resolved;
    }

    private static void publishStaging(Path staging, Path output) throws IOException {
        List<Path> files = new ArrayList<>();
        try (java.util.stream.Stream<Path> paths = Files.walk(staging)) {
            paths.filter(Files::isRegularFile).forEach(files::add);
        }
        List<Path> published = new ArrayList<>();
        try {
            for (Path file : files) {
                Path target = output.resolve(staging.relativize(file));
                Files.createDirectories(target.getParent());
                if (Files.exists(target)) {
                    if (!sha256(file).equals(sha256(target))) {
                        throw new IOException("existing Java artifact conflicts with staged output");
                    }
                    continue;
                }
                try {
                    Files.move(file, target, StandardCopyOption.ATOMIC_MOVE);
                } catch (java.nio.file.AtomicMoveNotSupportedException unsupported) {
                    Files.move(file, target);
                }
                published.add(target);
            }
        } catch (IOException failure) {
            for (Path path : published) {
                Files.deleteIfExists(path);
            }
            throw failure;
        }
    }

    private static void deleteTree(Path root) throws IOException {
        if (!Files.exists(root)) {
            return;
        }
        try (java.util.stream.Stream<Path> paths = Files.walk(root)) {
            List<Path> entries = paths.sorted(java.util.Comparator.reverseOrder()).collect(java.util.stream.Collectors.toList());
            for (Path entry : entries) {
                Files.deleteIfExists(entry);
            }
        }
    }

    private static String sha256(Path path) throws IOException {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            try (java.io.InputStream input = Files.newInputStream(path)) {
                byte[] buffer = new byte[8192];
                int count;
                while ((count = input.read(buffer)) >= 0) {
                    digest.update(buffer, 0, count);
                }
            }
            StringBuilder result = new StringBuilder();
            for (byte value : digest.digest()) {
                result.append(String.format(java.util.Locale.ROOT, "%02x", value & 0xff));
            }
            return result.toString();
        } catch (NoSuchAlgorithmException error) {
            throw new IllegalStateException("SHA-256 is unavailable", error);
        }
    }

    private static String sha256(byte[] value) throws IOException {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] result = digest.digest(value);
            StringBuilder hex = new StringBuilder();
            for (byte current : result) {
                hex.append(String.format(java.util.Locale.ROOT, "%02x", current & 0xff));
            }
            return hex.toString();
        } catch (NoSuchAlgorithmException error) {
            throw new IOException("SHA-256 is unavailable", error);
        }
    }

    private static int countLines(Path path) throws IOException {
        try (java.util.stream.Stream<String> lines = Files.lines(path, StandardCharsets.UTF_8)) {
            return Math.toIntExact(lines.count());
        }
    }

    private static JsonObject readObject(Path path) throws IOException {
        return parseObject(new String(Files.readAllBytes(path), StandardCharsets.UTF_8), path);
    }

    private static JsonObject parseObject(String json, Path path) throws IOException {
        try {
            JsonElement parsed = new JsonParser().parse(json);
            if (!parsed.isJsonObject()) {
                throw new IOException("expected JSON object in " + path);
            }
            return parsed.getAsJsonObject();
        } catch (RuntimeException error) {
            throw new IOException("invalid JSON in " + path, error);
        }
    }

    private static String requiredString(JsonObject object, String field) throws IOException {
        JsonElement value = object.get(field);
        if (value == null || value.isJsonNull() || value.getAsString().trim().isEmpty()) {
            throw new IOException("missing required field " + field);
        }
        return value.getAsString();
    }

    private static int requiredInt(JsonObject object, String field) throws IOException {
        try {
            return object.get(field).getAsInt();
        } catch (RuntimeException error) {
            throw new IOException("missing or invalid integer field " + field, error);
        }
    }

    private static long requiredLong(JsonObject object, String field) throws IOException {
        try {
            return object.get(field).getAsLong();
        } catch (RuntimeException error) {
            throw new IOException("missing or invalid long field " + field, error);
        }
    }

    private static List<String> strings(JsonObject object, String field) throws IOException {
        JsonElement value = object.get(field);
        if (value == null || !value.isJsonArray()) {
            throw new IOException("missing or invalid string array " + field);
        }
        List<String> result = new ArrayList<>();
        for (JsonElement item : value.getAsJsonArray()) {
            if (!item.isJsonPrimitive() || !item.getAsJsonPrimitive().isString()) {
                throw new IOException("invalid string array item in " + field);
            }
            result.add(item.getAsString());
        }
        return result;
    }

    private static final class GraphRecords {
        private final List<EntityRef> entities;
        private final List<GraphVertexRef> vertices;
        private final List<GraphEdgeRef> edges;

        private GraphRecords(List<EntityRef> entities, List<GraphVertexRef> vertices,
                             List<GraphEdgeRef> edges) {
            this.entities = entities;
            this.vertices = vertices;
            this.edges = edges;
        }
    }
}
