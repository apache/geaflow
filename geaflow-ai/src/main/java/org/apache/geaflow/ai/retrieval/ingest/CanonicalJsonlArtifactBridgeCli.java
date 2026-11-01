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
 */

package org.apache.geaflow.ai.retrieval.ingest;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.geaflow.ai.retrieval.channel.graph.GraphDelta;
import org.apache.geaflow.ai.retrieval.channel.graph.GraphSnapshot;
import org.apache.geaflow.ai.retrieval.codec.GraphDeltaJson;
import org.apache.geaflow.ai.retrieval.index.graph.GraphArtifactReader;
import org.apache.geaflow.ai.retrieval.metadata.DatasetManifest;
import org.apache.geaflow.ai.retrieval.metadata.MetadataJson;
import org.apache.geaflow.ai.retrieval.model.document.TextChunk;
import org.apache.geaflow.ai.retrieval.model.graph.EntityRef;
import org.apache.geaflow.ai.retrieval.model.graph.GraphEdgeRef;
import org.apache.geaflow.ai.retrieval.model.graph.GraphVertexRef;
import org.apache.geaflow.ai.retrieval.model.version.GraphVersion;

/** Generic command-line adapter for publishing native artifacts from a canonical release. */
public final class CanonicalJsonlArtifactBridgeCli {

    private CanonicalJsonlArtifactBridgeCli() {
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> options = options(args);
        Path release = requiredPath(options, "release");
        Path output = requiredPath(options, "output");
        DatasetManifest manifest = MetadataJson.fromJson(
            Files.readString(requiredPath(options, "manifest"), StandardCharsets.UTF_8),
            DatasetManifest.class);
        String graphName = required(options, "graph-name");
        String graphVersion = required(options, "graph-version");
        String indexVersion = required(options, "index-version");
        Path parent = output.toAbsolutePath().getParent();
        Files.createDirectories(parent);
        Path work = Files.createTempDirectory(parent, output.getFileName() + ".build-");
        Path staging = Files.createTempDirectory(parent, output.getFileName() + ".publish-");
        try {
            Map<String, float[]> vectors = readVectors(requiredPath(options, "vectors"));
            CanonicalJsonlArtifactBridge.build(release, work, manifest,
                new GraphVersion(graphName, graphVersion), indexVersion, vectors,
                required(options, "vector-source"), required(options, "vector-version"));
            String parentBundle = options.get("parent-bundle");
            assembleBundle(release, work, staging, graphName, graphVersion, indexVersion,
                parentBundle == null ? null : Path.of(parentBundle).toAbsolutePath().normalize());
            publish(staging, output);
        } finally {
            deleteTree(work);
            deleteTree(staging);
        }
    }

    private static Map<String, float[]> readVectors(Path path) throws IOException {
        Map<String, float[]> result = new HashMap<>();
        try (BufferedReader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty()) {
                    continue;
                }
                JsonObject value = new JsonParser().parse(line).getAsJsonObject();
                String id = value.get("chunkId").getAsString();
                JsonArray array = value.getAsJsonArray("vector");
                float[] vector = new float[array.size()];
                for (int index = 0; index < vector.length; index++) {
                    vector[index] = array.get(index).getAsFloat();
                }
                if (result.put(id, vector) != null) {
                    throw new IOException("duplicate vector chunkId: " + id);
                }
            }
        }
        return result;
    }

    private static void assembleBundle(Path release, Path work, Path staging,
                                       String graphName, String graphVersion,
                                       String indexVersion, Path parentBundle) throws IOException {
        copyTree(release, staging);
        copy(find(work, "graph-", graphVersion), staging.resolve("graph.bin"));
        copy(find(work, "vector-", indexVersion), staging.resolve("vector.bin"));
        copyTree(findDirectory(work, "bm25-", indexVersion), staging.resolve("bm25"));
        JsonObject ready = new JsonParser().parse(Files.readString(release.resolve("READY.json"),
            StandardCharsets.UTF_8)).getAsJsonObject();
        ready.addProperty("graph_name", graphName);
        ready.addProperty("index_version", indexVersion);
        ready.addProperty("graph_artifact", "graph.bin");
        ready.addProperty("vector_artifact", "vector.bin");
        ready.addProperty("bm25_artifact", "bm25");
        Path qualityPath = staging.resolve(ready.get("quality_report").getAsString());
        JsonObject quality = new JsonParser().parse(Files.readString(qualityPath,
            StandardCharsets.UTF_8)).getAsJsonObject();
        int chunks = ready.get("chunks").getAsInt();
        JsonObject coverage = new JsonObject();
        coverage.add("bm25", coverage(chunks));
        coverage.add("vector", coverage(chunks));
        quality.add("index_coverage", coverage);
        Files.writeString(qualityPath, quality.toString() + "\n", StandardCharsets.UTF_8);
        if (parentBundle != null) {
            addDeltaBundle(parentBundle, staging, work, ready, graphName, graphVersion, indexVersion);
        }
        JsonObject checksums = new JsonObject();
        try (Stream<Path> paths = Files.walk(staging)) {
            paths.filter(Files::isRegularFile).filter(path -> !path.getFileName().toString().equals("READY.json"))
                .forEach(path -> checksums.addProperty(staging.relativize(path).toString().replace('\\', '/'),
                    sha256(path)));
        }
        ready.add("artifact_checksums", checksums);
        Files.writeString(staging.resolve("READY.json"), ready.toString() + "\n", StandardCharsets.UTF_8);
    }

    private static void addDeltaBundle(Path parentBundle, Path staging, Path work, JsonObject ready,
                                       String graphName, String graphVersion, String indexVersion)
        throws IOException {
        JsonObject parentReady = new JsonParser().parse(Files.readString(
            parentBundle.resolve("READY.json"), StandardCharsets.UTF_8)).getAsJsonObject();
        if (!"READY".equals(parentReady.get("state").getAsString())
            || !graphName.equals(parentReady.has("graph_name")
            ? parentReady.get("graph_name").getAsString() : parentReady.get("dataset").getAsString())) {
            throw new IOException("parent bundle is not READY for the configured graph");
        }
        if (!validateBundle(parentBundle, parentReady)) {
            throw new IOException("parent bundle checksums are invalid");
        }
        String parentVersion = parentReady.get("version").getAsString();
        String parentIndexVersion = parentReady.has("index_version")
            ? parentReady.get("index_version").getAsString() : parentReady.get("version").getAsString();
        Path parentGraph = parentBundle.resolve(parentReady.has("graph_artifact")
            ? parentReady.get("graph_artifact").getAsString()
            : "graph-" + parentVersion + ".bin").normalize();
        Path parentChunks = parentBundle.resolve("chunks.jsonl").normalize();
        if (!parentGraph.startsWith(parentBundle) || !Files.isRegularFile(parentGraph)
            || !Files.isRegularFile(parentChunks)) {
            throw new IOException("parent bundle is missing graph or chunk artifacts");
        }
        Path currentGraph = find(work, "graph-", graphVersion);
        GraphSnapshot parentSnapshot;
        GraphSnapshot currentSnapshot;
        try (GraphArtifactReader reader = new GraphArtifactReader(parentGraph,
            parentVersion, parentIndexVersion)) {
            parentSnapshot = reader.toSnapshot(readChunks(parentChunks));
        }
        try (GraphArtifactReader reader = new GraphArtifactReader(currentGraph,
            graphVersion, indexVersion)) {
            currentSnapshot = reader.toSnapshot(readChunks(staging.resolve("chunks.jsonl")));
        }
        final GraphDelta delta = diff(parentSnapshot, currentSnapshot);
        Path lineage = staging.resolve("lineage");
        Files.createDirectories(lineage);
        copy(parentGraph, lineage.resolve("snapshot-graph.bin"));
        copy(parentChunks, lineage.resolve("snapshot-chunks.jsonl"));
        Path deltaPath = staging.resolve("graph/delta/" + graphVersion + ".jsonl");
        Files.createDirectories(deltaPath.getParent());
        StringWriter serialized = new StringWriter();
        GraphDeltaJson.write(delta, serialized);
        Files.writeString(deltaPath, serialized.toString(), StandardCharsets.UTF_8);

        JsonObject snapshot = new JsonObject();
        snapshot.addProperty("version", parentVersion);
        snapshot.addProperty("index_version", parentIndexVersion);
        snapshot.addProperty("artifact", "lineage/snapshot-graph.bin");
        snapshot.addProperty("chunks", "lineage/snapshot-chunks.jsonl");
        ready.add("graph_snapshot", snapshot);
        final JsonArray chain = new JsonArray();
        JsonObject item = new JsonObject();
        item.add("graph_version", graphVersion(graphName, graphVersion));
        item.add("parent_graph_version", graphVersion(graphName, parentVersion));
        item.addProperty("artifact", staging.relativize(deltaPath).toString().replace('\\', '/'));
        item.addProperty("digest", delta.getDigest());
        item.addProperty("statistics_version", currentSnapshot.getStatistics().getStatisticsVersion());
        chain.add(item);
        ready.add("graph_delta_chain", chain);
        ready.add("parent_graph_version", graphVersion(graphName, parentVersion));
        ready.addProperty("delta_digest", delta.getDigest());
        ready.addProperty("statistics_version", currentSnapshot.getStatistics().getStatisticsVersion());
    }

    private static JsonObject graphVersion(String graphName, String version) {
        JsonObject value = new JsonObject();
        value.addProperty("graphName", graphName);
        value.addProperty("version", version);
        return value;
    }

    private static GraphDelta diff(GraphSnapshot parent, GraphSnapshot current) {
        GraphDelta.Builder builder = GraphDelta.builder(current.getGraphVersion(), parent.getGraphVersion());
        diffValues(parent.getChunks(), current.getChunks(), TextChunk::getChunkId,
            builder::upsertChunk, builder::deleteChunk);
        diffValues(parent.getEntities(), current.getEntities(), EntityRef::getEntityId,
            builder::upsertEntity, builder::deleteEntity);
        diffValues(parent.getVertices(), current.getVertices(), GraphVertexRef::getVertexId,
            builder::upsertVertex, builder::deleteVertex);
        diffValues(parent.getEdges(), current.getEdges(), GraphEdgeRef::getEdgeId,
            builder::upsertEdge, builder::deleteEdge);
        return builder.build();
    }

    private static <T> void diffValues(List<T> previous, List<T> current,
                                       java.util.function.Function<T, String> key,
                                       java.util.function.Consumer<T> upsert,
                                       java.util.function.Consumer<String> delete) {
        Map<String, T> oldById = new HashMap<>();
        Map<String, T> newById = new HashMap<>();
        previous.forEach(value -> oldById.put(key.apply(value), value));
        current.forEach(value -> newById.put(key.apply(value), value));
        for (Map.Entry<String, T> entry : newById.entrySet()) {
            if (!entry.getValue().equals(oldById.get(entry.getKey()))) {
                upsert.accept(entry.getValue());
            }
        }
        for (String id : oldById.keySet()) {
            if (!newById.containsKey(id)) {
                delete.accept(id);
            }
        }
    }

    private static List<TextChunk> readChunks(Path path) throws IOException {
        List<TextChunk> result = new ArrayList<>();
        try (BufferedReader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty()) {
                    continue;
                }
                JsonObject chunk = new JsonParser().parse(line).getAsJsonObject();
                result.add(new TextChunk(chunk.get("chunk_id").getAsString(),
                    chunk.get("document_id").getAsString(), chunk.get("chunk_index").getAsInt(),
                    chunk.get("start_offset").getAsInt(), chunk.get("end_offset").getAsInt(),
                    chunk.get("token_estimate").getAsInt(), chunk.get("text").getAsString(),
                    optional(chunk, "policy_version"), optional(chunk, "text_hash"),
                    optional(chunk, "source_uri"), optional(chunk, "dataset"),
                    optional(chunk, "dataset_release"), optional(chunk, "split"),
                    optional(chunk, "source_hash")));
            }
        }
        return result;
    }

    private static String optional(JsonObject object, String key) {
        return object.has(key) && !object.get(key).isJsonNull()
            ? object.get(key).getAsString() : null;
    }

    private static JsonObject coverage(int chunks) {
        JsonObject result = new JsonObject();
        result.addProperty("covered_chunks", chunks);
        result.addProperty("total_chunks", chunks);
        result.addProperty("ratio", chunks == 0 ? 0.0 : 1.0);
        return result;
    }

    private static void publish(Path staging, Path output) throws IOException {
        if (Files.exists(output)) {
            Path ready = output.resolve("READY.json");
            if (Files.isRegularFile(ready)) {
                JsonObject existing = new JsonParser().parse(
                    Files.readString(ready, StandardCharsets.UTF_8)).getAsJsonObject();
                JsonObject candidate = new JsonParser().parse(Files.readString(
                    staging.resolve("READY.json"), StandardCharsets.UTF_8)).getAsJsonObject();
                if (sameRelease(existing, candidate) && validateBundle(output, existing)) {
                    return;
                }
            }
            throw new IOException("bundle output already exists with different contents: " + output);
        }
        try {
            Files.move(staging, output, StandardCopyOption.ATOMIC_MOVE);
        } catch (java.nio.file.AtomicMoveNotSupportedException unsupported) {
            Files.move(staging, output);
        }
    }

    private static boolean sameRelease(JsonObject existing, JsonObject candidate) {
        for (String key : new String[] {"state", "dataset", "dataset_release", "split", "sha256",
            "version", "chunks", "documents", "vector_source", "vector_version", "policy_version",
            "entity_schema_version", "random_seed"}) {
            if (!existing.has(key) || !candidate.has(key)
                || !existing.get(key).equals(candidate.get(key))) {
                return false;
            }
        }
        for (String key : new String[] {"index_version", "parent_graph_version", "delta_digest",
            "statistics_version"}) {
            if (existing.has(key) || candidate.has(key)) {
                if (!existing.has(key) || !candidate.has(key)
                    || !existing.get(key).equals(candidate.get(key))) {
                    return false;
                }
            }
        }
        return "READY".equals(existing.get("state").getAsString());
    }

    private static boolean validateBundle(Path root, JsonObject ready) throws IOException {
        JsonObject checksums = ready.getAsJsonObject("artifact_checksums");
        if (checksums == null) {
            return false;
        }
        for (Map.Entry<String, com.google.gson.JsonElement> entry : checksums.entrySet()) {
            Path artifact = root.resolve(entry.getKey()).normalize();
            if (!artifact.startsWith(root.normalize()) || !Files.isRegularFile(artifact)
                || !entry.getValue().getAsString().equals(sha256(artifact))) {
                return false;
            }
        }
        try (Stream<Path> paths = Files.walk(root)) {
            long actual = paths.filter(Files::isRegularFile)
                .filter(path -> !path.equals(root.resolve("READY.json"))).count();
            return actual == checksums.entrySet().size();
        }
    }

    private static void copy(Path source, Path target) throws IOException {
        if (!Files.isRegularFile(source) || Files.size(source) == 0) {
            throw new IOException("source artifact is empty: " + source);
        }
        Files.createDirectories(target.getParent());
        Files.copy(source, target);
    }

    private static void copyTree(Path source, Path target) throws IOException {
        try (Stream<Path> paths = Files.walk(source)) {
            for (Path path : (Iterable<Path>) paths::iterator) {
                Path destination = target.resolve(source.relativize(path));
                if (Files.isDirectory(path)) {
                    Files.createDirectories(destination);
                } else {
                    Files.createDirectories(destination.getParent());
                    Files.copy(path, destination, StandardCopyOption.REPLACE_EXISTING);
                }
            }
        }
    }

    private static void deleteTree(Path root) throws IOException {
        if (!Files.exists(root)) {
            return;
        }
        try (Stream<Path> paths = Files.walk(root)) {
            List<Path> entries = new ArrayList<>();
            paths.sorted(Comparator.reverseOrder()).forEach(entries::add);
            for (Path entry : entries) {
                Files.deleteIfExists(entry);
            }
        }
    }

    private static String sha256(Path path) {
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
                result.append(String.format("%02x", value));
            }
            return result.toString();
        } catch (Exception error) {
            throw new IllegalStateException("cannot hash artifact", error);
        }
    }

    private static Path find(Path root, String prefix, String version) throws IOException {
        try (Stream<Path> paths = Files.walk(root)) {
            return paths.filter(Files::isRegularFile)
                .filter(path -> path.toString().contains(prefix + version))
                .filter(path -> !path.getFileName().toString().endsWith(".lock"))
                .findFirst()
                .orElseThrow(() -> new IOException("missing artifact: " + prefix + version));
        }
    }

    private static Path findDirectory(Path root, String prefix, String version) throws IOException {
        try (Stream<Path> paths = Files.walk(root)) {
            return paths.filter(Files::isDirectory)
                .filter(path -> path.getFileName().toString().startsWith(prefix + version))
                .findFirst()
                .orElseThrow(() -> new IOException("missing artifact directory: " + prefix + version));
        }
    }

    private static Map<String, String> options(String[] args) {
        Map<String, String> result = new HashMap<>();
        for (int index = 0; index < args.length; index += 2) {
            if (index + 1 >= args.length || !args[index].startsWith("--")) {
                throw new IllegalArgumentException("arguments must be --name value pairs");
            }
            result.put(args[index].substring(2), args[index + 1]);
        }
        return result;
    }

    private static Path requiredPath(Map<String, String> options, String name) {
        return Path.of(required(options, name));
    }

    private static String required(Map<String, String> options, String name) {
        String value = options.get(name);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("missing --" + name);
        }
        return value;
    }
}
