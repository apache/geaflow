/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 */
package org.apache.geaflow.ai.retrieval.service;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalErrorCode;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalException;
import org.apache.geaflow.ai.retrieval.channel.graph.GraphDelta;
import org.apache.geaflow.ai.retrieval.channel.graph.GraphNeighborProvider;
import org.apache.geaflow.ai.retrieval.channel.graph.GraphSnapshot;
import org.apache.geaflow.ai.retrieval.channel.graph.VersionedGraphView;
import org.apache.geaflow.ai.retrieval.codec.GraphDeltaJson;
import org.apache.geaflow.ai.retrieval.config.RetrievalProperties;
import org.apache.geaflow.ai.retrieval.index.graph.GraphArtifactReader;
import org.apache.geaflow.ai.retrieval.model.document.TextChunk;
import org.apache.geaflow.ai.retrieval.model.version.GraphVersion;
import org.noear.solon.annotation.Component;
import org.noear.solon.annotation.Init;
import org.noear.solon.annotation.Inject;

/** Loads one immutable on-disk GraphRAG bundle during service startup. */
@Component
public final class ArtifactBundleLoader {

    @Inject
    private RetrievalProperties properties;
    @Inject
    private RetrievalFixtureRegistry registry;

    @Init
    public void initialize() {
        String root = properties.getArtifactBundleRoot();
        if (root == null || root.trim().isEmpty()) {
            return;
        }
        try {
            load(Path.of(root.trim()));
        } catch (IOException | RuntimeException error) {
            throw new RetrievalException(RetrievalErrorCode.INDEX_NOT_READY,
                "unable to load retrieval artifact bundle", error);
        }
    }

    void load(Path root) throws IOException {
        if (root == null || !Files.isDirectory(root)) {
            throw new IOException("artifact bundle directory is missing: " + root);
        }
        JsonObject ready = readObject(root.resolve("READY.json"));
        require(ready, "READY".equals(text(ready, "state")), "bundle is not READY");
        String graphName = properties.getArtifactGraphName();
        String graphVersion = properties.getArtifactGraphVersion();
        String indexVersion = properties.getArtifactIndexVersion();
        require(ready, graphName.equals(text(ready, "dataset")) || graphName.equals(text(ready, "graph_name")),
            "configured graph name does not match bundle");
        String readyVersion = text(ready, "version");
        require(ready, graphVersion.equals(readyVersion), "configured graph version does not match bundle");
        String readyIndexVersion = ready.has("index_version")
            ? text(ready, "index_version") : readyVersion;
        require(ready, indexVersion.equals(readyIndexVersion),
            "configured index version does not match bundle");
        String vectorSource = text(ready, "vector_source");
        String vectorVersion = text(ready, "vector_version");
        require(ready, properties.getArtifactVectorSource().equals(vectorSource),
            "configured vector source does not match bundle");
        require(ready, properties.getArtifactVectorVersion().equals(vectorVersion),
            "configured vector version does not match bundle");
        validateChecksums(root, ready);
        final List<TextChunk> chunks = readChunks(root.resolve("chunks.jsonl"));
        Path graph = root.resolve(properties.getArtifactGraphPath()).normalize();
        Path bm25 = root.resolve(properties.getArtifactBm25Path()).normalize();
        Path vector = root.resolve(properties.getArtifactVectorPath()).normalize();
        requireInside(root, graph);
        requireInside(root, bm25);
        requireInside(root, vector);
        if (ready.has("graph_delta_chain")) {
            GraphViewBundle view = readGraphView(root, ready, graphName, graphVersion);
            registry.loadBundle(graphName, graphVersion, indexVersion, bm25, vector, chunks,
                view.entities, view.edges, view.provider, vectorSource, vectorVersion);
        } else {
            registry.loadBundle(graphName, graphVersion, indexVersion, graph, bm25, vector, chunks,
                vectorSource, vectorVersion);
        }
    }

    private static GraphViewBundle readGraphView(Path root, JsonObject ready,
                                                 String graphName, String targetVersion)
        throws IOException {
        JsonObject snapshotMetadata = ready.getAsJsonObject("graph_snapshot");
        if (snapshotMetadata == null) {
            throw new IOException("graph delta bundle is missing graph_snapshot metadata");
        }
        String snapshotVersion = text(snapshotMetadata, "version");
        Path snapshotArtifact = resolveInside(root, text(snapshotMetadata, "artifact"));
        Path snapshotChunksArtifact = resolveInside(root, text(snapshotMetadata, "chunks"));
        List<TextChunk> snapshotChunks = readChunks(snapshotChunksArtifact);
        GraphSnapshot snapshot;
        try (GraphArtifactReader reader = new GraphArtifactReader(snapshotArtifact,
            snapshotVersion, text(snapshotMetadata, "index_version"))) {
            reader.getIdentity().validate(graphName, snapshotVersion,
                text(snapshotMetadata, "index_version"), snapshotChunks);
            reader.validateReferences(snapshotChunks);
            snapshot = reader.toSnapshot(snapshotChunks);
        }
        JsonArray chain = ready.getAsJsonArray("graph_delta_chain");
        if (chain == null || chain.size() == 0) {
            throw new IOException("graph delta bundle has an empty graph_delta_chain");
        }
        GraphVersion expectedParent = snapshot.getGraphVersion();
        List<GraphDelta> deltas = new ArrayList<>();
        for (int index = 0; index < chain.size(); index++) {
            JsonObject item = chain.get(index).getAsJsonObject();
            GraphVersion version = version(item.getAsJsonObject("graph_version"));
            GraphVersion parent = version(item.getAsJsonObject("parent_graph_version"));
            if (!expectedParent.equals(parent)) {
                throw new IOException("graph delta chain has a missing or out-of-order parent");
            }
            Path deltaPath = resolveInside(root, text(item, "artifact"));
            GraphDelta delta;
            try (BufferedReader input = Files.newBufferedReader(deltaPath, StandardCharsets.UTF_8)) {
                delta = GraphDeltaJson.read(input, version, parent, text(item, "digest"));
            }
            deltas.add(delta);
            expectedParent = version;
        }
        if (!graphName.equals(expectedParent.getGraphName())
            || !targetVersion.equals(expectedParent.getVersion())) {
            throw new IOException("graph delta chain does not end at configured graph version");
        }
        JsonObject lastDelta = chain.get(chain.size() - 1).getAsJsonObject();
        if (!text(ready, "delta_digest").equals(text(lastDelta, "digest"))) {
            throw new IOException("bundle delta digest does not match the delta chain");
        }
        VersionedGraphView view = VersionedGraphView.from(snapshot, deltas);
        String statisticsVersion = view.getStatisticsVersion();
        if (!statisticsVersion.equals(text(ready, "statistics_version"))
            || !statisticsVersion.equals(text(lastDelta, "statistics_version"))) {
            throw new IOException("bundle statistics version does not match the loaded graph view");
        }
        return new GraphViewBundle(view.getSnapshot().getEntities(), view.getSnapshot().getEdges(), view);
    }

    private static GraphVersion version(JsonObject value) throws IOException {
        if (value == null) {
            throw new IOException("graph delta version metadata is missing");
        }
        return new GraphVersion(text(value, "graphName"), text(value, "version"));
    }

    private static Path resolveInside(Path root, String relative) throws IOException {
        Path candidate = root.resolve(relative).normalize();
        requireInside(root, candidate);
        return candidate;
    }

    private static final class GraphViewBundle {
        private final List<org.apache.geaflow.ai.retrieval.model.graph.EntityRef> entities;
        private final List<org.apache.geaflow.ai.retrieval.model.graph.GraphEdgeRef> edges;
        private final GraphNeighborProvider provider;

        private GraphViewBundle(List<org.apache.geaflow.ai.retrieval.model.graph.EntityRef> entities,
                                List<org.apache.geaflow.ai.retrieval.model.graph.GraphEdgeRef> edges,
                                GraphNeighborProvider provider) {
            this.entities = entities;
            this.edges = edges;
            this.provider = provider;
        }
    }

    private static List<TextChunk> readChunks(Path path) throws IOException {
        if (!Files.isRegularFile(path)) {
            throw new IOException("missing canonical chunks: " + path);
        }
        List<TextChunk> chunks = new ArrayList<>();
        for (String line : Files.readAllLines(path, StandardCharsets.UTF_8)) {
            if (line.trim().isEmpty()) {
                continue;
            }
            JsonObject value = new JsonParser().parse(line).getAsJsonObject();
            chunks.add(new TextChunk(text(value, "chunk_id"), text(value, "document_id"),
                integer(value, "chunk_index"), integer(value, "start_offset"),
                integer(value, "end_offset"), integer(value, "token_estimate"),
                text(value, "text"), text(value, "policy_version"), text(value, "text_hash"),
                text(value, "source_uri"), text(value, "dataset"), text(value, "dataset_release"),
                text(value, "split"), text(value, "source_hash")));
        }
        if (chunks.isEmpty()) {
            throw new IOException("canonical chunks are empty");
        }
        return chunks;
    }

    private static JsonObject readObject(Path path) throws IOException {
        if (!Files.isRegularFile(path)) {
            throw new IOException("missing bundle metadata: " + path);
        }
        return new JsonParser().parse(Files.readString(path, StandardCharsets.UTF_8)).getAsJsonObject();
    }

    private static String text(JsonObject object, String key) {
        if (!object.has(key) || object.get(key).isJsonNull() || object.get(key).getAsString().trim().isEmpty()) {
            throw new RetrievalException(RetrievalErrorCode.INDEX_NOT_READY, "bundle field is missing: " + key);
        }
        return object.get(key).getAsString();
    }

    private static int integer(JsonObject object, String key) {
        return object.get(key).getAsInt();
    }

    private static void require(JsonObject ignored, boolean condition, String message) {
        if (!condition) {
            throw new RetrievalException(RetrievalErrorCode.INDEX_NOT_READY, message);
        }
    }

    private static void requireInside(Path root, Path candidate) throws IOException {
        Path realRoot = root.toRealPath();
        Path normalized = candidate.toAbsolutePath().normalize();
        if (!Files.exists(normalized) || !normalized.toRealPath().startsWith(realRoot)) {
            throw new IOException("bundle artifact is missing or outside root: " + candidate);
        }
    }

    private static void validateChecksums(Path root, JsonObject ready) throws IOException {
        if (!ready.has("artifact_checksums") || !ready.get("artifact_checksums").isJsonObject()) {
            throw new IOException("bundle artifact checksums are missing");
        }
        Set<String> expectedPaths = new HashSet<>();
        for (Map.Entry<String, com.google.gson.JsonElement> entry
            : ready.getAsJsonObject("artifact_checksums").entrySet()) {
            String relative = entry.getKey().replace('\\', '/');
            Path file = root.resolve(relative).normalize();
            requireInside(root, file);
            if (!Files.isRegularFile(file)) {
                throw new IOException("bundle checksum target is not a file: " + entry.getKey());
            }
            String expected = entry.getValue().getAsString();
            if (!expected.matches("[0-9a-fA-F]{64}")) {
                throw new IOException("bundle checksum is not SHA-256: " + entry.getKey());
            }
            java.security.MessageDigest digest;
            try {
                digest = java.security.MessageDigest.getInstance("SHA-256");
            } catch (java.security.NoSuchAlgorithmException error) {
                throw new IOException("SHA-256 is unavailable", error);
            }
            try (java.io.InputStream input = Files.newInputStream(file)) {
                byte[] buffer = new byte[1024 * 1024];
                int count;
                while ((count = input.read(buffer)) >= 0) {
                    digest.update(buffer, 0, count);
                }
            }
            StringBuilder actual = new StringBuilder();
            for (byte value : digest.digest()) {
                actual.append(String.format(java.util.Locale.ROOT, "%02x", value & 255));
            }
            if (!actual.toString().equalsIgnoreCase(expected)) {
                throw new IOException("bundle checksum mismatch: " + entry.getKey());
            }
            expectedPaths.add(relative);
        }
        Set<String> actualPaths = new HashSet<>();
        try (java.util.stream.Stream<Path> paths = Files.walk(root)) {
            paths.filter(Files::isRegularFile).forEach(path -> {
                if (!path.equals(root.resolve("READY.json"))) {
                    actualPaths.add(root.relativize(path).toString().replace('\\', '/'));
                }
            });
        }
        if (!actualPaths.equals(expectedPaths)) {
            throw new IOException("bundle checksum manifest does not cover bundle files");
        }
    }
}
