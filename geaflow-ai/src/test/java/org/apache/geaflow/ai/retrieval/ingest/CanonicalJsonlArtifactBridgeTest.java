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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.geaflow.ai.retrieval.metadata.ChunkingConfiguration;
import org.apache.geaflow.ai.retrieval.metadata.DatasetManifest;
import org.apache.geaflow.ai.retrieval.model.version.GraphVersion;
import org.junit.jupiter.api.Test;

class CanonicalJsonlArtifactBridgeTest {

    private static final String CHUNK_ID =
        "d8c6c04a12407a604c3d4a06119df7f9a76ec9e1f94c9963b02f11a283a8e0c8";
    private static final String SOURCE_HASH =
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    @Test
    void buildsNativeArtifactsFromVerifiedPythonRelease() throws Exception {
        Path release = createRelease();
        Path output = Files.createTempDirectory("bridge-output");
        DatasetManifest manifest = manifest();
        Map<String, float[]> vectors = Collections.singletonMap(CHUNK_ID, new float[] {1.0F, 0.5F});

        assertDoesNotThrow(() -> CanonicalJsonlArtifactBridge.build(release, output, manifest,
            new GraphVersion("graph", "g1"), "index-1", vectors, "fixture", "v1"));
        try (java.util.stream.Stream<Path> files = Files.list(output)) {
            org.junit.jupiter.api.Assertions.assertTrue(files.findAny().isPresent());
        }
    }

    @Test
    void rejectsChecksumTamperingAndGraphProvenanceMismatch() throws Exception {
        Path release = createRelease();
        Path output = Files.createTempDirectory("bridge-corrupt");
        Files.writeString(release.resolve("chunks.jsonl"), "tampered\n", StandardCharsets.UTF_8);
        assertThrows(java.io.IOException.class, () -> CanonicalJsonlArtifactBridge.build(release, output,
            manifest(), new GraphVersion("graph", "g1"), "index-1",
            Collections.singletonMap(CHUNK_ID, new float[] {1.0F, 0.5F}), "fixture", "v1"));

        Path provenanceRelease = createRelease();
        Path vertex = provenanceRelease.resolve("graph/g1/vertices.jsonl");
        Files.writeString(vertex, Files.readString(vertex).replace(SOURCE_HASH, SOURCE_HASH.substring(0, 63) + "0"),
            StandardCharsets.UTF_8);
        refreshChecksums(provenanceRelease);
        assertThrows(java.io.IOException.class, () -> CanonicalJsonlArtifactBridge.build(provenanceRelease, output,
            manifest(), new GraphVersion("graph", "g1"), "index-1",
            Collections.singletonMap(CHUNK_ID, new float[] {1.0F, 0.5F}), "fixture", "v1"));
    }

    @Test
    void rejectsChunkIdentityMismatchEvenWhenReleaseChecksumsAreUpdated() throws Exception {
        Path release = createRelease();
        Path chunk = release.resolve("chunks.jsonl");
        Files.writeString(chunk, Files.readString(chunk).replace("\"dataset\":\"fixture\"",
            "\"dataset\":\"other\""), StandardCharsets.UTF_8);
        refreshChecksums(release);

        assertThrows(java.io.IOException.class, () -> CanonicalJsonlArtifactBridge.build(release,
            Files.createTempDirectory("bridge-chunk-identity"), manifest(),
            new GraphVersion("graph", "g1"), "index-1",
            Collections.singletonMap(CHUNK_ID, new float[] {1.0F, 0.5F}), "fixture", "v1"));
    }

    private static DatasetManifest manifest() {
        return new DatasetManifest("v1", "fixture", "release", "dev", "file:///fixture", null,
            SOURCE_HASH, "preprocess", new ChunkingConfiguration("policy", 100, 10),
            "schema", "fixture", "v1", 7L);
    }

    private static Path createRelease() throws Exception {
        Path root = Files.createTempDirectory("python-release");
        Files.createDirectories(root.resolve("graph/g1"));
        Files.writeString(root.resolve("documents.jsonl"), "{}\n", StandardCharsets.UTF_8);
        Files.writeString(root.resolve("chunks.jsonl"), chunkJson(), StandardCharsets.UTF_8);
        String vertex = "{\"canonical_name\":\"Alpha\",\"dataset\":\"fixture\","
            + "\"dataset_release\":\"release\",\"entity_id\":\"entity-1\","
            + "\"entity_type\":\"CONCEPT\",\"source_chunk_ids\":[\""
            + CHUNK_ID + "\"],"
            + "\"source_document_ids\":[\"doc-1\"],\"source_hash\":\"" + SOURCE_HASH
            + "\",\"split\":\"dev\",\"vertex_id\":\"entity-1\",\"vertex_type\":\"ENTITY\"}\n";
        Files.writeString(root.resolve("graph/g1/vertices.jsonl"), vertex, StandardCharsets.UTF_8);
        Files.writeString(root.resolve("graph/g1/edges.jsonl"), "", StandardCharsets.UTF_8);
        String verticesChecksum = sha256(root.resolve("graph/g1/vertices.jsonl"));
        String edgesChecksum = sha256(root.resolve("graph/g1/edges.jsonl"));
        Files.writeString(root.resolve("graph/g1/manifest.json"),
            "{\"edge_count\":0,\"extractor_version\":\"extractor\","
                + "\"graph_version\":\"g1\",\"schema_version\":\"schema\","
                + "\"vertex_count\":1,\"vertices_sha256\":\"" + verticesChecksum
                + "\",\"edges_sha256\":\"" + edgesChecksum + "\"}\n",
            StandardCharsets.UTF_8);
        Files.writeString(root.resolve("quality-report.json"), "{}\n", StandardCharsets.UTF_8);
        Files.writeString(root.resolve("bm25.artifact"), "placeholder\n", StandardCharsets.UTF_8);
        Files.writeString(root.resolve("vector.artifact"), "placeholder\n", StandardCharsets.UTF_8);
        writeReady(root);
        return root;
    }

    private static String chunkJson() {
        return "{\"chunk_id\":\"" + CHUNK_ID
            + "\",\"chunk_index\":0,\"dataset\":\"fixture\","
            + "\"dataset_release\":\"release\",\"document_id\":\"doc-1\","
            + "\"end_offset\":5,\"policy_version\":\"policy\",\"source_hash\":\""
            + SOURCE_HASH + "\",\"source_uri\":\"file:///fixture\",\"split\":\"dev\","
            + "\"normalization_version\":\"preprocess\","
            + "\"start_offset\":0,\"text\":\"Alpha\",\"text_hash\":"
            + "\"b1a96dd646bccaa24cef7a3db22a6f995f05658f4f1c3272913e258c03e6fb24\","
            + "\"token_estimate\":2}\n";
    }

    private static void writeReady(Path root) throws Exception {
        Map<String, String> checksums = new HashMap<>();
        try (java.util.stream.Stream<Path> paths = Files.walk(root)) {
            paths.filter(Files::isRegularFile).filter(path -> !path.getFileName().toString().equals("READY.json"))
                .forEach(path -> {
                    try {
                        checksums.put(root.relativize(path).toString().replace('\\', '/'), sha256(path));
                    } catch (Exception error) {
                        throw new IllegalStateException(error);
                    }
                });
        }
        StringBuilder json = new StringBuilder("{\"artifact_checksums\":{");
        boolean first = true;
        for (Map.Entry<String, String> entry : checksums.entrySet()) {
            if (!first) {
                json.append(',');
            }
            first = false;
            json.append('"').append(entry.getKey()).append("\":\"")
                .append(entry.getValue()).append('"');
        }
        json.append("},\"dataset\":\"fixture\",\"dataset_release\":\"release\","
            + "\"documents\":1,\"chunks\":1,\"vertices\":1,\"edges\":0,"
            + "\"entity_extractor_version\":\"extractor\",\"entity_schema_version\":\"schema\","
            + "\"graph_manifest\":\"graph/g1/manifest.json\",\"pipeline_version\":\"disk-spill-v1\","
            + "\"preprocessing_version\":\"preprocess\","
            + "\"policy_version\":\"policy\",\"random_seed\":7,\"sha256\":\"" + SOURCE_HASH
            + "\",\"split\":\"dev\",\"state\":\"READY\",\"version\":\"g1\","
            + "\"vector_source\":\"fixture\",\"vector_version\":\"v1\","
            + "\"bm25_artifact\":\"bm25.artifact\",\"vector_artifact\":\"vector.artifact\","
            + "\"quality_report\":\"quality-report.json\"}\n");
        Files.writeString(root.resolve("READY.json"), json, StandardCharsets.UTF_8);
    }

    private static void refreshChecksums(Path root) throws Exception {
        JsonObject ready = new JsonParser().parse(Files.readString(root.resolve("READY.json"))).getAsJsonObject();
        JsonObject checksums = ready.getAsJsonObject("artifact_checksums");
        checksums.addProperty("graph/g1/vertices.jsonl", sha256(root.resolve("graph/g1/vertices.jsonl")));
        Files.writeString(root.resolve("READY.json"), ready.toString() + "\n", StandardCharsets.UTF_8);
    }

    private static String sha256(Path path) throws Exception {
        byte[] digest = MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(path));
        StringBuilder result = new StringBuilder();
        for (byte value : digest) {
            result.append(String.format(java.util.Locale.ROOT, "%02x", value & 0xff));
        }
        return result.toString();
    }
}
