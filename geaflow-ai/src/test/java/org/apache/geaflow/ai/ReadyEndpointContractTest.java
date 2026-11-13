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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.geaflow.ai;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.geaflow.ai.retrieval.config.RetrievalProperties;
import org.apache.geaflow.ai.retrieval.ingest.CanonicalJsonlArtifactBridgeCli;
import org.apache.geaflow.ai.retrieval.model.document.TextChunk;
import org.apache.geaflow.ai.retrieval.model.graph.EntityRef;
import org.apache.geaflow.ai.retrieval.model.graph.GraphEdgeRef;
import org.apache.geaflow.ai.retrieval.service.ArtifactBundleLoader;
import org.apache.geaflow.ai.retrieval.service.RetrievalChannelRegistry;
import org.junit.jupiter.api.Test;

/**
 * Contract tests for the readiness signal an operator and a load balancer rely on.
 *
 * <p>{@code /ready} used to answer "some READY fixture exists", so a bundle whose BM25 or vector
 * artifact had gone missing still reported green while single-channel requests failed later with
 * {@code INDEX_NOT_READY}. A readiness probe that stays green while the deployment cannot serve is
 * worse than no probe, so these tests drive the same per-component check the endpoint uses and
 * assert the failure is attributed to the artifact that actually broke.
 */
class ReadyEndpointContractTest {
    private static final String GRAPH = "contract-graph";
    private static final String VERSION = "v1";

    @Test
    void missingVectorArtifactMakesReadinessFailAndNamesTheVector() {
        RetrievalChannelRegistry registry = new RetrievalChannelRegistry();
        registry.register(GRAPH, VERSION, VERSION, null, null, chunks(), entities(), edges(), true);

        RetrievalChannelRegistry.ReadinessReport report = registry.readiness(GRAPH, VERSION, VERSION);

        assertFalse(report.isReady(), "a bundle without a vector artifact cannot serve vector queries");
        assertEquals("VECTOR_NOT_READY", report.getComponents().get("vector"));
        assertReadinessBodyIs503AndNames(report);
    }

    @Test
    void missingBm25ArtifactMakesReadinessFailAndNamesBm25() {
        RetrievalChannelRegistry registry = new RetrievalChannelRegistry();
        registry.register(GRAPH, VERSION, VERSION, null, null, chunks(), entities(), edges(), true);

        RetrievalChannelRegistry.ReadinessReport report = registry.readiness(GRAPH, VERSION, VERSION);

        assertFalse(report.isReady());
        assertEquals("BM25_NOT_READY", report.getComponents().get("bm25"),
            "the first missing component reported must be the BM25 artifact");
        assertReadinessBodyIs503AndNames(report);
    }

    /**
     * An unready fixture is the staging case: the bundle is registered but must not be served.
     */
    @Test
    void stagingFixtureIsNotReady() {
        RetrievalChannelRegistry registry = new RetrievalChannelRegistry();
        registry.register(GRAPH, VERSION, VERSION, null, null, chunks(), entities(), edges(), false);

        RetrievalChannelRegistry.ReadinessReport report = registry.readiness(GRAPH, VERSION, VERSION);

        assertFalse(report.isReady(), "a fixture that never reached READY must not serve traffic");
        assertEquals("GRAPH_NOT_READY", report.getReason());
    }

    /**
     * Readiness must be exact about versions: serving a request pinned to a version that is not
     * published would silently fall back to different data.
     */
    @Test
    void unpublishedVersionIsNotReady() {
        RetrievalChannelRegistry registry = new RetrievalChannelRegistry();
        registry.register(GRAPH, VERSION, VERSION, null, null, chunks(), entities(), edges(), true);

        RetrievalChannelRegistry.ReadinessReport report = registry.readiness(GRAPH, "v2", "v2");

        assertFalse(report.isReady());
        assertEquals("GRAPH_NOT_READY", report.getReason());
    }

    @Test
    void readyBodyReportsEveryComponentAndValidJson() {
        RetrievalChannelRegistry registry = new RetrievalChannelRegistry();
        registry.register(GRAPH, VERSION, VERSION, null, null, chunks(), entities(), edges(), true);
        RetrievalChannelRegistry.ReadinessReport report = registry.readiness(GRAPH, VERSION, VERSION);

        String body = GeaFlowMemoryServer.readyBody(report.isReady(), GRAPH, VERSION,
            report.getReason(), report.getComponents());
        JsonObject parsed = new JsonParser().parse(body).getAsJsonObject();

        assertEquals("NOT_READY", parsed.get("status").getAsString());
        JsonObject components = parsed.getAsJsonObject("components");
        assertTrue(components.has("graph") && components.has("bm25") && components.has("vector"),
            "the ready body must expose all three artifact outcomes, got " + components);
        assertEquals("READY", components.get("graph").getAsString());
        assertEquals("BM25_NOT_READY", components.get("bm25").getAsString());
        assertEquals("VECTOR_NOT_READY", components.get("vector").getAsString());
    }

    @Test
    void readyBodyWithoutComponentsKeepsTheLegacyShape() {
        String body = GeaFlowMemoryServer.readyBody(true, GRAPH, VERSION, "READY");
        JsonObject parsed = new JsonParser().parse(body).getAsJsonObject();

        assertEquals("READY", parsed.get("status").getAsString());
        assertFalse(parsed.has("components"),
            "omitting the component map must not emit a null/empty object");
    }

    /**
     * A published READY bundle that loses an artifact on disk must stop reporting ready.
     *
     * <p>The bundle is produced by the real ingestion CLI, so it carries genuine {@code graph.bin},
     * {@code vector.bin} and {@code bm25} artifacts plus their checksums. Deleting a declared
     * artifact is exactly the drift a presence-only readiness check misses.
     */
    @Test
    void bundleLosingAnArtifactCannotBeReloaded() throws Exception {
        Path bundle = buildReadyBundleThroughCli();
        RetrievalProperties properties = bundleProperties(bundle);

        // The intact bundle loads, which proves the fixture itself is valid.
        loaderFor(properties).initialize();

        Files.delete(bundle.resolve("vector.bin"));
        org.junit.jupiter.api.Assertions.assertThrows(
            org.apache.geaflow.ai.retrieval.api.model.RetrievalException.class,
            loaderFor(properties)::initialize,
            "a bundle missing a declared artifact must not be reported ready");
    }

    /**
     * A declared artifact whose bytes no longer match the READY manifest is the subtler failure:
     * the file exists, so a presence-only check stays green while queries would read garbage.
     */
    @Test
    void bundleWithCorruptedArtifactCannotBeReloaded() throws Exception {
        Path bundle = buildReadyBundleThroughCli();
        RetrievalProperties properties = bundleProperties(bundle);

        Files.writeString(bundle.resolve("vector.bin"), "corrupted\n", StandardCharsets.UTF_8);

        org.junit.jupiter.api.Assertions.assertThrows(
            org.apache.geaflow.ai.retrieval.api.model.RetrievalException.class,
            loaderFor(properties)::initialize,
            "a corrupted artifact must fail its checksum instead of serving bad data");
    }

    private static ArtifactBundleLoader loaderFor(RetrievalProperties properties) {
        ArtifactBundleLoader loader = new ArtifactBundleLoader();
        set(loader, "properties", properties);
        set(loader, "registry", new RetrievalChannelRegistry());
        return loader;
    }

    private static RetrievalProperties bundleProperties(Path bundle) {
        RetrievalProperties properties = new RetrievalProperties();
        properties.setArtifactBundleRoot(bundle.toString());
        properties.setArtifactGraphName(GRAPH);
        properties.setArtifactGraphVersion(VERSION);
        properties.setArtifactIndexVersion(VERSION);
        properties.setArtifactVectorSource("fixture");
        properties.setArtifactVectorVersion("v1");
        properties.validateConfiguration();
        return properties;
    }

    /**
     * Builds a complete READY bundle with the production ingestion CLI so the artifacts and their
     * checksums are real rather than hand-written placeholders.
     */
    private static org.apache.geaflow.ai.retrieval.metadata.DatasetManifest manifest() {
        return new org.apache.geaflow.ai.retrieval.metadata.DatasetManifest("v1", "fixture",
            "release", "dev", "file:///fixture", null, sourceHash(), "preprocess",
            new org.apache.geaflow.ai.retrieval.metadata.ChunkingConfiguration("policy", 100, 10),
            "schema", "fixture", "v1", 7L);
    }

    private static Path buildReadyBundleThroughCli() throws Exception {
        Path root = Files.createTempDirectory("ready-bundle");
        Path release = root.resolve("release");
        Path graphRoot = release.resolve("graph").resolve(VERSION);
        Files.createDirectories(graphRoot);
        Files.writeString(release.resolve("documents.jsonl"), "{}\n", StandardCharsets.UTF_8);
        Files.writeString(release.resolve("chunks.jsonl"), chunkJson(), StandardCharsets.UTF_8);
        String vertex = "{\"canonical_name\":\"Alpha\",\"dataset\":\"fixture\","
            + "\"dataset_release\":\"release\",\"entity_id\":\"entity-1\","
            + "\"entity_type\":\"CONCEPT\",\"source_chunk_ids\":[\"" + chunkId() + "\"],"
            + "\"source_document_ids\":[\"doc-1\"],\"source_hash\":\"" + sourceHash() + "\","
            + "\"split\":\"dev\",\"vertex_id\":\"entity-1\",\"vertex_type\":\"ENTITY\"}\n";
        Files.writeString(graphRoot.resolve("vertices.jsonl"), vertex, StandardCharsets.UTF_8);
        Files.writeString(graphRoot.resolve("edges.jsonl"), "", StandardCharsets.UTF_8);
        Files.writeString(graphRoot.resolve("manifest.json"),
            "{\"edge_count\":0,\"extractor_version\":\"extractor\","
                + "\"graph_version\":\"" + VERSION + "\",\"schema_version\":\"schema\","
                + "\"vertex_count\":1,"
                + "\"vertices_sha256\":\"" + sha256(graphRoot.resolve("vertices.jsonl")) + "\","
                + "\"edges_sha256\":\"" + sha256(graphRoot.resolve("edges.jsonl")) + "\"}\n",
            StandardCharsets.UTF_8);
        Files.writeString(release.resolve("quality-report.json"), "{}\n", StandardCharsets.UTF_8);
        writeReleaseReady(release);

        Path manifest = root.resolve("manifest.json");
        Files.writeString(manifest,
            org.apache.geaflow.ai.retrieval.metadata.MetadataJson.toJson(manifest()), StandardCharsets.UTF_8);
        Path vectors = root.resolve("vectors.jsonl");
        Files.writeString(vectors,
            "{\"chunkId\":\"" + chunkId() + "\",\"vector\":[1.0,0.5]}\n", StandardCharsets.UTF_8);

        Path bundle = root.resolve("bundle");
        CanonicalJsonlArtifactBridgeCli.main(new String[] {
            "--release", release.toString(), "--output", bundle.toString(),
            "--manifest", manifest.toString(), "--vectors", vectors.toString(),
            "--graph-name", GRAPH, "--graph-version", VERSION, "--index-version", VERSION,
            "--vector-source", "fixture", "--vector-version", "v1"});
        return bundle;
    }

    private static void writeReleaseReady(Path release) throws Exception {
        Map<String, String> checksums = new java.util.LinkedHashMap<>();
        try (java.util.stream.Stream<Path> paths = Files.walk(release)) {
            paths.filter(Files::isRegularFile)
                .filter(path -> !path.getFileName().toString().equals("READY.json"))
                .sorted()
                .forEach(path -> {
                    try {
                        checksums.put(release.relativize(path).toString().replace('\\', '/'),
                            sha256(path));
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
            + "\"entity_extractor_version\":\"extractor\","
            + "\"entity_schema_version\":\"schema\","
            + "\"graph_manifest\":\"graph/" + VERSION + "/manifest.json\","
            + "\"pipeline_version\":\"disk-spill-v1\","
            + "\"preprocessing_version\":\"preprocess\","
            + "\"policy_version\":\"policy\",\"random_seed\":7,\"sha256\":\"" + sourceHash()
            + "\",\"split\":\"dev\",\"state\":\"READY\",\"version\":\"" + VERSION + "\","
            + "\"vector_source\":\"fixture\",\"vector_version\":\"v1\","
            + "\"bm25_artifact\":\"bm25.artifact\","
            + "\"vector_artifact\":\"vector.artifact\","
            + "\"quality_report\":\"quality-report.json\"}\n");
        Files.writeString(release.resolve("READY.json"), json.toString(), StandardCharsets.UTF_8);
    }

    private static void assertReadinessBodyIs503AndNames(
        RetrievalChannelRegistry.ReadinessReport report) {
        String body = GeaFlowMemoryServer.readyBody(report.isReady(), GRAPH, VERSION,
            report.getReason(), report.getComponents());
        JsonObject parsed = new JsonParser().parse(body).getAsJsonObject();
        assertEquals("NOT_READY", parsed.get("status").getAsString(),
            "a not-ready report must serialize as NOT_READY so the endpoint answers 503");
        assertEquals(report.getReason(), parsed.get("reason").getAsString(),
            "the reason must name the artifact that is missing");
    }

    private static void set(ArtifactBundleLoader loader, String name, Object value) {
        try {
            java.lang.reflect.Field field = ArtifactBundleLoader.class.getDeclaredField(name);
            field.setAccessible(true);
            field.set(loader, value);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("cannot set " + name, e);
        }
    }

    private static String sha256(Path path) throws Exception {
        byte[] digest = java.security.MessageDigest.getInstance("SHA-256")
            .digest(Files.readAllBytes(path));
        StringBuilder hex = new StringBuilder();
        for (byte value : digest) {
            hex.append(String.format("%02x", value));
        }
        return hex.toString();
    }

    private static String chunkId() {
        return "d8c6c04a12407a604c3d4a06119df7f9a76ec9e1f94c9963b02f11a283a8e0c8";
    }

    private static String sourceHash() {
        return "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    }

    private static String chunkJson() {
        return "{\"chunk_id\":\"" + chunkId() + "\",\"chunk_index\":0,\"dataset\":\"fixture\","
            + "\"dataset_release\":\"release\",\"document_id\":\"doc-1\","
            + "\"end_offset\":5,\"policy_version\":\"policy\",\"source_hash\":\""
            + sourceHash() + "\",\"source_uri\":\"file:///fixture\",\"split\":\"dev\","
            + "\"normalization_version\":\"preprocess\","
            + "\"start_offset\":0,\"text\":\"Alpha\",\"text_hash\":\""
            + "b1a96dd646bccaa24cef7a3db22a6f995f05658f4f1c3272913e258c03e6fb24\","
            + "\"token_estimate\":2}\n";
    }

    private static List<TextChunk> chunks() {
        return Collections.singletonList(new TextChunk(chunkId(), "doc-1", 0, 0, 5, 1, "Alpha",
            null, null, "https://example.test/doc-1"));
    }

    private static List<EntityRef> entities() {
        return Collections.singletonList(new EntityRef("entity-1", "Alpha",
            Collections.singletonList("alpha"), "CONCEPT",
            Collections.singletonList(chunkId())));
    }

    private static List<GraphEdgeRef> edges() {
        return Collections.emptyList();
    }

}