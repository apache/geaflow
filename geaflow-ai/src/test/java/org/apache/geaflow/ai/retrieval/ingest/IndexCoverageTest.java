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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.gson.JsonObject;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.geaflow.ai.retrieval.index.bm25.LuceneBm25IndexBuilder;
import org.apache.geaflow.ai.retrieval.index.vector.OfflineVectorIndexBuilder;
import org.apache.geaflow.ai.retrieval.metadata.ChunkingConfiguration;
import org.apache.geaflow.ai.retrieval.metadata.DatasetManifest;
import org.apache.geaflow.ai.retrieval.model.document.TextChunk;
import org.apache.geaflow.ai.retrieval.model.version.GraphVersion;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Verifies that index coverage can be measured from a published bundle.
 *
 * <p>Bundles built through the canonical-JSONL bridge never run the Python index builders, so their
 * quality report carried an empty {@code index_coverage}. Without a measurement a READY bundle
 * could not state that its indexes actually covered the corpus.
 */
class IndexCoverageTest {
    private static final String VERSION = "v1";

    @TempDir
    Path tempDir;

    @Test
    void reportsFullCoverageWhenEveryChunkIsIndexed() throws Exception {
        Path bundle = bundleWithIndexes(Arrays.asList(
            chunk("c1", "first passage"), chunk("c2", "second passage")));

        JsonObject coverage = IndexCoverageCli.compute(bundle);

        assertEquals(2, coverage.getAsJsonObject("bm25").get("covered_chunks").getAsInt());
        assertEquals(2, coverage.getAsJsonObject("bm25").get("total_chunks").getAsInt());
        assertEquals(1.0D, coverage.getAsJsonObject("bm25").get("ratio").getAsDouble(), 1.0e-9);
        assertEquals(2, coverage.getAsJsonObject("vector").get("covered_chunks").getAsInt());
        assertEquals(1.0D, coverage.getAsJsonObject("vector").get("ratio").getAsDouble(), 1.0e-9);
    }

    @Test
    void rejectsBundleWithoutChunkManifest() {
        assertThrows(java.io.IOException.class, () -> IndexCoverageCli.compute(tempDir));
    }

    @Test
    void omitsIndexesThatAreAbsentInsteadOfReportingZero() throws Exception {
        Path bundle = tempDir.resolve("chunks-only");
        Files.createDirectories(bundle);
        try (Writer writer = Files.newBufferedWriter(bundle.resolve("chunks.jsonl"),
            StandardCharsets.UTF_8)) {
            writer.write(chunksJson(Arrays.asList(chunk("c1", "first passage"))));
        }

        JsonObject coverage = IndexCoverageCli.compute(bundle);

        assertEquals(0, coverage.entrySet().size(),
            "a bundle without indexes reports no coverage entries");
    }

    private Path bundleWithIndexes(List<TextChunk> chunks) throws Exception {
        Path bundle = tempDir.resolve("bundle");
        Files.createDirectories(bundle);
        try (Writer writer = Files.newBufferedWriter(bundle.resolve("chunks.jsonl"),
            StandardCharsets.UTF_8)) {
            writer.write(chunksJson(chunks));
        }
        Map<String, float[]> vectors = new HashMap<>();
        vectors.put("c1", new float[] {1.0F, 0.0F});
        vectors.put("c2", new float[] {0.0F, 1.0F});
        GraphVersion version = new GraphVersion("coverage", VERSION);
        try (org.apache.geaflow.ai.retrieval.index.IndexArtifact ignored =
                 new LuceneBm25IndexBuilder(bundle).build(
                     new IngestionContext(manifest(), version, "test"), chunks);
             org.apache.geaflow.ai.retrieval.index.IndexArtifact ignoredVector =
                 new OfflineVectorIndexBuilder(bundle, vectors, "fixture", VERSION)
                     .build(new IngestionContext(manifest(), version, "test"), chunks)) {
            // Artifacts are complete before coverage is measured.
        }
        return bundle;
    }

    private static String chunksJson(List<TextChunk> chunks) {
        StringBuilder builder = new StringBuilder();
        for (TextChunk chunk : chunks) {
            builder.append("{\"chunk_id\":\"").append(chunk.getChunkId()).append("\",\"text\":\"")
                .append(chunk.getText()).append("\"}\n");
        }
        return builder.toString();
    }

    private static TextChunk chunk(String id, String text) {
        return new TextChunk(id, "doc-" + id, 0, 0, text.length(), 2, text);
    }

    private static DatasetManifest manifest() {
        return new DatasetManifest(VERSION, "dataset", "revision", "dev", null, "cache",
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", "policy",
            new ChunkingConfiguration("chunk-v1", 100, 10), "graph", "fixture", VERSION, 1L);
    }
}