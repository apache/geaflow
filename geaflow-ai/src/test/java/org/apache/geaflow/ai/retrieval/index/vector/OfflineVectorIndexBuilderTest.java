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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.DataOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.geaflow.ai.retrieval.ingest.IngestionContext;
import org.apache.geaflow.ai.retrieval.metadata.ChunkingConfiguration;
import org.apache.geaflow.ai.retrieval.metadata.DatasetManifest;
import org.apache.geaflow.ai.retrieval.model.document.TextChunk;
import org.apache.geaflow.ai.retrieval.model.version.GraphVersion;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalErrorCode;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalException;
import org.junit.jupiter.api.Test;

class OfflineVectorIndexBuilderTest {

    @Test
    void rejectsNonFiniteAndMissingVectors() throws Exception {
        Path directory = Files.createTempDirectory("vector-test");
        TextChunk chunk = new TextChunk("c1", "d", 0, 0, 1, 1, "x");
        IngestionContext context = context();
        Map<String, float[]> missing = new HashMap<>();
        OfflineVectorIndexBuilder missingBuilder = new OfflineVectorIndexBuilder(directory, missing, "fixture", "v1");
        assertThrows(java.io.IOException.class, () -> missingBuilder.build(context, Arrays.asList(chunk)));
        Map<String, float[]> invalid = new HashMap<>();
        invalid.put("c1", new float[] {Float.NaN});
        OfflineVectorIndexBuilder invalidBuilder = new OfflineVectorIndexBuilder(directory, invalid, "fixture", "v1");
        assertThrows(java.io.IOException.class, () -> invalidBuilder.build(context, Arrays.asList(chunk)));
    }

    @Test
    void rejectsPathTraversalVersionBeforeCreatingOutputDirectory() {
        Path directory = tempDir.resolve("unsafe-vector");
        TextChunk chunk = new TextChunk("c1", "d", 0, 0, 1, 1, "x");
        Map<String, float[]> vectors = Collections.singletonMap("c1", new float[] {1.0F});
        OfflineVectorIndexBuilder builder = new OfflineVectorIndexBuilder(directory, vectors, "fixture", "v1");

        assertThrows(java.io.IOException.class,
            () -> builder.build(context("../outside"), Collections.singletonList(chunk)));
        assertFalse(Files.exists(directory));
    }

    @Test
    void readerRejectsCorruptArtifact() throws Exception {
        Path artifact = Files.createTempFile("vector-corrupt", ".bin");
        try (DataOutputStream output = new DataOutputStream(Files.newOutputStream(artifact))) {
            output.writeUTF("bad-magic");
        }
        RetrievalException corrupt = assertThrows(RetrievalException.class,
            () -> new VectorIndexReader(artifact));
        org.junit.jupiter.api.Assertions.assertEquals(RetrievalErrorCode.INDEX_NOT_READY,
            corrupt.getCode());
    }

    @Test
    void rejectsZeroNormAndChangedExistingVectors() throws Exception {
        Path directory = Files.createTempDirectory("vector-existing");
        TextChunk chunk = new TextChunk("chunk", "doc", 0, 0, 4, 1, "text");
        Map<String, float[]> vectors = new HashMap<>();
        vectors.put("chunk", new float[] {0.0F, 0.0F});
        assertThrows(java.io.IOException.class, () -> new OfflineVectorIndexBuilder(directory, vectors, "fixture", "v1")
            .build(context(), Collections.singletonList(chunk)));
        vectors.put("chunk", new float[] {1.0F, 0.0F});
        new OfflineVectorIndexBuilder(directory, vectors, "fixture", "v1").build(context(), Collections.singletonList(chunk)).close();
        vectors.put("chunk", new float[] {0.0F, 1.0F});
        assertThrows(java.io.IOException.class, () -> new OfflineVectorIndexBuilder(directory, vectors, "fixture", "v1")
            .build(context(), Collections.singletonList(chunk)));
    }

    @Test
    void removesStagingFileWhenExistingArtifactValidationFails() throws Exception {
        Path directory = Files.createTempDirectory("vector-cleanup");
        TextChunk chunk = new TextChunk("chunk", "doc", 0, 0, 4, 1, "text");
        Map<String, float[]> vectors = new HashMap<>();
        vectors.put("chunk", new float[] {1.0F, 0.0F});
        OfflineVectorIndexBuilder builder = new OfflineVectorIndexBuilder(directory, vectors, "fixture", "v1");
        builder.build(context(), Collections.singletonList(chunk)).close();
        vectors.put("chunk", new float[] {0.0F, 1.0F});
        assertThrows(java.io.IOException.class,
            () -> builder.build(context(), Collections.singletonList(chunk)));
        try (java.util.stream.Stream<Path> paths = Files.list(directory)) {
            assertFalse(paths.anyMatch(path -> path.getFileName().toString().endsWith(".tmp")));
        }
    }

    private static IngestionContext context() {
        return context("v1");
    }

    private static IngestionContext context(String graphVersion) {
        DatasetManifest manifest = new DatasetManifest("v1", "d", "r", "dev", null,
            "cache", "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            "p", new ChunkingConfiguration("chunk-v1", 100, 10), "graph-v1", "fixture", "v1", 1L);
        return new IngestionContext(manifest, new GraphVersion("g", graphVersion), "test");
    }

    @org.junit.jupiter.api.io.TempDir
    Path tempDir;
}
