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

package org.apache.geaflow.ai.retrieval.index.bm25;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import org.apache.geaflow.ai.retrieval.ingest.IngestionContext;
import org.apache.geaflow.ai.retrieval.metadata.DatasetManifest;
import org.apache.geaflow.ai.retrieval.metadata.ChunkingConfiguration;
import org.apache.geaflow.ai.retrieval.model.document.TextChunk;
import org.apache.geaflow.ai.retrieval.model.version.GraphVersion;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalErrorCode;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalException;
import org.junit.jupiter.api.Test;

class LuceneBm25IndexBuilderTest {

    @Test
    void buildsReadableDeterministicArtifact() throws Exception {
        Path directory = Files.createTempDirectory("bm25-test");
        GraphVersion version = new GraphVersion("g", "v1");
        DatasetManifest manifest = new DatasetManifest("v1", "d", "r", "dev", null,
            "cache", "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            "p", new ChunkingConfiguration("chunk-v1", 100, 10), "graph-v1", "offline", "v1", 1L);
        IngestionContext context = new IngestionContext(manifest, version, "test");
        TextChunk first = new TextChunk("b", "doc", 1, 2, 3, 1, "beta", "p", "hash-b");
        TextChunk second = new TextChunk("a", "doc", 0, 0, 2, 1, "alpha", "p", "hash-a");
        LuceneBm25IndexBuilder builder = new LuceneBm25IndexBuilder(directory);
        try (org.apache.geaflow.ai.retrieval.index.IndexArtifact artifact =
                 builder.build(context, Arrays.asList(first, second))) {
            assertEquals("bm25", artifact.getMetadata().getIndexType());
            try (org.apache.lucene.store.Directory luceneDirectory =
                     org.apache.lucene.store.FSDirectory.open(directory.resolve("bm25-v1"));
                 org.apache.lucene.index.DirectoryReader reader =
                     org.apache.lucene.index.DirectoryReader.open(luceneDirectory)) {
                assertEquals(2, reader.numDocs());
            }
        }
    }

    @Test
    void readerReportsMissingArtifactAsIndexNotReady() {
        RetrievalException exception = org.junit.jupiter.api.Assertions.assertThrows(
            RetrievalException.class,
            () -> new Bm25IndexReader(java.nio.file.Paths.get("/tmp/missing-bm25-artifact")));
        assertEquals(RetrievalErrorCode.INDEX_NOT_READY, exception.getCode());
    }

    @Test
    void rejectsPathTraversalVersionBeforeCreatingOutputDirectory() {
        Path directory = java.nio.file.Paths.get(System.getProperty("java.io.tmpdir"),
            "bm25-unsafe-" + java.util.UUID.randomUUID());
        TextChunk chunk = new TextChunk("c1", "doc", 0, 0, 1, 1, "text");

        assertThrows(java.io.IOException.class,
            () -> new LuceneBm25IndexBuilder(directory).build(context("../outside"), Arrays.asList(chunk)));
        assertFalse(Files.exists(directory));
    }

    @Test
    void removesStagingDirectoryWhenPublishedArtifactValidationFails() throws Exception {
        Path directory = Files.createTempDirectory("bm25-cleanup-test");
        Path published = Files.createDirectory(directory.resolve("bm25-v1"));
        Files.write(published.resolve("invalid"), new byte[] {1});
        TextChunk chunk = new TextChunk("c1", "doc", 0, 0, 1, 1, "text");
        assertThrows(java.io.IOException.class,
            () -> new LuceneBm25IndexBuilder(directory).build(context(), Arrays.asList(chunk)));
        try (java.util.stream.Stream<Path> paths = Files.list(directory)) {
            assertFalse(paths.anyMatch(path -> path.getFileName().toString().startsWith("bm25-v1-")));
        }
    }

    private static IngestionContext context() {
        return context("v1");
    }

    private static IngestionContext context(String graphVersion) {
        DatasetManifest manifest = new DatasetManifest("v1", "d", "r", "dev", null,
            "cache", "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            "p", new ChunkingConfiguration("chunk-v1", 100, 10), "graph-v1", "offline", "v1", 1L);
        return new IngestionContext(manifest, new GraphVersion("g", graphVersion), "test");
    }
}
