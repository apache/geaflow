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
import static org.junit.jupiter.api.Assertions.assertTrue;

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
    void readerEscapesNaturalLanguagePunctuation() throws Exception {
        Path directory = Files.createTempDirectory("bm25-query-test");
        TextChunk chunk = new TextChunk("c1", "doc", 0, 0, 20, 5,
            "The director of Polish-Russian War (Film) was Xawery Zulawski.");
        try (org.apache.geaflow.ai.retrieval.index.IndexArtifact ignored =
                 new LuceneBm25IndexBuilder(directory).build(context(), Arrays.asList(chunk))) {
            Path artifact = directory.resolve("bm25-v1");
            Bm25IndexReader reader = new Bm25IndexReader(artifact);
            try {
                assertTrue(reader.search("Who directed Polish-Russian War (Film)?", 10, 5).size() > 0);
            } finally {
                reader.close();
            }
        }
    }

    @Test
    void exactSearchRanksMatchesBeyondDocumentPrefix() throws Exception {
        Path directory = Files.createTempDirectory("bm25-exact-test");
        TextChunk first = new TextChunk("c1", "doc1", 0, 0, 20, 5, "target");
        TextChunk stronger = new TextChunk("c2", "doc2", 0, 0, 40, 10, "target target target");
        try (org.apache.geaflow.ai.retrieval.index.IndexArtifact ignored =
                 new LuceneBm25IndexBuilder(directory).build(context(), Arrays.asList(first, stronger))) {
            try (Bm25IndexReader reader = new Bm25IndexReader(directory.resolve("bm25-v1"))) {
                // The candidate limit bounds how many hits are retained, not how far the index is
                // traversed. Both paths must therefore return the more relevant document; a limit
                // applied to traversal order returned "c1" purely because its chunkId sorts first.
                assertEquals("c2", reader.search("target", 1, 1).get(0).getChunkId());
                assertEquals("c2", reader.searchExact("target", 1, 1).get(0).getChunkId());
            }
        }
    }

    @Test
    void boundedSearchRetainsTopScoringMatchesRegardlessOfIndexOrder() throws Exception {
        Path directory = Files.createTempDirectory("bm25-bounded-rank-test");
        // Index every strong match under a high chunkId so that traversal order (which follows
        // chunkId) visits the filler documents first. A limit applied to traversal would return
        // only filler; the limit must instead select by score.
        java.util.List<TextChunk> chunks = new java.util.ArrayList<>();
        for (int index = 0; index < 40; index++) {
            chunks.add(new TextChunk(String.format("a-%03d", index), "docFiller" + index,
                0, 0, 4, 1, "target"));
        }
        TextChunk strong = new TextChunk("z-999", "docStrong", 0, 0, 40, 10,
            "target target target target target");
        chunks.add(strong);
        try (org.apache.geaflow.ai.retrieval.index.IndexArtifact ignored =
                 new LuceneBm25IndexBuilder(directory).build(context(), chunks)) {
            try (Bm25IndexReader reader = new Bm25IndexReader(directory.resolve("bm25-v1"))) {
                java.util.List<Bm25Hit> hits = reader.search("target", 5, 5);
                assertEquals("z-999", hits.get(0).getChunkId(),
                    "the highest-scoring chunk must survive a candidate limit smaller than the hit set");
                assertTrue(hits.get(0).getScore() >= hits.get(hits.size() - 1).getScore(),
                    "retained hits must be ordered by descending score");
                assertEquals(5, hits.size(), "the candidate limit still bounds the retained set");
            }
        }
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
