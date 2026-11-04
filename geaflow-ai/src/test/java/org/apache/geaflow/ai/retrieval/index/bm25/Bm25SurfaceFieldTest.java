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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.geaflow.ai.retrieval.ingest.IngestionContext;
import org.apache.geaflow.ai.retrieval.metadata.ChunkingConfiguration;
import org.apache.geaflow.ai.retrieval.metadata.DatasetManifest;
import org.apache.geaflow.ai.retrieval.model.document.TextChunk;
import org.apache.geaflow.ai.retrieval.model.version.GraphVersion;
import org.junit.jupiter.api.Test;

/**
 * Pins the design requirement that the BM25 index covers the title, the chunk text and entity
 * aliases, and that every hit reports matched terms and IDF statistics.
 *
 * <p>A body-only index cannot answer a title-only or alias-only query, and an adaptive router cannot
 * reason about lexical confidence without the IDF of the terms that matched.
 */
class Bm25SurfaceFieldTest {

    @Test
    void titleAndAliasSurfacesAreSearchable() throws Exception {
        Path directory = Files.createTempDirectory("bm25-surface");
        IngestionContext context = context();
        TextChunk chunk = new TextChunk("c1", "doc-1", 0, 0, 21, 4,
            "He taught ethics here", "p", "hash-c1");
        Map<String, List<String>> surfaces = new HashMap<>();
        surfaces.put("c1", Arrays.asList("Confucius", "Kongzi"));
        new LuceneBm25IndexBuilder(directory)
            .build(context, Collections.singletonList(chunk), surfaces).close();

        try (Bm25IndexReader reader = new Bm25IndexReader(directory.resolve("bm25-v1"))) {
            // The body never mentions the alias; only the surface field can match it.
            assertFalse(reader.search("Kongzi", 10, 5).isEmpty(),
                "an alias-only query must match through the surface field");
            assertFalse(reader.search("Confucius", 10, 5).isEmpty(),
                "a title-only query must match through the surface field");
            assertEquals("c1", reader.search("Kongzi", 10, 5).get(0).getChunkId());
        }
    }

    @Test
    void bodyOnlyIndexStillAnswersBodyQueries() throws Exception {
        Path directory = Files.createTempDirectory("bm25-body-only");
        TextChunk chunk = new TextChunk("c1", "doc-1", 0, 0, 21, 4,
            "He taught ethics here", "p", "hash-c1");
        new LuceneBm25IndexBuilder(directory)
            .build(context(), Collections.singletonList(chunk)).close();

        try (Bm25IndexReader reader = new Bm25IndexReader(directory.resolve("bm25-v1"))) {
            assertFalse(reader.search("ethics", 10, 5).isEmpty());
            assertTrue(reader.search("Kongzi", 10, 5).isEmpty(),
                "a body-only artifact has no surface field to match");
        }
    }

    @Test
    void hitsReportMatchedTermsAndIdf() throws Exception {
        Path directory = Files.createTempDirectory("bm25-terms");
        TextChunk matching = new TextChunk("c1", "doc-1", 0, 0, 19, 4,
            "Confucius taught ethics", "p", "hash-c1");
        TextChunk other = new TextChunk("c2", "doc-2", 0, 0, 19, 4,
            "Astronomy studies stars", "p", "hash-c2");
        new LuceneBm25IndexBuilder(directory)
            .build(context(), Arrays.asList(matching, other)).close();

        try (Bm25IndexReader reader = new Bm25IndexReader(directory.resolve("bm25-v1"))) {
            List<Bm25Hit> hits = reader.search("Confucius ethics", 10, 5);
            assertFalse(hits.isEmpty());
            Bm25Hit hit = hits.get(0);
            assertEquals(Arrays.asList("confucius", "ethics"), hit.getMatchedTerms(),
                "matched terms are reported in query order");
            assertTrue(hit.getAverageIdf() > 0.0D, "matched terms must report a positive IDF");
            assertTrue(hit.getAverageIdf() <= Math.log(1.0D + 2.0D),
                "IDF must be bounded by the corpus size");
        }
    }

    @Test
    void unmatchedTermsAreNotReported() throws Exception {
        Path directory = Files.createTempDirectory("bm25-unmatched");
        TextChunk chunk = new TextChunk("c1", "doc-1", 0, 0, 19, 4,
            "Confucius taught ethics", "p", "hash-c1");
        new LuceneBm25IndexBuilder(directory)
            .build(context(), Collections.singletonList(chunk)).close();

        try (Bm25IndexReader reader = new Bm25IndexReader(directory.resolve("bm25-v1"))) {
            Bm25Hit hit = reader.search("Confucius", 10, 5).get(0);
            assertEquals(Collections.singletonList("confucius"), hit.getMatchedTerms());
        }
    }

    private IngestionContext context() {
        GraphVersion version = new GraphVersion("g", "v1");
        DatasetManifest manifest = new DatasetManifest("v1", "d", "r", "dev", null,
            "cache", "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            "p", new ChunkingConfiguration("chunk-v1", 100, 10), "graph-v1", "offline", "v1", 1L);
        return new IngestionContext(manifest, version, "test");
    }
}