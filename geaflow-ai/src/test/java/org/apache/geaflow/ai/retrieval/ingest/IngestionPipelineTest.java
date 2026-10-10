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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.geaflow.ai.retrieval.index.IndexArtifact;
import org.apache.geaflow.ai.retrieval.metadata.ChunkingConfiguration;
import org.apache.geaflow.ai.retrieval.metadata.DatasetManifest;
import org.apache.geaflow.ai.retrieval.metadata.GraphBuildMetadata;
import org.apache.geaflow.ai.retrieval.metadata.ImportMetadata;
import org.apache.geaflow.ai.retrieval.metadata.ImportState;
import org.apache.geaflow.ai.retrieval.metadata.IndexBuildMetadata;
import org.apache.geaflow.ai.retrieval.metadata.InMemoryMetadataStore;
import org.apache.geaflow.ai.retrieval.metadata.MetadataException;
import org.apache.geaflow.ai.retrieval.metadata.QualityCounters;
import org.apache.geaflow.ai.retrieval.model.document.SourceDocument;
import org.apache.geaflow.ai.retrieval.model.document.TextChunk;
import org.apache.geaflow.ai.retrieval.model.graph.EntityRef;
import org.apache.geaflow.ai.retrieval.model.graph.GraphEdgeRef;
import org.apache.geaflow.ai.retrieval.model.version.GraphVersion;
import org.junit.jupiter.api.Test;

/** Contract tests for the storage-neutral ingestion extension points. */
public class IngestionPipelineTest {

    private static final String SHA256 = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    @Test
    public void runsStagesInOrderAndReportsCounters() throws Exception {
        List<String> calls = new java.util.ArrayList<>();
        AtomicBoolean published = new AtomicBoolean();
        IngestionContext context = context();
        SourceDocument document = document();
        TextChunk chunk = chunk();
        TrackingGraphArtifact graph = new TrackingGraphArtifact(calls, "graph");
        TrackingIndexArtifact bm25 = new TrackingIndexArtifact(calls, "bm25");
        TrackingIndexArtifact vector = new TrackingIndexArtifact(calls, "vector");
        IngestionPipeline pipeline = new IngestionPipeline(
            value -> {
                calls.add("source");
                return Collections.singletonList(document);
            },
            (documents, value) -> {
                calls.add("normalize");
                return Collections.singletonList(chunk);
            },
            (chunks, value) -> {
                calls.add("extract");
                return new ExtractionResult(Collections.singletonList(entity()),
                    Collections.singletonList(edge()));
            },
            (value, documents, chunks, extraction) -> {
                calls.add("graph");
                return graph;
            },
            (value, chunks) -> {
                calls.add("bm25");
                return bm25;
            },
            (value, chunks) -> {
                calls.add("vector");
                return vector;
            },
            new MetadataPublisher() {
                @Override
                public ImportAttempt begin(IngestionContext value) {
                    calls.add("begin");
                    return new ImportAttempt(value.getGraphVersion());
                }

                @Override
                public ImportMetadata publish(IngestionContext value, ImportAttempt attempt,
                                              GraphArtifact valueGraph,
                                              List<IndexArtifact> indexes) {
                    calls.add("publish");
                    assertEquals(2, indexes.size());
                    published.set(true);
                    return null;
                }

                @Override
                public void fail(IngestionContext value, ImportAttempt attempt, Exception failure) {
                    throw new AssertionError("unexpected failure", failure);
                }
            });

        pipeline.run(context);

        assertEquals(Arrays.asList("begin", "source", "normalize", "extract", "graph",
            "bm25", "vector", "publish", "close-bm25", "close-vector", "close-graph"), calls);
        assertTrue(published.get());
        assertEquals(new QualityCounters(1, 1, 1, 1, 0, 0), context.getQualityCounters());
    }

    @Test
    public void failureClosesArtifactsAndNeverPublishes() throws Exception {
        AtomicBoolean published = new AtomicBoolean();
        AtomicBoolean failed = new AtomicBoolean();
        TrackingGraphArtifact graph = new TrackingGraphArtifact(new java.util.ArrayList<>(), "graph");
        TrackingIndexArtifact bm25 = new TrackingIndexArtifact(new java.util.ArrayList<>(), "bm25");
        IngestionPipeline pipeline = new IngestionPipeline(
            value -> Collections.singletonList(document()),
            (documents, value) -> Collections.singletonList(chunk()),
            (chunks, value) -> new ExtractionResult(Collections.emptyList(), Collections.emptyList()),
            (value, documents, chunks, extraction) -> graph,
            (value, chunks) -> bm25,
            (value, chunks) -> {
                throw new IllegalStateException("vector build failed");
            },
            new MetadataPublisher() {
                @Override
                public ImportAttempt begin(IngestionContext value) {
                    return new ImportAttempt(value.getGraphVersion());
                }

                @Override
                public ImportMetadata publish(IngestionContext value, ImportAttempt attempt,
                                              GraphArtifact valueGraph,
                                              List<IndexArtifact> indexes) {
                    published.set(true);
                    return null;
                }

                @Override
                public void fail(IngestionContext value, ImportAttempt attempt, Exception failure) {
                    failed.set(true);
                    assertTrue(failure.getMessage().contains("vector build failed"));
                }
            });

        try {
            pipeline.run(context());
        } catch (IllegalStateException expected) {
            assertEquals("vector build failed", expected.getMessage());
        }

        assertFalse(published.get());
        assertTrue(failed.get());
        assertTrue(graph.closed);
        assertTrue(bm25.closed);
    }

    @Test
    public void beginFailureDoesNotFailAnAttemptItDoesNotOwn() {
        AtomicBoolean failed = new AtomicBoolean();
        IngestionPipeline pipeline = new IngestionPipeline(
            value -> Collections.singletonList(document()),
            (documents, value) -> Collections.singletonList(chunk()),
            (chunks, value) -> new ExtractionResult(Collections.emptyList(), Collections.emptyList()),
            (value, documents, chunks, extraction) -> new TrackingGraphArtifact(
                new java.util.ArrayList<>(), "graph"),
            (value, chunks) -> new TrackingIndexArtifact(new java.util.ArrayList<>(), "bm25"),
            (value, chunks) -> new TrackingIndexArtifact(new java.util.ArrayList<>(), "vector"),
            new MetadataPublisher() {
                @Override
                public ImportAttempt begin(IngestionContext value) {
                    throw new IllegalStateException("begin failed");
                }

                @Override
                public ImportMetadata publish(IngestionContext value, ImportAttempt attempt,
                                              GraphArtifact graph, List<IndexArtifact> indexes) {
                    throw new AssertionError("unexpected publish");
                }

                @Override
                public void fail(IngestionContext value, ImportAttempt attempt, Exception failure) {
                    failed.set(true);
                }
            });

        org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class,
            () -> pipeline.run(context()));
        assertFalse(failed.get());
    }

    @Test
    public void metadataStorePublisherCompletesReadyLifecycle() throws Exception {
        String source = "source";
        String checksum = "41cf6794ba4200b839c53531555f0f3998df4cbb01a4d5cb0b94e3ca5e23947d";
        GraphVersion version = new GraphVersion("graph", "g1");
        DatasetManifest manifest = new DatasetManifest("v1", "dataset", "release", "dev", "uri", null,
            checksum, "preprocess-v1", new ChunkingConfiguration("chunk-v1", 100, 10),
            "schema-v1", "model", "v1", 7);
        IngestionContext context = new IngestionContext(manifest, version, "importer-v1");
        InMemoryMetadataStore store = new InMemoryMetadataStore();
        MetadataStorePublisher publisher = new MetadataStorePublisher(store,
            value -> new ByteArrayInputStream(source.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        TrackingGraphArtifact graph = new TrackingGraphArtifact(new java.util.ArrayList<>(), "graph",
            version, Arrays.asList("bm25", "vector"));
        TrackingIndexArtifact bm25 = new TrackingIndexArtifact(new java.util.ArrayList<>(), "bm25", version);
        TrackingIndexArtifact vector = new TrackingIndexArtifact(new java.util.ArrayList<>(), "vector", version);
        IngestionPipeline pipeline = new IngestionPipeline(
            value -> Collections.singletonList(document()),
            (documents, value) -> Collections.singletonList(chunk()),
            (chunks, value) -> new ExtractionResult(Collections.singletonList(entity()),
                Collections.singletonList(edge())),
            (value, documents, chunks, extraction) -> graph,
            (value, chunks) -> bm25,
            (value, chunks) -> vector,
            publisher);

        ImportMetadata result = pipeline.run(context);

        assertEquals(ImportState.READY, result.getState());
        assertEquals(version, store.getPublishedVersion("graph").get());
        assertEquals(Arrays.asList("bm25", "vector"), result.getGraph().getRequiredIndexes());
        assertEquals(2, result.getIndexes().size());
    }

    @Test
    public void rejectedConcurrentDuplicateLeavesOwnerAbleToPublishReady() throws Exception {
        String source = "source";
        String checksum = "41cf6794ba4200b839c53531555f0f3998df4cbb01a4d5cb0b94e3ca5e23947d";
        GraphVersion version = new GraphVersion("graph", "g1");
        DatasetManifest manifest = new DatasetManifest("v1", "dataset", "release", "dev", "uri", null,
            checksum, "preprocess-v1", new ChunkingConfiguration("chunk-v1", 100, 10),
            "schema-v1", "model", "v1", 7);
        IngestionContext ownerContext = new IngestionContext(manifest, version, "importer-v1");
        IngestionContext duplicateContext = new IngestionContext(manifest, version, "importer-v1");
        InMemoryMetadataStore store = new InMemoryMetadataStore();
        MetadataStorePublisher publisher = new MetadataStorePublisher(store,
            value -> new ByteArrayInputStream(source.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        CountDownLatch ownerStarted = new CountDownLatch(1);
        CountDownLatch ownerReleased = new CountDownLatch(1);
        TrackingGraphArtifact graph = new TrackingGraphArtifact(new java.util.ArrayList<>(), "graph",
            version, Arrays.asList("bm25", "vector"));
        TrackingIndexArtifact bm25 = new TrackingIndexArtifact(new java.util.ArrayList<>(), "bm25", version);
        TrackingIndexArtifact vector = new TrackingIndexArtifact(new java.util.ArrayList<>(), "vector", version);
        IngestionPipeline pipeline = new IngestionPipeline(value -> {
            ownerStarted.countDown();
            try {
                if (!ownerReleased.await(5, TimeUnit.SECONDS)) {
                    throw new java.io.IOException("owner release timed out");
                }
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
                throw new java.io.IOException(error);
            }
            return Collections.singletonList(document());
        }, (documents, value) -> Collections.singletonList(chunk()),
            (chunks, value) -> new ExtractionResult(Collections.singletonList(entity()),
                Collections.singletonList(edge())),
            (value, documents, chunks, extraction) -> graph,
            (value, chunks) -> bm25, (value, chunks) -> vector, publisher);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<ImportMetadata> owner = executor.submit(() -> pipeline.run(ownerContext));
            assertTrue(ownerStarted.await(5, TimeUnit.SECONDS));
            MetadataException rejected = assertThrows(MetadataException.class,
                () -> pipeline.run(duplicateContext));
            assertEquals(MetadataException.Code.VERSION_CONFLICT, rejected.getCode());
            assertEquals(ImportState.IMPORTING, store.find(version).get().getState());
            assertFalse(store.getPublishedVersion(version.getGraphName()).isPresent());

            ownerReleased.countDown();
            ImportMetadata result = owner.get(5, TimeUnit.SECONDS);
            assertNotNull(result);
            assertEquals(ImportState.READY, result.getState());
            assertEquals(ImportState.READY, store.find(version).get().getState());
            assertEquals(version, store.getPublishedVersion(version.getGraphName()).get());
            assertEquals(Arrays.asList("bm25", "vector"), result.getGraph().getRequiredIndexes());
            assertEquals(2, result.getIndexes().size());
            assertTrue(graph.closed);
            assertTrue(bm25.closed);
            assertTrue(vector.closed);
        } finally {
            ownerReleased.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    public void concurrentDuplicateDoesNotFailOwnerAndForeignTokenCannotPublish() throws Exception {
        IngestionContext context = context();
        InMemoryMetadataStore store = new InMemoryMetadataStore();
        MetadataStorePublisher publisher = new MetadataStorePublisher(store,
            ignored -> new ByteArrayInputStream(new byte[0]));
        CountDownLatch ownerStarted = new CountDownLatch(1);
        CountDownLatch ownerReleased = new CountDownLatch(1);
        AtomicBoolean ownerFailed = new AtomicBoolean();
        IngestionPipeline pipeline = new IngestionPipeline(value -> {
            ownerStarted.countDown();
            try {
                if (!ownerReleased.await(5, TimeUnit.SECONDS)) {
                    throw new java.io.IOException("owner release timed out");
                }
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
                throw new java.io.IOException(error);
            }
            throw new java.io.IOException("owner build failure");
        }, (documents, value) -> Collections.singletonList(chunk()),
            (chunks, value) -> new ExtractionResult(Collections.emptyList(), Collections.emptyList()),
            (value, documents, chunks, extraction) -> new TrackingGraphArtifact(new java.util.ArrayList<>(), "graph"),
            (value, chunks) -> new TrackingIndexArtifact(new java.util.ArrayList<>(), "bm25"),
            (value, chunks) -> new TrackingIndexArtifact(new java.util.ArrayList<>(), "vector"), publisher);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> owner = executor.submit(() -> {
                assertThrows(java.io.IOException.class, () -> pipeline.run(context));
                ownerFailed.set(true);
            });
            assertTrue(ownerStarted.await(5, TimeUnit.SECONDS));
            assertThrows(MetadataException.class, () -> pipeline.run(context));
            assertEquals(ImportState.IMPORTING, store.find(context.getGraphVersion()).get().getState());
            ImportAttempt foreign = new ImportAttempt(context.getGraphVersion());
            publisher.fail(context, foreign, new java.io.IOException("foreign failure"));
            assertThrows(MetadataException.class, () -> publisher.publish(context, foreign,
                new TrackingGraphArtifact(new java.util.ArrayList<>(), "graph"), Collections.emptyList()));
            assertEquals(ImportState.IMPORTING, store.find(context.getGraphVersion()).get().getState());
            ownerReleased.countDown();
            owner.get(5, TimeUnit.SECONDS);
            assertTrue(ownerFailed.get());
            assertEquals(ImportState.FAILED, store.find(context.getGraphVersion()).get().getState());
        } finally {
            ownerReleased.countDown();
            executor.shutdownNow();
        }
    }

    private static IngestionContext context() {
        DatasetManifest manifest = new DatasetManifest("v1", "hotpotqa", "release-1", "train",
            null, "cache/hotpotqa", SHA256, "normalizer-1",
            new ChunkingConfiguration("chunker-1", 100, 10), "graph-schema-1", "fixture", "v1", 7);
        return new IngestionContext(manifest, new GraphVersion("graph-1", "version-1"), "importer-1");
    }

    private static SourceDocument document() {
        return new SourceDocument("doc-1", "hotpotqa", "release-1", "train", "uri:doc-1", SHA256);
    }

    private static TextChunk chunk() {
        return new TextChunk("chunk-1", "doc-1", 0, 0, 10, 3, "A fixed chunk.");
    }

    private static EntityRef entity() {
        return new EntityRef("entity-1", "Fixed entity", "person");
    }

    private static GraphEdgeRef edge() {
        return new GraphEdgeRef("edge-1", "related", "entity-1", "entity-1");
    }

    private static final class TrackingGraphArtifact implements GraphArtifact {
        private final List<String> calls;
        private final String name;
        private final GraphVersion version;
        private final List<String> requiredIndexes;
        private boolean closed;

        private TrackingGraphArtifact(List<String> calls, String name) {
            this(calls, name, new GraphVersion("graph-1", "version-1"), Collections.emptyList());
        }

        private TrackingGraphArtifact(List<String> calls, String name, GraphVersion version,
                                      List<String> requiredIndexes) {
            this.calls = calls;
            this.name = name;
            this.version = version;
            this.requiredIndexes = requiredIndexes;
        }

        @Override
        public GraphBuildMetadata getMetadata() {
            return new GraphBuildMetadata(version, "memory:" + name, "fixture", "builder-1", true,
                requiredIndexes);
        }

        @Override
        public void close() {
            closed = true;
            calls.add("close-" + name);
        }
    }

    private static final class TrackingIndexArtifact implements IndexArtifact {
        private final List<String> calls;
        private final String name;
        private final GraphVersion version;
        private boolean closed;

        private TrackingIndexArtifact(List<String> calls, String name) {
            this(calls, name, new GraphVersion("graph-1", "version-1"));
        }

        private TrackingIndexArtifact(List<String> calls, String name, GraphVersion version) {
            this.calls = calls;
            this.name = name;
            this.version = version;
        }

        @Override
        public IndexBuildMetadata getMetadata() {
            return new IndexBuildMetadata(version,
                new org.apache.geaflow.ai.retrieval.model.version.IndexVersion(name, "v1", version.getVersion()),
                name, "builder-1", "memory:" + name, true);
        }

        @Override
        public void close() {
            closed = true;
            calls.add("close-" + name);
        }
    }
}
