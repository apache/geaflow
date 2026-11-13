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
package org.apache.geaflow.ai.retrieval.service;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalErrorCode;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalException;
import org.apache.geaflow.ai.retrieval.channel.graph.EntityAnchorResolver;
import org.apache.geaflow.ai.retrieval.channel.graph.GraphNeighborProvider;
import org.apache.geaflow.ai.retrieval.channel.graph.InMemoryGraphNeighborProvider;
import org.apache.geaflow.ai.retrieval.execution.LexicalStatistics;
import org.apache.geaflow.ai.retrieval.index.bm25.Bm25IndexReader;
import org.apache.geaflow.ai.retrieval.index.graph.GraphArtifactReader;
import org.apache.geaflow.ai.retrieval.index.vector.VectorIndexReader;
import org.apache.geaflow.ai.retrieval.model.document.RetrievalIds;
import org.apache.geaflow.ai.retrieval.model.document.TextChunk;
import org.apache.geaflow.ai.retrieval.model.graph.EntityRef;
import org.apache.geaflow.ai.retrieval.model.graph.GraphEdgeRef;
import org.noear.solon.annotation.Component;

/**
 * Session-free registry of the channels published for one logical graph version.
 *
 * <p>Holds an atomically published bundle - BM25, vector, graph and the graph provider that backs
 * bounded expansion - together with the leases that keep a graph version alive for the duration of
 * a request. The name says "channel" rather than "fixture" because this is the production
 * container the server injects; test fixtures only populate it.
 */
@Component
public final class RetrievalChannelRegistry implements AutoCloseable {
    private final Map<String, Fixture> fixtures = new HashMap<>();
    private boolean closed;

    public synchronized void register(String graphName, String graphVersion, String indexVersion,
                                      Bm25IndexReader bm25, VectorIndexReader vector,
                                      List<TextChunk> chunks, List<EntityRef> entities,
                                      List<GraphEdgeRef> edges, boolean ready) {
        register(graphName, graphVersion, indexVersion, bm25, vector, chunks, entities, edges, ready,
            new InMemoryGraphNeighborProvider(edges), Collections.emptyMap());
    }

    private synchronized void register(String graphName, String graphVersion, String indexVersion,
                                       Bm25IndexReader bm25, VectorIndexReader vector,
                                       List<TextChunk> chunks, List<EntityRef> entities, List<GraphEdgeRef> edges,
                                       boolean ready, GraphNeighborProvider graphProvider,
                                       Map<String, RetrievalException> failures) {
        register(graphName, graphVersion, indexVersion, bm25, vector, chunks, entities, edges, ready,
            graphProvider, failures, false);
    }

    private synchronized void register(String graphName, String graphVersion, String indexVersion,
                                       Bm25IndexReader bm25, VectorIndexReader vector,
                                       List<TextChunk> chunks, List<EntityRef> entities, List<GraphEdgeRef> edges,
                                       boolean ready, GraphNeighborProvider graphProvider,
                                       Map<String, RetrievalException> failures, boolean immutableEdges) {
        boolean registered = false;
        try {
            requireVersion(graphName, graphVersion, indexVersion);
            if (chunks == null || entities == null || edges == null
                || (bm25 == null && vector == null && graphProvider == null
                    && !fixtures.containsKey(key(graphName, graphVersion, indexVersion)))) {
                throw notReady("fixture contains no usable channel or incomplete graph fixture");
            }
            Map<String, TextChunk> chunksById = new HashMap<>();
            for (TextChunk chunk : chunks) {
                if (chunk == null || chunksById.put(chunk.getChunkId(), chunk) != null
                    || chunk.getSourceUri() == null) {
                    throw notReady("invalid, duplicate, or untraceable fixture chunk");
                }
            }
            validateGraphReferences(entities, edges, chunksById);
            if (bm25 != null) {
                bm25.getIdentity().validate(graphName, graphVersion, indexVersion, chunks);
            }
            if (vector != null) {
                vector.getIdentity().validate(graphName, graphVersion, indexVersion, chunks);
            }
            ensureOpen();
            String fixtureKey = key(graphName, graphVersion, indexVersion);
            Fixture previous = fixtures.get(fixtureKey);
            GraphNeighborProvider publishedGraphProvider = graphProvider;
            if (publishedGraphProvider == null) {
                publishedGraphProvider = previous == null
                    ? new InMemoryGraphNeighborProvider(edges) : previous.getGraphProvider();
            }
            Fixture replacement = new Fixture(graphName, graphVersion, indexVersion, bm25, vector,
                chunks, entities, edges, publishedGraphProvider, ready, failures, immutableEdges);
            fixtures.put(fixtureKey, replacement);
            registered = true;
            if (previous != null) {
                previous.retire();
            }
        } catch (RuntimeException error) {
            // Registration takes ownership of readers. Release them if validation or publication fails.
            if (!registered) {
                closeReaders(bm25, vector, error);
            }
            throw error;
        }
    }

    /** Loads channels independently, preserving typed failures for explicit partial retrieval. */
    public void load(String graphName, String graphVersion, String indexVersion,
                     Path bm25Artifact, Path vectorArtifact, List<TextChunk> chunks,
                     List<EntityRef> entities, List<GraphEdgeRef> edges,
                     String vectorSource, String vectorVersion) {
        requireVersion(graphName, graphVersion, indexVersion);
        Map<String, RetrievalException> failures = new LinkedHashMap<>();
        Bm25IndexReader bm25 = null;
        VectorIndexReader vector = null;
        try {
            bm25 = new Bm25IndexReader(bm25Artifact, graphVersion, indexVersion);
            bm25.getIdentity().validate(graphName, graphVersion, indexVersion, chunks);
        } catch (RetrievalException error) {
            if (bm25 != null) {
                try {
                    bm25.close();
                } catch (IOException closeError) {
                    error.addSuppressed(closeError);
                }
                bm25 = null;
            }
            failures.put("BM25", error);
        }
        try {
            vector = new VectorIndexReader(vectorArtifact, graphVersion, indexVersion, vectorSource, vectorVersion);
            vector.getIdentity().validate(graphName, graphVersion, indexVersion, chunks);
        } catch (RetrievalException error) {
            if (vector != null) {
                vector.close();
            }
            vector = null;
            failures.put("VECTOR", error);
        }
        if (bm25 == null && vector == null) {
            throw notReady("no usable BM25 or vector artifact");
        }
        register(graphName, graphVersion, indexVersion, bm25, vector, chunks, entities, edges, true,
            null, failures);
    }

    public void loadGraph(String graphName, String graphVersion, String indexVersion, Path graphArtifact,
                           List<TextChunk> chunks) {
        GraphArtifactReader reader = new GraphArtifactReader(graphArtifact, graphVersion, indexVersion);
        try {
            reader.getIdentity().validate(graphName, graphVersion, indexVersion, chunks);
            reader.validateReferences(chunks);
            register(graphName, graphVersion, indexVersion, null, null, chunks, reader.getEntities(),
                reader.getEdges(), true, reader.getNeighbors(), Collections.emptyMap(), true);
        } catch (RuntimeException error) {
            reader.close();
            throw error;
        }
    }

    /** Loads graph, BM25, and vector artifacts and publishes them as one fixture. */
    public void loadBundle(String graphName, String graphVersion, String indexVersion,
                           Path graphArtifact, Path bm25Artifact, Path vectorArtifact,
                           List<TextChunk> chunks, String vectorSource, String vectorVersion) {
        requireVersion(graphName, graphVersion, indexVersion);
        Objects.requireNonNull(chunks, "chunks");
        GraphArtifactReader graph = new GraphArtifactReader(graphArtifact, graphVersion, indexVersion);
        Bm25IndexReader bm25 = null;
        VectorIndexReader vector = null;
        boolean published = false;
        try {
            graph.getIdentity().validate(graphName, graphVersion, indexVersion, chunks);
            graph.validateReferences(chunks);
            bm25 = new Bm25IndexReader(bm25Artifact, graphVersion, indexVersion);
            bm25.getIdentity().validate(graphName, graphVersion, indexVersion, chunks);
            vector = new VectorIndexReader(vectorArtifact, graphVersion, indexVersion,
                vectorSource, vectorVersion);
            vector.getIdentity().validate(graphName, graphVersion, indexVersion, chunks);
            register(graphName, graphVersion, indexVersion, bm25, vector, chunks,
                graph.getEntities(), graph.getEdges(), true, graph.getNeighbors(), Collections.emptyMap(), true);
            published = true;
        } finally {
            if (!published) {
                closeReaders(bm25, vector, new IllegalStateException("bundle load failed"));
            }
            graph.close();
        }
    }

    /** Loads rebuilt indexes with a graph provider assembled from a snapshot and delta chain. */
    public void loadBundle(String graphName, String graphVersion, String indexVersion,
                           Path bm25Artifact, Path vectorArtifact, List<TextChunk> chunks,
                           List<EntityRef> entities, List<GraphEdgeRef> edges,
                           GraphNeighborProvider graphProvider,
                           String vectorSource, String vectorVersion) {
        requireVersion(graphName, graphVersion, indexVersion);
        Objects.requireNonNull(chunks, "chunks");
        Objects.requireNonNull(graphProvider, "graphProvider");
        Bm25IndexReader bm25 = null;
        VectorIndexReader vector = null;
        boolean published = false;
        try {
            bm25 = new Bm25IndexReader(bm25Artifact, graphVersion, indexVersion);
            bm25.getIdentity().validate(graphName, graphVersion, indexVersion, chunks);
            vector = new VectorIndexReader(vectorArtifact, graphVersion, indexVersion,
                vectorSource, vectorVersion);
            vector.getIdentity().validate(graphName, graphVersion, indexVersion, chunks);
            register(graphName, graphVersion, indexVersion, bm25, vector, chunks,
                entities, edges, true, graphProvider, Collections.emptyMap());
            published = true;
        } finally {
            if (!published) {
                closeReaders(bm25, vector, new IllegalStateException("bundle load failed"));
            }
        }
    }

    public synchronized Fixture get(String graphName, String graphVersion, String indexVersion) {
        requireVersion(graphName, graphVersion, indexVersion);
        Fixture fixture = fixtures.get(key(graphName, graphVersion, indexVersion));
        if (fixture == null || !fixture.isReady()) {
            throw notReady("fixture is missing or not READY");
        }
        return fixture;
    }

    /** Returns whether any published READY fixture exists for a graph. */
    public synchronized boolean isReady(String graphName) {
        if (graphName == null) {
            return false;
        }
        for (Fixture fixture : fixtures.values()) {
            if (graphName.equals(fixture.getGraphName()) && fixture.isReady()) {
                return true;
            }
        }
        return false;
    }

    /** Returns whether the exact graph, graph version, and index version are READY. */
    public synchronized boolean isReady(String graphName, String graphVersion, String indexVersion) {
        if (graphName == null || graphVersion == null || indexVersion == null) {
            return false;
        }
        Fixture fixture = fixtures.get(key(graphName, graphVersion, indexVersion));
        return fixture != null && fixture.isReady();
    }

    /**
     * Reports readiness per artifact component instead of a single boolean.
     *
     * <p>A published fixture is only usable when its graph provider and its graph-backed readers
     * are individually present and open. Reporting the components lets {@code /ready} fail with the
     * specific missing artifact rather than staying green because some fixture exists.
     */
    public synchronized ReadinessReport readiness(String graphName, String graphVersion,
                                                  String indexVersion) {
        Map<String, String> components = new LinkedHashMap<>();
        if (graphName == null) {
            return new ReadinessReport(false, components, "MISSING_GRAPH_NAME");
        }
        Fixture fixture = graphVersion == null || indexVersion == null
            ? firstFixture(graphName) : fixtures.get(key(graphName, graphVersion, indexVersion));
        if (fixture == null) {
            return new ReadinessReport(false, components, "GRAPH_NOT_READY");
        }
        components.put("graph", component(fixture.isReady() && fixture.getGraphProvider() != null,
            "GRAPH_NOT_READY"));
        components.put("bm25", component(fixture.getBm25() != null, "BM25_NOT_READY"));
        components.put("vector", component(fixture.getVector() != null, "VECTOR_NOT_READY"));
        if (!fixture.isReady()) {
            return new ReadinessReport(false, components, "GRAPH_NOT_READY");
        }
        for (Map.Entry<String, String> entry : components.entrySet()) {
            if (!"READY".equals(entry.getValue())) {
                return new ReadinessReport(false, components, entry.getValue());
            }
        }
        return new ReadinessReport(true, components, "READY");
    }

    private Fixture firstFixture(String graphName) {
        for (Fixture fixture : fixtures.values()) {
            if (graphName.equals(fixture.getGraphName())) {
                return fixture;
            }
        }
        return null;
    }

    private static String component(boolean ready, String failure) {
        return ready ? "READY" : failure;
    }

    /** Per-component readiness outcome. */
    public static final class ReadinessReport {
        private final boolean ready;
        private final Map<String, String> components;
        private final String reason;

        ReadinessReport(boolean ready, Map<String, String> components, String reason) {
            this.ready = ready;
            this.components = Collections.unmodifiableMap(new LinkedHashMap<>(components));
            this.reason = reason;
        }

        public boolean isReady() {
            return ready;
        }

        public Map<String, String> getComponents() {
            return components;
        }

        public String getReason() {
            return reason;
        }
    }

    /** Acquires a lease that keeps the selected fixture readers open until released. */
    public synchronized Fixture.Lease acquire(String graphName, String graphVersion, String indexVersion) {
        return get(graphName, graphVersion, indexVersion).acquire();
    }

    public synchronized void clear() {
        RuntimeException failure = null;
        for (Fixture fixture : fixtures.values()) {
            try {
                fixture.retire();
            } catch (RuntimeException error) {
                if (failure == null) {
                    failure = new IllegalStateException("failed to close fixture readers", error);
                } else {
                    failure.addSuppressed(error);
                }
            }
        }
        fixtures.clear();
        if (failure != null) {
            throw failure;
        }
    }

    /** Closes all published readers and prevents further registrations. */
    @Override
    public synchronized void close() {
        if (!closed) {
            closed = true;
            clear();
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("fixture registry is closed");
        }
    }

    private static void closeReaders(Bm25IndexReader bm25, VectorIndexReader vector,
                                     Exception failure) {
        Exception closeFailure = null;
        if (bm25 != null) {
            try {
                bm25.close();
            } catch (IOException closeError) {
                failure.addSuppressed(closeError);
                closeFailure = closeError;
            }
        }
        if (vector != null) {
            try {
                vector.close();
            } catch (RuntimeException closeError) {
                failure.addSuppressed(closeError);
                if (closeFailure != null) {
                    closeFailure.addSuppressed(closeError);
                }
            }
        }
    }

    private static void validateGraphReferences(List<EntityRef> entities, List<GraphEdgeRef> edges,
                                                Map<String, TextChunk> chunks) {
        if (chunks.values().stream().noneMatch(chunk -> chunk.getDataset() != null)) {
            return;
        }
        for (EntityRef entity : entities) {
            if (entity.getDataset() != null && entity.getSourceChunkIds().isEmpty()) {
                throw notReady("entity " + entity.getEntityId()
                    + " requires source chunks for scoped provenance");
            }
            validateGraphReference(entity.getDataset(), entity.getDatasetVersion(), entity.getSplit(),
                entity.getSourceHash(), entity.getSourceChunkIds(), chunks,
                "entity " + entity.getEntityId());
        }
        for (GraphEdgeRef edge : edges) {
            validateGraphReference(edge.getDataset(), edge.getDatasetVersion(), edge.getSplit(),
                edge.getSourceHash(), edge.getSourceChunkIds(), chunks,
                "edge " + edge.getEdgeId());
        }
    }

    private static void validateGraphReference(String dataset, String datasetVersion, String split,
                                               String sourceHash, List<String> sourceChunkIds,
                                               Map<String, TextChunk> chunks, String owner) {
        if (sourceChunkIds.isEmpty()) {
            if (dataset == null && chunks.values().stream()
                .anyMatch(chunk -> chunk.getDataset() != null)) {
                throw notReady(owner + " is missing provenance for scoped chunks");
            }
            return;
        }
        java.util.Set<String> sourceHashes = new java.util.TreeSet<>();
        boolean hasScopedChunk = false;
        boolean hasUnscopedChunk = false;
        for (String chunkId : sourceChunkIds) {
            TextChunk chunk = chunks.get(chunkId);
            if (chunk == null) {
                throw notReady(owner + " references unknown chunk " + chunkId);
            }
            if (chunk.getDataset() == null) {
                hasUnscopedChunk = true;
            } else {
                hasScopedChunk = true;
                sourceHashes.add(chunk.getSourceHash());
            }
        }
        if (dataset == null) {
            if (hasScopedChunk) {
                throw notReady(owner + " is missing provenance for scoped chunks");
            }
            return;
        }
        if (hasUnscopedChunk) {
            throw notReady(owner + " provenance references an unscoped chunk");
        }
        for (String chunkId : sourceChunkIds) {
            TextChunk chunk = chunks.get(chunkId);
            if (!dataset.equals(chunk.getDataset())
                || !datasetVersion.equals(chunk.getDatasetVersion())
                || !split.equals(chunk.getSplit())) {
                throw notReady(owner + " provenance does not match referenced chunk");
            }
        }
        if (!sourceHash.equals(RetrievalIds.aggregateSourceHash(
            new ArrayList<>(sourceHashes)))) {
            throw notReady(owner + " sourceHash does not match referenced chunks");
        }
    }

    private static void requireVersion(String graphName, String graphVersion, String indexVersion) {
        if (blank(graphName) || blank(graphVersion) || blank(indexVersion)) {
            throw notReady("graph/index versions are missing");
        }
    }

    private static String key(String graphName, String graphVersion, String indexVersion) {
        return graphName + "\u0000" + graphVersion + "\u0000" + indexVersion;
    }

    private static boolean blank(String value) {
        return value == null || value.trim().isEmpty();
    }

    private static RetrievalException notReady(String message) {
        return new RetrievalException(RetrievalErrorCode.INDEX_NOT_READY, message);
    }

    /** Immutable published fixture. */
    public static final class Fixture {
        private final String graphName;
        private final String graphVersion;
        private final String indexVersion;
        private final Bm25IndexReader bm25;
        private final VectorIndexReader vector;
        private final List<TextChunk> chunks;
        private final Map<String, TextChunk> chunksById;
        private final List<EntityRef> entities;
        private final List<GraphEdgeRef> edges;
        private final GraphNeighborProvider graphProvider;
        private final EntityAnchorResolver anchorResolver;
        private final Map<String, Double> idfByToken;
        private final boolean ready;
        private final Map<String, RetrievalException> failures;
        private boolean closed;
        private boolean retired;
        private int activeLeases;

        private Fixture(String graphName, String graphVersion, String indexVersion,
                        Bm25IndexReader bm25, VectorIndexReader vector, List<TextChunk> chunks,
                        List<EntityRef> entities, List<GraphEdgeRef> edges, GraphNeighborProvider graphProvider,
                        boolean ready, Map<String, RetrievalException> failures, boolean immutableEdges) {
            this.graphName = graphName;
            this.graphVersion = graphVersion;
            this.indexVersion = indexVersion;
            this.bm25 = bm25;
            this.vector = vector;
            this.chunks = immutable(chunks);
            Map<String, TextChunk> indexedChunks = new HashMap<>();
            for (TextChunk chunk : this.chunks) {
                indexedChunks.put(chunk.getChunkId(), chunk);
            }
            this.chunksById = Collections.unmodifiableMap(indexedChunks);
            this.entities = immutable(entities);
            this.edges = immutableEdges ? Collections.unmodifiableList(edges) : immutable(edges);
            this.graphProvider = graphProvider;
            this.anchorResolver = new EntityAnchorResolver(this.entities);
            this.idfByToken = LexicalStatistics.buildIdfIndex(this.chunks);
            this.ready = ready;
            this.failures = Collections.unmodifiableMap(new LinkedHashMap<>(failures));
        }

        private static <T> List<T> immutable(List<T> values) {
            return Collections.unmodifiableList(new ArrayList<>(values));
        }

        public String getGraphName() {
            return graphName;
        }

        public String getGraphVersion() {
            return graphVersion;
        }

        public String getIndexVersion() {
            return indexVersion;
        }

        public Bm25IndexReader getBm25() {
            return bm25;
        }

        public VectorIndexReader getVector() {
            return vector;
        }

        public List<TextChunk> getChunks() {
            return chunks;
        }

        public Map<String, TextChunk> getChunksById() {
            return chunksById;
        }

        public List<EntityRef> getEntities() {
            return entities;
        }

        public List<GraphEdgeRef> getEdges() {
            return edges;
        }

        public GraphNeighborProvider getGraphProvider() {
            return graphProvider;
        }

        public EntityAnchorResolver getAnchorResolver() {
            return anchorResolver;
        }

        public Map<String, Double> getIdfByToken() {
            return idfByToken;
        }

        public Map<String, RetrievalException> getFailures() {
            return failures;
        }

        public boolean isReady() {
            return ready;
        }

        private synchronized Lease acquire() {
            if (closed || retired && activeLeases == 0) {
                throw new IllegalStateException("fixture is retired");
            }
            activeLeases++;
            return new Lease(this);
        }

        Lease acquireLease() {
            return acquire();
        }

        /** Retires the fixture, deferring reader closure while leases remain active. */
        private synchronized void retire() {
            retired = true;
            closeIfUnused();
        }

        private synchronized void release() {
            if (activeLeases <= 0) {
                throw new IllegalStateException("fixture lease released more than once");
            }
            activeLeases--;
            closeIfUnused();
        }

        private void closeIfUnused() {
            if (!retired || closed || activeLeases != 0) {
                return;
            }
            closed = true;
            Exception failure = null;
            if (bm25 != null) {
                try {
                    bm25.close();
                } catch (IOException | RuntimeException error) {
                    failure = error;
                }
            }
            if (vector != null) {
                try {
                    vector.close();
                } catch (RuntimeException error) {
                    if (failure == null) {
                        failure = error;
                    } else {
                        failure.addSuppressed(error);
                    }
                }
            }
            if (failure != null) {
                throw new IllegalStateException("failed to close fixture readers", failure);
            }
        }

        /** A scoped reader lease held for the duration of one retrieval. */
        public static final class Lease implements AutoCloseable {
            private Fixture fixture;

            private Lease(Fixture fixture) {
                this.fixture = fixture;
            }

            public Fixture getFixture() {
                if (fixture == null) {
                    throw new IllegalStateException("fixture lease is closed");
                }
                return fixture;
            }

            @Override
            public void close() {
                Fixture leased = fixture;
                if (leased != null) {
                    fixture = null;
                    leased.release();
                }
            }
        }
    }
}
