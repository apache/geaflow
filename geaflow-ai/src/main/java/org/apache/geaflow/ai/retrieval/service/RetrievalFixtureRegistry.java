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
import org.apache.geaflow.ai.retrieval.api.model.RetrievalErrorCode;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalException;
import org.apache.geaflow.ai.retrieval.channel.graph.GraphNeighborProvider;
import org.apache.geaflow.ai.retrieval.channel.graph.InMemoryGraphNeighborProvider;
import org.apache.geaflow.ai.retrieval.index.bm25.Bm25IndexReader;
import org.apache.geaflow.ai.retrieval.index.graph.GraphArtifactReader;
import org.apache.geaflow.ai.retrieval.index.vector.VectorIndexReader;
import org.apache.geaflow.ai.retrieval.model.document.TextChunk;
import org.apache.geaflow.ai.retrieval.model.graph.EntityRef;
import org.apache.geaflow.ai.retrieval.model.graph.GraphEdgeRef;

/** Session-free registry for an atomically published GraphRAG fixture. */
public final class RetrievalFixtureRegistry implements AutoCloseable {
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
        boolean registered = false;
        try {
            requireVersion(graphName, graphVersion, indexVersion);
            if (chunks == null || entities == null || edges == null
                || graphProvider == null || (bm25 == null && vector == null && graphProvider == null)) {
                throw notReady("fixture contains no usable channel or incomplete graph fixture");
            }
            java.util.Set<String> chunkIds = new java.util.HashSet<>();
            for (TextChunk chunk : chunks) {
                if (chunk == null || !chunkIds.add(chunk.getChunkId()) || chunk.getSourceUri() == null) {
                    throw notReady("invalid, duplicate, or untraceable fixture chunk");
                }
            }
            if (bm25 != null) {
                bm25.getIdentity().validate(graphName, graphVersion, indexVersion, chunks);
            }
            if (vector != null) {
                vector.getIdentity().validate(graphName, graphVersion, indexVersion, chunks);
            }
            ensureOpen();
            String fixtureKey = key(graphName, graphVersion, indexVersion);
            Fixture replacement = new Fixture(graphName, graphVersion, indexVersion, bm25, vector,
                chunks, entities, edges, graphProvider, ready, failures);
            Fixture previous = fixtures.put(fixtureKey, replacement);
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
            new InMemoryGraphNeighborProvider(edges), failures);
    }

    public void loadGraph(String graphName, String graphVersion, String indexVersion, Path graphArtifact,
                           List<TextChunk> chunks) {
        GraphArtifactReader reader = new GraphArtifactReader(graphArtifact, graphVersion, indexVersion);
        try {
            reader.getIdentity().validate(graphName, graphVersion, indexVersion, chunks);
            reader.validateReferences(chunks);
            register(graphName, graphVersion, indexVersion, null, null, chunks, reader.getEntities(),
                reader.getEdges(), true, reader.getNeighbors(), Collections.emptyMap());
        } catch (RuntimeException error) {
            reader.close();
            throw error;
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

    private static void requireVersion(String graphName, String graphVersion, String indexVersion) {
        if (blank(graphName) || blank(graphVersion) || blank(indexVersion)
            || !graphVersion.equals(indexVersion)) {
            throw notReady("graph/index versions are missing or inconsistent");
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
        private final boolean ready;
        private final Map<String, RetrievalException> failures;
        private boolean closed;
        private boolean retired;
        private int activeLeases;

        private Fixture(String graphName, String graphVersion, String indexVersion,
                        Bm25IndexReader bm25, VectorIndexReader vector, List<TextChunk> chunks,
                        List<EntityRef> entities, List<GraphEdgeRef> edges, GraphNeighborProvider graphProvider,
                        boolean ready,
                        Map<String, RetrievalException> failures) {
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
            this.edges = immutable(edges);
            this.graphProvider = graphProvider;
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

        public Map<String, RetrievalException> getFailures() {
            return failures;
        }

        public boolean isReady() {
            return ready;
        }

        private synchronized Lease acquire() {
            if (retired || closed) {
                throw new IllegalStateException("fixture is retired");
            }
            activeLeases++;
            return new Lease(this);
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
