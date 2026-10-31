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

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.geaflow.ai.retrieval.index.IndexArtifact;
import org.apache.geaflow.ai.retrieval.metadata.GraphBuildMetadata;
import org.apache.geaflow.ai.retrieval.metadata.ImportMetadata;
import org.apache.geaflow.ai.retrieval.metadata.ImportState;
import org.apache.geaflow.ai.retrieval.metadata.MetadataException;
import org.apache.geaflow.ai.retrieval.metadata.MetadataStore;
import org.apache.geaflow.ai.retrieval.model.version.GraphVersion;

/**
 * Metadata publisher backed by the metadata store lifecycle.
 *
 * <p>The publisher verifies the source before recording graph and index metadata and captures the
 * published pointer at begin time for compare-and-set publication. The source stream is opened
 * only for verification and is always closed by this publisher.</p>
 */
public final class MetadataStorePublisher implements MetadataPublisher {

    private static final List<String> REQUIRED_INDEXES = Collections.unmodifiableList(
        Arrays.asList("bm25", "vector"));

    private final MetadataStore store;
    private final SourceStreamProvider sourceProvider;
    private final Map<GraphVersion, AttemptState> attempts = new HashMap<>();

    public MetadataStorePublisher(MetadataStore store, SourceStreamProvider sourceProvider) {
        this.store = Objects.requireNonNull(store, "store");
        this.sourceProvider = Objects.requireNonNull(sourceProvider, "sourceProvider");
    }

    @Override
    public synchronized ImportAttempt begin(IngestionContext context) {
        Objects.requireNonNull(context, "context");
        GraphVersion version = context.getGraphVersion();
        if (attempts.containsKey(version)) {
            throw new MetadataException(MetadataException.Code.VERSION_CONFLICT,
                "ingestion attempt already started");
        }
        ImportAttempt token = new ImportAttempt(version);
        GraphBuildMetadata graph = new GraphBuildMetadata(version, null, "graph", "pending",
            false, REQUIRED_INDEXES);
        GraphVersion expected = store.getPublishedVersion(version.getGraphName()).orElse(null);
        store.begin(context.getManifest(), context.getImporterVersion(), graph);
        attempts.put(version, new AttemptState(token, expected));
        return token;
    }

    @Override
    public synchronized ImportMetadata publish(IngestionContext context, ImportAttempt token, GraphArtifact graph,
                                               List<IndexArtifact> indexes) throws IOException {
        Objects.requireNonNull(context, "context");
        Objects.requireNonNull(token, "attempt");
        Objects.requireNonNull(graph, "graph");
        Objects.requireNonNull(indexes, "indexes");
        GraphVersion version = context.getGraphVersion();
        final AttemptState attempt = requireAttempt(version, token);
        GraphBuildMetadata graphMetadata = graph.getMetadata();
        if (!version.equals(graphMetadata.getGraphVersion())
            || !REQUIRED_INDEXES.equals(graphMetadata.getRequiredIndexes())) {
            throw new MetadataException(MetadataException.Code.INVALID_METADATA,
                "graph metadata must match the ingestion version and required indexes");
        }
        try (InputStream source = openSource(context)) {
            store.verifySource(version, source);
        }
        store.finishImport(version, graphMetadata, context.getQualityCounters());
        for (IndexArtifact index : indexes) {
            Objects.requireNonNull(index, "index");
            store.putIndex(version, index.getMetadata());
        }
        ImportMetadata published = store.publish(version, attempt.expectedPublishedVersion);
        attempts.remove(version);
        return published;
    }

    @Override
    public synchronized void fail(IngestionContext context, ImportAttempt token, Exception failure)
        throws IOException {
        Objects.requireNonNull(context, "context");
        Objects.requireNonNull(token, "attempt");
        Objects.requireNonNull(failure, "failure");
        GraphVersion version = context.getGraphVersion();
        AttemptState attempt = attempts.get(version);
        if (attempt == null || !sameToken(attempt.token, token)) {
            return;
        }
        attempts.remove(version);
        ImportMetadata current = store.find(version).orElse(null);
        if (current == null || current.getState() == ImportState.FAILED
            || current.getState() == ImportState.READY) {
            return;
        }
        MetadataException.Code code = failure instanceof MetadataException
            ? ((MetadataException) failure).getCode() : MetadataException.Code.INVALID_METADATA;
        String reason = failure.getMessage() == null ? failure.getClass().getName() : failure.getMessage();
        store.fail(version, code, reason);
    }

    private InputStream openSource(IngestionContext context) throws IOException {
        InputStream source = sourceProvider.open(context);
        if (source == null) {
            throw new IOException("source provider returned null");
        }
        return source;
    }

    private AttemptState requireAttempt(GraphVersion version, ImportAttempt token) {
        AttemptState attempt = attempts.get(version);
        if (attempt == null || !sameToken(attempt.token, token)) {
            throw new MetadataException(MetadataException.Code.INVALID_TRANSITION,
                "ingestion attempt does not own this version");
        }
        return attempt;
    }

    private static boolean sameToken(ImportAttempt expected, ImportAttempt actual) {
        return expected == actual && expected.getAttemptId().equals(actual.getAttemptId());
    }

    private static final class AttemptState {
        private final ImportAttempt token;
        private final GraphVersion expectedPublishedVersion;

        private AttemptState(ImportAttempt token, GraphVersion expectedPublishedVersion) {
            this.token = token;
            this.expectedPublishedVersion = expectedPublishedVersion;
        }
    }
}
