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

package org.apache.geaflow.ai.retrieval.metadata;

import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.geaflow.ai.retrieval.ingest.ImportStateMachine;
import org.apache.geaflow.ai.retrieval.model.version.GraphVersion;

/** Single-process reference implementation with atomic snapshot publication. */
public final class InMemoryMetadataStore implements MetadataStore {

    private final Map<GraphVersion, ImportMetadata> attempts = new LinkedHashMap<>();
    private final Map<String, ImportMetadata> published = new LinkedHashMap<>();
    private final Clock clock;

    public InMemoryMetadataStore() {
        this(Clock.systemUTC());
    }

    public InMemoryMetadataStore(Clock clock) {
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public synchronized ImportMetadata begin(DatasetManifest manifest, String importerVersion, GraphBuildMetadata graph) {
        Objects.requireNonNull(manifest, "manifest");
        MetadataValidation.required(importerVersion, "importerVersion");
        Objects.requireNonNull(graph, "graph");
        GraphVersion version = graph.getGraphVersion();
        if (attempts.containsKey(version)) {
            throw new MetadataException(MetadataException.Code.VERSION_CONFLICT, "attempt already exists");
        }
        if (graph.isReady()) {
            throw MetadataValidation.invalid("new attempt cannot have a ready graph");
        }
        long now = clock.millis();
        ImportMetadata value = new ImportMetadata(manifest, importerVersion, graph, Collections.emptyList(),
            new QualityCounters(0, 0, 0, 0, 0, 0), ImportState.IMPORTING, null, null, null, now, now);
        attempts.put(version, value);
        return value;
    }

    @Override
    public synchronized ImportMetadata retry(GraphVersion failedVersion, GraphBuildMetadata newGraph) {
        Objects.requireNonNull(failedVersion, "failedVersion");
        Objects.requireNonNull(newGraph, "newGraph");
        ImportMetadata old = require(failedVersion);
        if (old.getState() != ImportState.FAILED
            || !failedVersion.getGraphName().equals(newGraph.getGraphVersion().getGraphName())) {
            throw new MetadataException(MetadataException.Code.INVALID_TRANSITION,
                "retry requires a failed attempt of the same graph");
        }
        return begin(old.getManifest(), old.getImporterVersion(), newGraph);
    }

    @Override
    public synchronized ImportMetadata verifySource(GraphVersion version, InputStream source) throws IOException {
        Objects.requireNonNull(source, "source");
        final ImportMetadata old = requireState(version, ImportState.IMPORTING);
        String hash = digest(source);
        if (!old.getManifest().getSha256().equals(hash)) {
            fail(version, MetadataException.Code.CHECKSUM_MISMATCH, "source SHA-256 differs from manifest");
            throw new MetadataException(MetadataException.Code.CHECKSUM_MISMATCH, "source SHA-256 differs from manifest");
        }
        return save(old, old.getGraph(), old.getIndexes(), old.getCounters(), old.getState(), hash, null, null);
    }

    @Override
    public synchronized ImportMetadata finishImport(GraphVersion version, GraphBuildMetadata graph, QualityCounters counters) {
        Objects.requireNonNull(graph, "graph");
        Objects.requireNonNull(counters, "counters");
        ImportMetadata old = requireState(version, ImportState.IMPORTING);
        if (!version.equals(graph.getGraphVersion())
            || !old.getGraph().getRequiredIndexes().equals(graph.getRequiredIndexes())) {
            throw MetadataValidation.invalid("graph identity and required indexes cannot change within an attempt");
        }
        if (!graph.isReady() || old.getVerifiedSha256() == null) {
            throw new MetadataException(MetadataException.Code.NOT_READY, "verified source and completed graph required");
        }
        ImportStateMachine.validate(old.getState(), ImportState.INDEXING);
        return save(old, graph, old.getIndexes(), counters, ImportState.INDEXING, old.getVerifiedSha256(), null, null);
    }

    @Override
    public synchronized ImportMetadata putIndex(GraphVersion version, IndexBuildMetadata index) {
        Objects.requireNonNull(index, "index");
        ImportMetadata old = requireState(version, ImportState.INDEXING);
        if (!version.equals(index.getGraphVersion())) {
            throw MetadataValidation.invalid("index belongs to another graph version");
        }
        List<IndexBuildMetadata> indexes = new ArrayList<>(old.getIndexes());
        for (IndexBuildMetadata existing : indexes) {
            if (existing.getIndexVersion().getIndexName().equals(index.getIndexVersion().getIndexName())
                && existing.isReady()) {
                throw new MetadataException(MetadataException.Code.VERSION_CONFLICT,
                    "completed index is immutable");
            }
        }
        indexes.removeIf(existing -> existing.getIndexVersion().getIndexName()
            .equals(index.getIndexVersion().getIndexName()));
        indexes.add(index);
        return save(old, old.getGraph(), indexes, old.getCounters(), old.getState(),
            old.getVerifiedSha256(), null, null);
    }

    @Override
    public synchronized ImportMetadata fail(GraphVersion version, MetadataException.Code code, String reason) {
        Objects.requireNonNull(code, "code");
        MetadataValidation.required(reason, "failureReason");
        ImportMetadata old = require(version);
        ImportStateMachine.validate(old.getState(), ImportState.FAILED);
        return save(old, old.getGraph(), old.getIndexes(), old.getCounters(), ImportState.FAILED,
            old.getVerifiedSha256(), code, reason);
    }

    @Override
    public synchronized ImportMetadata publish(GraphVersion version, GraphVersion expectedPublishedVersion) {
        ImportMetadata old = requireState(version, ImportState.INDEXING);
        ImportMetadata current = published.get(version.getGraphName());
        GraphVersion actual = current == null ? null : current.getGraph().getGraphVersion();
        if (!Objects.equals(expectedPublishedVersion, actual)) {
            throw new MetadataException(MetadataException.Code.VERSION_CONFLICT, "published version changed");
        }
        if (!old.readinessReasons().isEmpty()) {
            throw new MetadataException(MetadataException.Code.NOT_READY,
                old.readinessReasons().toString());
        }
        ImportStateMachine.validate(old.getState(), ImportState.READY);
        ImportMetadata ready = save(old, old.getGraph(), old.getIndexes(), old.getCounters(),
            ImportState.READY, old.getVerifiedSha256(), null, null);
        published.put(version.getGraphName(), ready);
        return ready;
    }

    @Override
    public synchronized Optional<ImportMetadata> find(GraphVersion version) {
        return Optional.ofNullable(attempts.get(version));
    }

    @Override
    public synchronized Optional<ImportMetadata> published(String graphName) {
        return Optional.ofNullable(published.get(graphName));
    }

    @Override
    public synchronized List<ImportMetadata> findDataset(String dataset, String release, String split) {
        return Collections.unmodifiableList(attempts.values().stream().filter(value ->
            value.getManifest().getDataset().equals(dataset)
                && value.getManifest().getDatasetRelease().equals(release)
                && value.getManifest().getSplit().equals(split)).collect(Collectors.toList()));
    }

    /** Returns the published version pointer without exposing mutable storage state. */
    public synchronized Optional<GraphVersion> publishedVersion(String graphName) {
        ImportMetadata value = published.get(graphName);
        return value == null ? Optional.empty() : Optional.of(value.getGraphVersion());
    }

    /** Alias for callers that use a get-style publication query. */
    public synchronized Optional<ImportMetadata> getPublished(String graphName) {
        return published(graphName);
    }

    /** Alias for callers that use a find-style publication query. */
    public synchronized Optional<ImportMetadata> findPublished(String graphName) {
        return published(graphName);
    }

    private ImportMetadata require(GraphVersion version) {
        ImportMetadata value = attempts.get(version);
        if (value == null) {
            throw new MetadataException(MetadataException.Code.NOT_FOUND, "unknown graph version");
        }
        return value;
    }

    private ImportMetadata requireState(GraphVersion version, ImportState state) {
        ImportMetadata value = require(version);
        if (value.getState() != state) {
            throw new MetadataException(MetadataException.Code.INVALID_TRANSITION,
                "expected " + state + " but was " + value.getState());
        }
        return value;
    }

    private ImportMetadata save(ImportMetadata old, GraphBuildMetadata graph,
                                List<IndexBuildMetadata> indexes, QualityCounters counters,
                                ImportState state, String checksum, MetadataException.Code code,
                                String reason) {
        ImportMetadata value = new ImportMetadata(old.getManifest(), old.getImporterVersion(), graph, indexes,
            counters, state, checksum, code, reason, old.getStartedAt(),
            Math.max(old.getUpdatedAt(), clock.millis()));
        attempts.put(graph.getGraphVersion(), value);
        return value;
    }

    private static String digest(InputStream source) throws IOException {
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
        byte[] buffer = new byte[8192];
        int length;
        while ((length = source.read(buffer)) != -1) {
            digest.update(buffer, 0, length);
        }
        StringBuilder hash = new StringBuilder(64);
        for (byte value : digest.digest()) {
            hash.append(Character.forDigit((value & 0xff) >>> 4, 16));
            hash.append(Character.forDigit(value & 0xf, 16));
        }
        return hash.toString();
    }
}
