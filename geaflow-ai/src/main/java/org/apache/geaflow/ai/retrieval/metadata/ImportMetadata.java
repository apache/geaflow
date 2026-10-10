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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.geaflow.ai.retrieval.validation.ModelValidation;

/** Immutable attempt snapshot; timestamps are UTC epoch milliseconds. */
public final class ImportMetadata {

    private final DatasetManifest manifest;
    private final String importerVersion;
    private final GraphBuildMetadata graph;
    private final List<IndexBuildMetadata> indexes;
    private final QualityCounters counters;
    private final ImportState state;
    private final String verifiedSha256;
    private final MetadataException.Code failureCode;
    private final String failureReason;
    private final long startedAt;
    private final long updatedAt;

    public ImportMetadata(DatasetManifest manifest, String importerVersion, GraphBuildMetadata graph,
                          List<IndexBuildMetadata> indexes, QualityCounters counters, ImportState state,
                          String verifiedSha256, MetadataException.Code failureCode, String failureReason,
                          long startedAt, long updatedAt) {
        this.manifest = Objects.requireNonNull(manifest, "manifest");
        this.importerVersion = MetadataValidation.required(importerVersion, "importerVersion");
        this.graph = Objects.requireNonNull(graph, "graph");
        this.indexes = ModelValidation.immutableList(indexes, "indexes");
        this.counters = Objects.requireNonNull(counters, "counters");
        this.state = Objects.requireNonNull(state, "state");
        this.verifiedSha256 = verifiedSha256 == null ? null : MetadataValidation.checksum(verifiedSha256);
        this.failureCode = failureCode;
        this.failureReason = failureReason;
        this.startedAt = MetadataValidation.nonNegative(startedAt, "startedAt");
        this.updatedAt = MetadataValidation.nonNegative(updatedAt, "updatedAt");
        if (updatedAt < startedAt) {
            throw MetadataValidation.invalid("updatedAt precedes startedAt");
        }
        if (this.verifiedSha256 != null && !manifest.getSha256().equals(this.verifiedSha256)) {
            throw MetadataValidation.invalid("verifiedSha256 does not match manifest sha256");
        }
        if (state == ImportState.FAILED) {
            Objects.requireNonNull(failureCode, "failureCode");
            MetadataValidation.required(failureReason, "failureReason");
        } else if (failureCode != null || failureReason != null) {
            throw MetadataValidation.invalid("failure is only allowed in FAILED");
        }
        if (this.indexes.stream().map(index -> index.getIndexVersion().getIndexName()).distinct().count()
            != this.indexes.size()) {
            throw MetadataValidation.invalid("duplicate index names");
        }
        for (IndexBuildMetadata index : this.indexes) {
            if (!graph.getGraphVersion().equals(index.getGraphVersion())) {
                throw MetadataValidation.invalid("index belongs to another graph");
            }
        }
        if (state == ImportState.IMPORTING && graph.isReady()) {
            throw MetadataValidation.invalid("IMPORTING graph must not be ready");
        }
        if (state == ImportState.INDEXING || state == ImportState.READY) {
            if (!graph.isReady() || !manifest.getSha256().equals(this.verifiedSha256)) {
                throw new MetadataException(MetadataException.Code.NOT_READY,
                    "verified source and completed graph required");
            }
        }
        if (state == ImportState.READY && !readinessReasons().isEmpty()) {
            throw new MetadataException(MetadataException.Code.NOT_READY, readinessReasons().toString());
        }
    }

    public List<String> readinessReasons() {
        List<String> reasons = new ArrayList<>();
        if (state == ImportState.FAILED) {
            reasons.add("FAILED: " + failureReason);
        }
        if (state == ImportState.IMPORTING) {
            reasons.add("STATE_NOT_READY: IMPORTING");
        }
        if (!manifest.getSha256().equals(verifiedSha256)) {
            reasons.add("CHECKSUM_NOT_VERIFIED");
        }
        if (!graph.isReady()) {
            reasons.add("GRAPH_NOT_READY");
        }
        for (String name : graph.getRequiredIndexes()) {
            if (indexes.stream().noneMatch(index -> name.equals(index.getIndexVersion().getIndexName()) && index.isReady())) {
                reasons.add("INDEX_NOT_READY: " + name);
            }
        }
        return Collections.unmodifiableList(reasons);
    }

    public DatasetManifest getManifest() {
        return manifest;
    }

    public String getImporterVersion() {
        return importerVersion;
    }

    public GraphBuildMetadata getGraph() {
        return graph;
    }

    public List<IndexBuildMetadata> getIndexes() {
        return indexes;
    }

    public QualityCounters getCounters() {
        return counters;
    }

    public ImportState getState() {
        return state;
    }

    public String getVerifiedSha256() {
        return verifiedSha256;
    }

    public MetadataException.Code getFailureCode() {
        return failureCode;
    }

    public String getFailureReason() {
        return failureReason;
    }

    public long getStartedAt() {
        return startedAt;
    }

    public long getUpdatedAt() {
        return updatedAt;
    }

    public GraphBuildMetadata getGraphMetadata() {
        return graph;
    }

    public GraphBuildMetadata getGraphBuildMetadata() {
        return graph;
    }

    public org.apache.geaflow.ai.retrieval.model.version.GraphVersion getGraphVersion() {
        return graph.getGraphVersion();
    }

    public List<String> getReadinessReasons() {
        return readinessReasons();
    }

    public String getReadinessReason() {
        List<String> reasons = readinessReasons();
        return reasons.isEmpty() ? null : String.join(", ", reasons);
    }

    public String getChecksum() {
        return verifiedSha256;
    }

    public long getCreatedAt() {
        return startedAt;
    }

    public long getLastUpdatedAt() {
        return updatedAt;
    }

    public boolean isReady() {
        return state == ImportState.READY && readinessReasons().isEmpty();
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof ImportMetadata)) {
            return false;
        }
        ImportMetadata that = (ImportMetadata) object;
        return startedAt == that.startedAt
            && updatedAt == that.updatedAt
            && Objects.equals(manifest, that.manifest)
            && Objects.equals(importerVersion, that.importerVersion)
            && Objects.equals(graph, that.graph)
            && Objects.equals(indexes, that.indexes)
            && Objects.equals(counters, that.counters)
            && state == that.state
            && Objects.equals(verifiedSha256, that.verifiedSha256)
            && failureCode == that.failureCode
            && Objects.equals(failureReason, that.failureReason);
    }

    @Override
    public int hashCode() {
        return Objects.hash(manifest, importerVersion, graph, indexes, counters, state,
            verifiedSha256, failureCode, failureReason, startedAt, updatedAt);
    }
}
