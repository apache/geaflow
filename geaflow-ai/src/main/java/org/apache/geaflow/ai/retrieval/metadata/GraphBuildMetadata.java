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

import java.util.List;
import java.util.Objects;
import org.apache.geaflow.ai.retrieval.model.version.GraphVersion;
import org.apache.geaflow.ai.retrieval.validation.ModelValidation;

/** Graph artifact and the exact required index names for its publication. */
public final class GraphBuildMetadata {

    private final GraphVersion graphVersion;
    private final String artifactUri;
    private final String graphType;
    private final String builderVersion;
    private final boolean ready;
    private final List<String> requiredIndexes;
    private final GraphVersion parentGraphVersion;
    private final String snapshotUri;
    private final String deltaUri;
    private final String tombstoneUri;
    private final String deltaDigest;
    private final String statisticsVersion;
    /** Versioned association-score components; null for artifacts published before them. */
    private AssociationScoreMetadata associationScoreMetadata;

    /** Creates graph metadata with the artifact details used by the current build. */
    public GraphBuildMetadata(
        GraphVersion graphVersion,
        String artifactUri,
        String graphType,
        String builderVersion,
        boolean ready,
        List<String> requiredIndexes) {
        this.graphVersion = Objects.requireNonNull(graphVersion, "graphVersion");
        this.artifactUri = artifactUri;
        this.graphType = MetadataValidation.required(graphType, "graphType");
        this.builderVersion = MetadataValidation.required(builderVersion, "builderVersion");
        if (ready) {
            MetadataValidation.required(artifactUri, "artifactUri");
        }
        List<String> names = ModelValidation.sortedStrings(requiredIndexes, "requiredIndexes");
        if (names.stream().distinct().count() != names.size()) {
            throw MetadataValidation.invalid("requiredIndexes must be unique");
        }
        this.ready = ready;
        this.requiredIndexes = names;
        this.parentGraphVersion = null;
        this.snapshotUri = artifactUri;
        this.deltaUri = null;
        this.tombstoneUri = null;
        this.deltaDigest = null;
        this.statisticsVersion = null;
    }

    /** Creates metadata for a snapshot plus optional immutable delta overlay. */
    public GraphBuildMetadata(
        GraphVersion graphVersion,
        GraphVersion parentGraphVersion,
        String snapshotUri,
        String deltaUri,
        String tombstoneUri,
        String deltaDigest,
        String statisticsVersion,
        String graphType,
        String builderVersion,
        boolean ready,
        List<String> requiredIndexes) {
        this(graphVersion, snapshotUri == null ? deltaUri : snapshotUri, graphType, builderVersion, ready,
            requiredIndexes, parentGraphVersion, snapshotUri, deltaUri, tombstoneUri, deltaDigest,
            statisticsVersion);
    }

    private GraphBuildMetadata(
        GraphVersion graphVersion,
        String artifactUri,
        String graphType,
        String builderVersion,
        boolean ready,
        List<String> requiredIndexes,
        GraphVersion parentGraphVersion,
        String snapshotUri,
        String deltaUri,
        String tombstoneUri,
        String deltaDigest,
        String statisticsVersion) {
        this.graphVersion = Objects.requireNonNull(graphVersion, "graphVersion");
        this.artifactUri = artifactUri;
        this.graphType = MetadataValidation.required(graphType, "graphType");
        this.builderVersion = MetadataValidation.required(builderVersion, "builderVersion");
        if (ready && artifactUri == null && snapshotUri == null && deltaUri == null) {
            MetadataValidation.required(artifactUri, "artifactUri");
        }
        if (parentGraphVersion != null) {
            if (!graphVersion.getGraphName().equals(parentGraphVersion.getGraphName())
                || graphVersion.equals(parentGraphVersion)) {
                throw MetadataValidation.invalid("parent graph version must be a different version of the same graph");
            }
        }
        boolean overlay = deltaUri != null || tombstoneUri != null;
        if (overlay && parentGraphVersion == null) {
            throw MetadataValidation.invalid("delta metadata requires parentGraphVersion");
        }
        if (ready && overlay) {
            MetadataValidation.required(deltaDigest, "deltaDigest");
            MetadataValidation.required(statisticsVersion, "statisticsVersion");
        }
        this.ready = ready;
        this.requiredIndexes = ModelValidation.sortedStrings(requiredIndexes, "requiredIndexes");
        if (this.requiredIndexes.stream().distinct().count() != this.requiredIndexes.size()) {
            throw MetadataValidation.invalid("requiredIndexes must be unique");
        }
        this.parentGraphVersion = parentGraphVersion;
        this.snapshotUri = snapshotUri;
        this.deltaUri = deltaUri;
        this.tombstoneUri = tombstoneUri;
        this.deltaDigest = deltaDigest == null ? null : MetadataValidation.required(deltaDigest, "deltaDigest");
        this.statisticsVersion = statisticsVersion == null
            ? null : MetadataValidation.required(statisticsVersion, "statisticsVersion");
    }

    /** Convenience constructor using the standard graph type and builder defaults. */
    public GraphBuildMetadata(
        GraphVersion graphVersion,
        GraphVersion parentGraphVersion,
        String snapshotUri,
        String deltaUri,
        String tombstoneUri,
        String deltaDigest,
        String statisticsVersion,
        boolean ready,
        List<String> requiredIndexes) {
        this(graphVersion, parentGraphVersion, snapshotUri, deltaUri, tombstoneUri, deltaDigest,
            statisticsVersion, "graph", "unknown", ready, requiredIndexes);
    }

    /** Creates graph metadata while retaining the original four-argument API. */
    public GraphBuildMetadata(
        GraphVersion graphVersion,
        String artifactUri,
        boolean ready,
        List<String> requiredIndexes) {
        this(graphVersion, artifactUri, "graph", "unknown", ready, requiredIndexes);
    }

    /** Creates graph metadata without required indexes for a graph-only build. */
    public GraphBuildMetadata(
        GraphVersion graphVersion,
        String artifactUri,
        String graphType,
        String builderVersion,
        boolean ready) {
        this(graphVersion, artifactUri, graphType, builderVersion, ready, java.util.Collections.emptyList());
    }

    /** Creates graph metadata with the list before the readiness flag. */
    public GraphBuildMetadata(
        GraphVersion graphVersion,
        String artifactUri,
        String graphType,
        String builderVersion,
        List<String> requiredIndexes,
        boolean ready) {
        this(graphVersion, artifactUri, graphType, builderVersion, ready, requiredIndexes);
    }

    public GraphVersion getGraphVersion() {
        return graphVersion;
    }

    public GraphVersion getVersion() {
        return graphVersion;
    }

    public String getArtifactUri() {
        return artifactUri;
    }

    public String getGraphType() {
        return graphType;
    }

    /** Alias for consumers that use a generic artifact type name. */
    public String getType() {
        return graphType;
    }

    public String getBuilderVersion() {
        return builderVersion;
    }

    public String getBuildVersion() {
        return builderVersion;
    }

    public boolean isReady() {
        return ready;
    }

    public boolean getReady() {
        return ready;
    }

    public List<String> getRequiredIndexes() {
        return requiredIndexes;
    }

    /** Alias that makes the publication role explicit at call sites. */
    public List<String> getRequiredIndexNames() {
        return requiredIndexes;
    }

    public GraphVersion getParentGraphVersion() {
        return parentGraphVersion;
    }

    public GraphVersion getParentVersion() {
        return parentGraphVersion;
    }

    public String getSnapshotUri() {
        return snapshotUri;
    }

    public String getDeltaUri() {
        return deltaUri;
    }

    public String getTombstoneUri() {
        return tombstoneUri;
    }

    public String getDeltaDigest() {
        return deltaDigest;
    }

    public String getStatisticsVersion() {
        return statisticsVersion;
    }

    /**
     * Returns the versioned association-score components, or null when the artifact predates them.
     *
     * <p>The design requires the components and their versions in build metadata so a published
     * graph is scored identically on every replay.
     */
    public AssociationScoreMetadata getAssociationScoreMetadata() {
        return associationScoreMetadata;
    }

    /** Attaches the association-score components recorded by the graph builder. */
    public GraphBuildMetadata withAssociationScoreMetadata(AssociationScoreMetadata value) {
        this.associationScoreMetadata = value;
        return this;
    }

    public boolean isDelta() {
        return parentGraphVersion != null || deltaUri != null || tombstoneUri != null;
    }

    public boolean isSnapshot() {
        return !isDelta();
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof GraphBuildMetadata)) {
            return false;
        }
        GraphBuildMetadata that = (GraphBuildMetadata) object;
        return ready == that.ready
            && Objects.equals(graphVersion, that.graphVersion)
            && Objects.equals(artifactUri, that.artifactUri)
            && Objects.equals(graphType, that.graphType)
            && Objects.equals(builderVersion, that.builderVersion)
            && Objects.equals(requiredIndexes, that.requiredIndexes)
            && Objects.equals(parentGraphVersion, that.parentGraphVersion)
            && Objects.equals(snapshotUri, that.snapshotUri)
            && Objects.equals(deltaUri, that.deltaUri)
            && Objects.equals(tombstoneUri, that.tombstoneUri)
            && Objects.equals(deltaDigest, that.deltaDigest)
            && Objects.equals(statisticsVersion, that.statisticsVersion);
    }

    @Override
    public int hashCode() {
        return Objects.hash(graphVersion, artifactUri, graphType, builderVersion, ready, requiredIndexes,
            parentGraphVersion, snapshotUri, deltaUri, tombstoneUri, deltaDigest, statisticsVersion);
    }
}
