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
            && Objects.equals(requiredIndexes, that.requiredIndexes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(graphVersion, artifactUri, graphType, builderVersion, ready, requiredIndexes);
    }
}
