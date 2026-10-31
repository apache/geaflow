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

import java.util.Objects;
import org.apache.geaflow.ai.retrieval.model.version.GraphVersion;
import org.apache.geaflow.ai.retrieval.model.version.IndexVersion;

/** Index artifact bound to a complete graph identity. */
public final class IndexBuildMetadata {

    private final GraphVersion graphVersion;
    private final IndexVersion indexVersion;
    private final String indexType;
    private final String builderVersion;
    private final String artifactUri;
    private final boolean ready;

    public IndexBuildMetadata(
        GraphVersion graphVersion,
        IndexVersion indexVersion,
        String indexType,
        String builderVersion,
        String artifactUri,
        boolean ready) {
        Objects.requireNonNull(graphVersion, "graphVersion");
        Objects.requireNonNull(indexVersion, "indexVersion");
        if (!graphVersion.getVersion().equals(indexVersion.getGraphVersion())) {
            throw MetadataValidation.invalid("index source graph version mismatch");
        }
        MetadataValidation.required(indexType, "indexType");
        MetadataValidation.required(builderVersion, "builderVersion");
        if (ready) {
            MetadataValidation.required(artifactUri, "artifactUri");
        }
        this.graphVersion = graphVersion;
        this.indexVersion = indexVersion;
        this.indexType = indexType;
        this.builderVersion = builderVersion;
        this.artifactUri = artifactUri;
        this.ready = ready;
    }

    public GraphVersion getGraphVersion() {
        return graphVersion;
    }

    public IndexVersion getIndexVersion() {
        return indexVersion;
    }

    public String getIndexType() {
        return indexType;
    }

    public String getBuilderVersion() {
        return builderVersion;
    }

    public String getBuildVersion() {
        return builderVersion;
    }

    public String getArtifactUri() {
        return artifactUri;
    }

    /** Alias for callers that use the shorter artifact terminology. */
    public String getType() {
        return indexType;
    }

    public boolean isReady() {
        return ready;
    }

    public boolean getReady() {
        return ready;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof IndexBuildMetadata)) {
            return false;
        }
        IndexBuildMetadata that = (IndexBuildMetadata) object;
        return ready == that.ready
            && Objects.equals(graphVersion, that.graphVersion)
            && Objects.equals(indexVersion, that.indexVersion)
            && Objects.equals(indexType, that.indexType)
            && Objects.equals(builderVersion, that.builderVersion)
            && Objects.equals(artifactUri, that.artifactUri);
    }

    @Override
    public int hashCode() {
        return Objects.hash(graphVersion, indexVersion, indexType, builderVersion, artifactUri, ready);
    }
}
