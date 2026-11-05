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

package org.apache.geaflow.ai.retrieval.model.evidence;

import com.google.gson.annotations.SerializedName;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalMode;
import org.apache.geaflow.ai.retrieval.model.graph.GraphPathRef;
import org.apache.geaflow.ai.retrieval.model.graph.GraphVertexRef;
import org.apache.geaflow.ai.retrieval.validation.ModelValidation;

/** Per-candidate graph provenance retained alongside a fused evidence item. */
public final class GraphEvidenceTrace {
    @SerializedName("queryId")
    private final String queryId;
    @SerializedName("selectedMode")
    private final RetrievalMode selectedMode;
    @SerializedName("graphVersion")
    private final String graphVersion;
    @SerializedName("anchorMatchType")
    private final String anchorMatchType;
    @SerializedName("anchorConfidence")
    private final double anchorConfidence;
    @SerializedName("path")
    private final GraphPathRef path;
    @SerializedName("supportingChunkIds")
    private final List<String> supportingChunkIds;

    private GraphEvidenceTrace() {
        queryId = null;
        selectedMode = null;
        graphVersion = null;
        anchorMatchType = null;
        anchorConfidence = 0.0D;
        path = null;
        supportingChunkIds = Collections.emptyList();
    }

    public GraphEvidenceTrace(String queryId, RetrievalMode selectedMode, String anchorMatchType,
                              double anchorConfidence, String graphVersion, GraphPathRef path,
                              List<String> supportingChunkIds) {
        this.queryId = ModelValidation.required(queryId, "queryId");
        this.selectedMode = Objects.requireNonNull(selectedMode, "selectedMode");
        this.graphVersion = ModelValidation.required(graphVersion, "graphVersion");
        this.anchorMatchType = ModelValidation.required(anchorMatchType, "anchorMatchType");
        this.anchorConfidence = ModelValidation.finite(anchorConfidence, "anchorConfidence");
        if (anchorConfidence < 0.0D || anchorConfidence > 1.0D) {
            throw new IllegalArgumentException("anchorConfidence must be in [0, 1]");
        }
        this.path = Objects.requireNonNull(path, "path");
        if (path.getVertices().isEmpty()) {
            throw new IllegalArgumentException("path must contain an anchor vertex");
        }
        this.supportingChunkIds = ModelValidation.sortedStrings(supportingChunkIds,
            "supportingChunkId");
    }

    public String getQueryId() {
        return queryId;
    }

    public RetrievalMode getSelectedMode() {
        return selectedMode;
    }

    /** The seed is the first vertex in the canonical path. */
    public GraphVertexRef getSeed() {
        return path == null || path.getVertices().isEmpty() ? null : path.getVertices().get(0);
    }

    public String getGraphVersion() {
        return graphVersion;
    }

    /** The anchor identity is derived from the first path vertex. */
    public String getAnchorEntityId() {
        GraphVertexRef seed = getSeed();
        return seed == null ? null : seed.getEntityId();
    }

    public String getAnchorMatchType() {
        return anchorMatchType;
    }

    public double getAnchorConfidence() {
        return anchorConfidence;
    }

    public GraphPathRef getPath() {
        return path;
    }

    public List<String> getSupportingChunkIds() {
        return Collections.unmodifiableList(supportingChunkIds == null
            ? Collections.emptyList() : supportingChunkIds);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof GraphEvidenceTrace)) {
            return false;
        }
        GraphEvidenceTrace that = (GraphEvidenceTrace) object;
        return Double.compare(anchorConfidence, that.anchorConfidence) == 0
            && Objects.equals(queryId, that.queryId)
            && selectedMode == that.selectedMode
            && Objects.equals(graphVersion, that.graphVersion)
            && Objects.equals(anchorMatchType, that.anchorMatchType)
            && Objects.equals(path, that.path)
            && Objects.equals(supportingChunkIds, that.supportingChunkIds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(queryId, selectedMode, graphVersion, anchorMatchType,
            anchorConfidence, path, supportingChunkIds);
    }
}
