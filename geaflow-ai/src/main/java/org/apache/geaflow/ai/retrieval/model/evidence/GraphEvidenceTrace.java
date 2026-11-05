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
import org.apache.geaflow.ai.retrieval.model.graph.GraphEdgeRef;
import org.apache.geaflow.ai.retrieval.model.graph.GraphPathRef;
import org.apache.geaflow.ai.retrieval.model.graph.GraphVertexRef;
import org.apache.geaflow.ai.retrieval.validation.ModelValidation;

/** Per-candidate graph provenance retained alongside a fused evidence item. */
public final class GraphEvidenceTrace {
    @SerializedName("queryId")
    private final String queryId;
    @SerializedName("selectedMode")
    private final RetrievalMode selectedMode;
    @SerializedName("seed")
    private final GraphVertexRef seed;
    @SerializedName("graphVersion")
    private final String graphVersion;
    @SerializedName("anchorEntityId")
    private final String anchorEntityId;
    @SerializedName("anchorMatchType")
    private final String anchorMatchType;
    @SerializedName("anchorConfidence")
    private final double anchorConfidence;
    @SerializedName("path")
    private final GraphPathRef path;
    @SerializedName("pathVertices")
    private final List<GraphVertexRef> pathVertices;
    @SerializedName("pathEdges")
    private final List<GraphEdgeRef> pathEdges;
    @SerializedName("supportingChunkIds")
    private final List<String> supportingChunkIds;

    private GraphEvidenceTrace() {
        queryId = null;
        selectedMode = null;
        seed = null;
        graphVersion = null;
        anchorEntityId = null;
        anchorMatchType = null;
        anchorConfidence = 0.0;
        path = null;
        pathVertices = Collections.emptyList();
        pathEdges = Collections.emptyList();
        supportingChunkIds = Collections.emptyList();
    }

    public GraphEvidenceTrace(String queryId, String graphVersion, String anchorEntityId,
                              String anchorMatchType, double anchorConfidence, GraphPathRef path,
                              List<String> supportingChunkIds) {
        this(queryId, null, null, anchorMatchType, anchorConfidence, graphVersion, path,
            supportingChunkIds, Collections.emptyList(), Collections.emptyList(), anchorEntityId);
    }

    public GraphEvidenceTrace(String queryId, RetrievalMode selectedMode, GraphVertexRef seed,
                              String anchorMatchType, double anchorConfidence, String graphVersion,
                              GraphPathRef path, List<String> supportingChunkIds) {
        this(queryId, selectedMode, seed, anchorMatchType, anchorConfidence, graphVersion, path,
            supportingChunkIds, Collections.emptyList(), Collections.emptyList(),
            seed == null ? null : seed.getEntityId());
    }

    public GraphEvidenceTrace(String queryId, RetrievalMode selectedMode, GraphVertexRef seed,
                              String anchorMatchType, double anchorConfidence, String graphVersion,
                              GraphPathRef path, List<String> supportingChunkIds,
                              List<GraphVertexRef> pathVertices, List<GraphEdgeRef> pathEdges) {
        this(queryId, selectedMode, seed, anchorMatchType, anchorConfidence, graphVersion, path,
            supportingChunkIds, pathVertices, pathEdges, seed == null ? null : seed.getEntityId());
    }

    private GraphEvidenceTrace(String queryId, RetrievalMode selectedMode, GraphVertexRef seed,
                               String anchorMatchType, double anchorConfidence, String graphVersion,
                               GraphPathRef path, List<String> supportingChunkIds,
                               List<GraphVertexRef> pathVertices, List<GraphEdgeRef> pathEdges,
                               String anchorEntityId) {
        this.queryId = ModelValidation.required(queryId, "queryId");
        if ((selectedMode == null) != (seed == null)) {
            throw new IllegalArgumentException("selectedMode and seed must be provided together");
        }
        this.selectedMode = selectedMode;
        this.seed = seed;
        this.graphVersion = ModelValidation.required(graphVersion, "graphVersion");
        this.anchorEntityId = ModelValidation.required(anchorEntityId, "anchorEntityId");
        this.anchorMatchType = ModelValidation.required(anchorMatchType, "anchorMatchType");
        this.anchorConfidence = ModelValidation.finite(anchorConfidence, "anchorConfidence");
        if (anchorConfidence < 0.0 || anchorConfidence > 1.0) {
            throw new IllegalArgumentException("anchorConfidence must be in [0, 1]");
        }
        this.path = Objects.requireNonNull(path, "path");
        if (path.getVertexIds().isEmpty()) {
            throw new IllegalArgumentException("path must contain a seed vertex");
        }
        if (seed == null && !anchorEntityId.equals(path.getVertexIds().get(0))) {
            throw new IllegalArgumentException("legacy path must start at anchorEntityId");
        }
        if (seed != null && (!anchorEntityId.equals(seed.getEntityId())
            || !seed.getVertexId().equals(path.getVertexIds().get(0)))) {
            throw new IllegalArgumentException("seed must identify anchor and first path vertex");
        }
        this.supportingChunkIds = ModelValidation.sortedStrings(supportingChunkIds, "supportingChunkId");
        this.pathVertices = ModelValidation.immutableList(pathVertices, "pathVertices");
        this.pathEdges = ModelValidation.immutableList(pathEdges, "pathEdges");
        if ((!this.pathVertices.isEmpty() || !this.pathEdges.isEmpty())
            && (this.pathVertices.size() != path.getHop() + 1
            || this.pathEdges.size() != path.getHop())) {
            throw new IllegalArgumentException("path references must match path hop");
        }
        for (int index = 0; index < this.pathVertices.size(); index++) {
            if (!this.pathVertices.get(index).getVertexId().equals(path.getVertexIds().get(index))) {
                throw new IllegalArgumentException("path vertex reference does not match ordered path");
            }
        }
        if (seed != null && !this.pathVertices.isEmpty()
            && !seed.sameIdentityAs(this.pathVertices.get(0))) {
            throw new IllegalArgumentException("seed reference does not match first path vertex");
        }
        for (int index = 0; index < this.pathEdges.size(); index++) {
            GraphEdgeRef edge = this.pathEdges.get(index);
            if (!edge.getEdgeId().equals(path.getEdgeIds().get(index))) {
                throw new IllegalArgumentException("path edge reference does not match ordered path");
            }
            if (!this.pathVertices.isEmpty()) {
                String source = this.pathVertices.get(index).getEntityId();
                String target = this.pathVertices.get(index + 1).getEntityId();
                if (!(source.equals(edge.getSourceEntityId()) && target.equals(edge.getTargetEntityId()))
                    && !(source.equals(edge.getTargetEntityId()) && target.equals(edge.getSourceEntityId()))) {
                    throw new IllegalArgumentException("path edge does not connect adjacent vertices");
                }
            }
        }
    }

    public String getQueryId() {
        return queryId;
    }

    public RetrievalMode getSelectedMode() {
        return selectedMode;
    }

    public GraphVertexRef getSeed() {
        return seed;
    }

    public String getGraphVersion() {
        return graphVersion;
    }

    public String getAnchorEntityId() {
        return anchorEntityId;
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

    public List<GraphVertexRef> getPathVertices() {
        return Collections.unmodifiableList(pathVertices == null
            ? Collections.emptyList() : pathVertices);
    }

    public List<GraphEdgeRef> getPathEdges() {
        return Collections.unmodifiableList(pathEdges == null
            ? Collections.emptyList() : pathEdges);
    }

    public List<String> getSupportingChunkIds() {
        return Collections.unmodifiableList(supportingChunkIds);
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
            && Objects.equals(seed, that.seed)
            && Objects.equals(graphVersion, that.graphVersion)
            && Objects.equals(anchorEntityId, that.anchorEntityId)
            && Objects.equals(anchorMatchType, that.anchorMatchType)
            && Objects.equals(path, that.path)
            && Objects.equals(pathVertices, that.pathVertices)
            && Objects.equals(pathEdges, that.pathEdges)
            && Objects.equals(supportingChunkIds, that.supportingChunkIds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(queryId, selectedMode, seed, graphVersion, anchorEntityId,
            anchorMatchType, anchorConfidence, path, pathVertices, pathEdges, supportingChunkIds);
    }
}
