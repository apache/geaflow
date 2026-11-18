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

package org.apache.geaflow.ai.retrieval.model.graph;

import com.google.gson.annotations.SerializedName;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.geaflow.ai.retrieval.validation.ModelValidation;
import org.apache.geaflow.ai.retrieval.validation.RetrievalModelValidationException;

/** Immutable ordered graph path with complete vertex and edge provenance. */
public final class GraphPathRef {

    @SerializedName("vertices")
    private final List<GraphVertexRef> vertices;
    @SerializedName("edges")
    private final List<GraphEdgeRef> edges;
    @SerializedName("sampled")
    private final boolean sampled;

    private GraphPathRef() {
        vertices = Collections.emptyList();
        edges = Collections.emptyList();
        sampled = false;
    }

    public GraphPathRef(List<GraphVertexRef> vertices, List<GraphEdgeRef> edges, boolean sampled) {
        this.vertices = ModelValidation.immutableList(vertices, "vertices");
        this.edges = ModelValidation.immutableList(edges, "edges");
        this.sampled = sampled;
        if (this.vertices.isEmpty() || this.vertices.size() != this.edges.size() + 1) {
            throw new RetrievalModelValidationException("path vertex/edge counts must match");
        }
        for (int index = 0; index < this.edges.size(); index++) {
            GraphEdgeRef edge = this.edges.get(index);
            String source = this.vertices.get(index).getEntityId();
            String target = this.vertices.get(index + 1).getEntityId();
            if (!(source.equals(edge.getSourceEntityId()) && target.equals(edge.getTargetEntityId()))
                && !(source.equals(edge.getTargetEntityId()) && target.equals(edge.getSourceEntityId()))) {
                throw new RetrievalModelValidationException(
                    "path edge does not connect adjacent vertices");
            }
        }
    }

    public List<GraphVertexRef> getVertices() {
        return Collections.unmodifiableList(vertices == null ? Collections.emptyList() : vertices);
    }

    public List<GraphEdgeRef> getEdges() {
        return Collections.unmodifiableList(edges == null ? Collections.emptyList() : edges);
    }

    /** Returns vertex identifiers derived from the complete vertex references. */
    public List<String> getVertexIds() {
        List<String> ids = new ArrayList<>(getVertices().size());
        for (GraphVertexRef vertex : getVertices()) {
            ids.add(vertex.getVertexId());
        }
        return Collections.unmodifiableList(ids);
    }

    /** Returns edge identifiers derived from the complete edge references. */
    public List<String> getEdgeIds() {
        List<String> ids = new ArrayList<>(getEdges().size());
        for (GraphEdgeRef edge : getEdges()) {
            ids.add(edge.getEdgeId());
        }
        return Collections.unmodifiableList(ids);
    }

    /** Returns the hop count derived from the ordered edges. */
    public int getHop() {
        return getEdges().size();
    }

    public boolean isSampled() {
        return sampled;
    }

    /** Path identity ignores repeated descriptive provenance while preserving order and sampling. */
    public boolean sameIdentityAs(GraphPathRef other) {
        return other != null && sampled == other.sampled
            && getVertexIds().equals(other.getVertexIds())
            && getEdgeIds().equals(other.getEdgeIds());
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof GraphPathRef)) {
            return false;
        }
        GraphPathRef that = (GraphPathRef) object;
        return sampled == that.sampled && Objects.equals(vertices, that.vertices)
            && Objects.equals(edges, that.edges);
    }

    @Override
    public int hashCode() {
        return Objects.hash(vertices, edges, sampled);
    }
}
