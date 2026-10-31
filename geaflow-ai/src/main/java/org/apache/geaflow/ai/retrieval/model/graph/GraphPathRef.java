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
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.geaflow.ai.retrieval.validation.ModelValidation;
import org.apache.geaflow.ai.retrieval.validation.RetrievalModelValidationException;

/** Immutable ordered graph path with parallel vertex and edge identifier sequences. */
public final class GraphPathRef {

    @SerializedName("vertexIds")
    private final List<String> vertexIds;
    @SerializedName("edgeIds")
    private final List<String> edgeIds;
    @SerializedName("hop")
    private final int hop;
    @SerializedName("sampled")
    private final boolean sampled;

    private GraphPathRef() {
        this.vertexIds = Collections.emptyList();
        this.edgeIds = Collections.emptyList();
        this.hop = 0;
        this.sampled = false;
    }

    public GraphPathRef(List<String> vertexIds, List<String> edgeIds, int hop, boolean sampled) {
        this.vertexIds = ModelValidation.immutableList(vertexIds, "vertexIds");
        this.edgeIds = ModelValidation.immutableList(edgeIds, "edgeIds");
        this.hop = ModelValidation.nonNegative(hop, "hop");
        this.sampled = sampled;
        if (this.vertexIds.size() != hop + 1 || this.edgeIds.size() != hop) {
            throw new RetrievalModelValidationException(
                "path vertex/edge counts must match hop");
        }
        for (String vertexId : this.vertexIds) {
            ModelValidation.required(vertexId, "vertexId");
        }
        for (String edgeId : this.edgeIds) {
            ModelValidation.required(edgeId, "edgeId");
        }
    }

    public List<String> getVertexIds() {
        return Collections.unmodifiableList(vertexIds == null ? Collections.emptyList() : vertexIds);
    }

    public List<String> getEdgeIds() {
        return Collections.unmodifiableList(edgeIds == null ? Collections.emptyList() : edgeIds);
    }

    public int getHop() {
        return hop;
    }

    public boolean isSampled() {
        return sampled;
    }

    public boolean sameIdentityAs(GraphPathRef other) {
        return equals(other);
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
        return hop == that.hop && sampled == that.sampled
            && Objects.equals(vertexIds, that.vertexIds)
            && Objects.equals(edgeIds, that.edgeIds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(vertexIds, edgeIds, hop, sampled);
    }
}
