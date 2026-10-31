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

/** Immutable graph edge reference connecting two entity identifiers. */
public final class GraphEdgeRef {

    @SerializedName("edgeId")
    private final String edgeId;
    @SerializedName("label")
    private final String label;
    @SerializedName("sourceEntityId")
    private final String sourceEntityId;
    @SerializedName("targetEntityId")
    private final String targetEntityId;
    @SerializedName("sourceChunkIds")
    private final List<String> sourceChunkIds;

    private GraphEdgeRef() {
        this.edgeId = null;
        this.label = null;
        this.sourceEntityId = null;
        this.targetEntityId = null;
        this.sourceChunkIds = Collections.emptyList();
    }

    public GraphEdgeRef(String edgeId, String label, String sourceEntityId, String targetEntityId) {
        this(edgeId, label, sourceEntityId, targetEntityId, java.util.Collections.emptyList());
    }

    public GraphEdgeRef(String edgeId, String label, String sourceEntityId,
                        String targetEntityId, List<String> sourceChunkIds) {
        this.edgeId = ModelValidation.required(edgeId, "edgeId");
        this.label = ModelValidation.required(label, "label");
        this.sourceEntityId = ModelValidation.required(sourceEntityId, "sourceEntityId");
        this.targetEntityId = ModelValidation.required(targetEntityId, "targetEntityId");
        this.sourceChunkIds = ModelValidation.sortedStrings(sourceChunkIds, "sourceChunkId");
    }

    public String getEdgeId() {
        return edgeId;
    }

    public String getLabel() {
        return label;
    }

    public String getSourceEntityId() {
        return sourceEntityId;
    }

    public String getTargetEntityId() {
        return targetEntityId;
    }

    public List<String> getSourceChunkIds() {
        return Collections.unmodifiableList(sourceChunkIds == null
            ? Collections.emptyList() : sourceChunkIds);
    }

    public boolean sameIdentityAs(GraphEdgeRef other) {
        return other != null && Objects.equals(edgeId, other.edgeId)
            && Objects.equals(label, other.label)
            && Objects.equals(sourceEntityId, other.sourceEntityId)
            && Objects.equals(targetEntityId, other.targetEntityId);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof GraphEdgeRef)) {
            return false;
        }
        GraphEdgeRef that = (GraphEdgeRef) object;
        return Objects.equals(edgeId, that.edgeId)
            && Objects.equals(label, that.label)
            && Objects.equals(sourceEntityId, that.sourceEntityId)
            && Objects.equals(targetEntityId, that.targetEntityId)
            && Objects.equals(sourceChunkIds, that.sourceChunkIds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(edgeId, label, sourceEntityId, targetEntityId, sourceChunkIds);
    }
}
