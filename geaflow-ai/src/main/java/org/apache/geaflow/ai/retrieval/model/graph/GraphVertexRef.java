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
import java.util.Objects;
import org.apache.geaflow.ai.retrieval.validation.ModelValidation;

/** Immutable graph vertex reference associated with an entity identifier. */
public final class GraphVertexRef {

    @SerializedName("label")
    private final String label;
    @SerializedName("vertexId")
    private final String vertexId;
    @SerializedName("entityId")
    private final String entityId;

    public GraphVertexRef(String label, String vertexId, String entityId) {
        this.label = ModelValidation.required(label, "label");
        this.vertexId = ModelValidation.required(vertexId, "vertexId");
        this.entityId = ModelValidation.required(entityId, "entityId");
    }

    public String getLabel() {
        return label;
    }

    public String getVertexId() {
        return vertexId;
    }

    public String getEntityId() {
        return entityId;
    }

    public boolean sameIdentityAs(GraphVertexRef other) {
        return other != null && Objects.equals(label, other.label)
            && Objects.equals(vertexId, other.vertexId)
            && Objects.equals(entityId, other.entityId);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof GraphVertexRef)) {
            return false;
        }
        GraphVertexRef that = (GraphVertexRef) object;
        return Objects.equals(label, that.label)
            && Objects.equals(vertexId, that.vertexId)
            && Objects.equals(entityId, that.entityId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(label, vertexId, entityId);
    }
}
