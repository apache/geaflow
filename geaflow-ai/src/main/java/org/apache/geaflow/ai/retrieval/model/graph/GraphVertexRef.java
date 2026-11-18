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
    @SerializedName("dataset")
    private final String dataset;
    @SerializedName("datasetVersion")
    private final String datasetVersion;
    @SerializedName("split")
    private final String split;
    @SerializedName("sourceHash")
    private final String sourceHash;

    public GraphVertexRef(String label, String vertexId, String entityId) {
        this(label, vertexId, entityId, null, null, null, null);
    }

    public GraphVertexRef(String label, String vertexId, String entityId, String dataset,
                          String datasetVersion, String split, String sourceHash) {
        this.label = ModelValidation.required(label, "label");
        this.vertexId = ModelValidation.required(vertexId, "vertexId");
        this.entityId = ModelValidation.required(entityId, "entityId");
        this.dataset = ModelValidation.optionalNonBlank(dataset, "dataset");
        this.datasetVersion = ModelValidation.optionalNonBlank(datasetVersion, "datasetVersion");
        this.split = ModelValidation.optionalNonBlank(split, "split");
        this.sourceHash = ModelValidation.optionalNonBlank(sourceHash, "sourceHash");
        boolean any = dataset != null || datasetVersion != null || split != null || sourceHash != null;
        boolean complete = dataset != null && datasetVersion != null && split != null && sourceHash != null;
        if (any && !complete) {
            throw new IllegalArgumentException(
                "vertex dataset, datasetVersion, split, and sourceHash must be provided together");
        }
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

    public String getDataset() {
        return dataset;
    }

    public String getDatasetVersion() {
        return datasetVersion;
    }

    public String getSplit() {
        return split;
    }

    public String getSourceHash() {
        return sourceHash;
    }

    public boolean sameIdentityAs(GraphVertexRef other) {
        return other != null && Objects.equals(label, other.label)
            && Objects.equals(vertexId, other.vertexId)
            && Objects.equals(entityId, other.entityId)
            && Objects.equals(dataset, other.dataset)
            && Objects.equals(datasetVersion, other.datasetVersion)
            && Objects.equals(split, other.split)
            && Objects.equals(sourceHash, other.sourceHash);
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
            && Objects.equals(entityId, that.entityId)
            && Objects.equals(dataset, that.dataset)
            && Objects.equals(datasetVersion, that.datasetVersion)
            && Objects.equals(split, that.split)
            && Objects.equals(sourceHash, that.sourceHash);
    }

    @Override
    public int hashCode() {
        return Objects.hash(label, vertexId, entityId, dataset, datasetVersion, split, sourceHash);
    }
}
