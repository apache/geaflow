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

package org.apache.geaflow.ai.retrieval.model.document;

import com.google.gson.annotations.SerializedName;
import java.util.Objects;
import org.apache.geaflow.ai.retrieval.validation.ModelValidation;

/** Immutable metadata describing a source document in a retrieval corpus. */
public final class SourceDocument {

    @SerializedName("documentId")
    private final String documentId;
    @SerializedName("dataset")
    private final String dataset;
    @SerializedName("datasetVersion")
    private final String datasetVersion;
    @SerializedName("split")
    private final String split;
    @SerializedName("title")
    private final String title;
    @SerializedName("sourceUri")
    private final String sourceUri;
    @SerializedName("sourceHash")
    private final String sourceHash;

    public SourceDocument(String documentId, String dataset, String datasetVersion,
                          String split, String sourceUri, String sourceHash) {
        this(documentId, dataset, datasetVersion, split, null, sourceUri, sourceHash);
    }

    public SourceDocument(String documentId, String dataset, String datasetVersion,
                          String split, String title, String sourceUri, String sourceHash) {
        this.documentId = ModelValidation.required(documentId, "documentId");
        this.dataset = ModelValidation.required(dataset, "dataset");
        this.datasetVersion = ModelValidation.required(datasetVersion, "datasetVersion");
        this.split = ModelValidation.required(split, "split");
        this.title = ModelValidation.optional(title);
        this.sourceUri = ModelValidation.required(sourceUri, "sourceUri");
        this.sourceHash = ModelValidation.required(sourceHash, "sourceHash");
    }

    public String getDocumentId() {
        return documentId;
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

    public String getTitle() {
        return title;
    }

    public String getSourceUri() {
        return sourceUri;
    }

    public String getSourceHash() {
        return sourceHash;
    }

    public boolean sameIdentityAs(SourceDocument other) {
        return other != null && Objects.equals(documentId, other.documentId)
            && Objects.equals(dataset, other.dataset)
            && Objects.equals(datasetVersion, other.datasetVersion)
            && Objects.equals(split, other.split)
            && Objects.equals(sourceUri, other.sourceUri)
            && Objects.equals(sourceHash, other.sourceHash);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof SourceDocument)) {
            return false;
        }
        SourceDocument that = (SourceDocument) object;
        return Objects.equals(documentId, that.documentId)
            && Objects.equals(dataset, that.dataset)
            && Objects.equals(datasetVersion, that.datasetVersion)
            && Objects.equals(split, that.split)
            && Objects.equals(title, that.title)
            && Objects.equals(sourceUri, that.sourceUri)
            && Objects.equals(sourceHash, that.sourceHash);
    }

    @Override
    public int hashCode() {
        return Objects.hash(documentId, dataset, datasetVersion, split, title, sourceUri, sourceHash);
    }
}
