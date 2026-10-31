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
import org.apache.geaflow.ai.retrieval.validation.RetrievalModelValidationException;

/** Immutable citation pointing to a document and, optionally, a text span. */
public final class SourceRef {

    @SerializedName("documentId")
    private final String documentId;
    @SerializedName("sourceUri")
    private final String sourceUri;
    @SerializedName("startOffset")
    private final Integer startOffset;
    @SerializedName("endOffset")
    private final Integer endOffset;

    public SourceRef(String documentId, String sourceUri) {
        this(documentId, sourceUri, null, null);
    }

    public SourceRef(String documentId, String sourceUri, Integer startOffset, Integer endOffset) {
        this.documentId = ModelValidation.required(documentId, "documentId");
        this.sourceUri = ModelValidation.required(sourceUri, "sourceUri");
        if ((startOffset == null) != (endOffset == null)) {
            throw new RetrievalModelValidationException("source offsets must be provided together");
        }
        if (startOffset != null) {
            ModelValidation.nonNegative(startOffset, "startOffset");
            ModelValidation.nonNegative(endOffset, "endOffset");
            if (endOffset < startOffset) {
                throw new RetrievalModelValidationException("endOffset must not be before startOffset");
            }
        }
        this.startOffset = startOffset;
        this.endOffset = endOffset;
    }

    public String getDocumentId() {
        return documentId;
    }

    public String getSourceUri() {
        return sourceUri;
    }

    public Integer getStartOffset() {
        return startOffset;
    }

    public Integer getEndOffset() {
        return endOffset;
    }

    public boolean sameIdentityAs(SourceRef other) {
        return equals(other);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof SourceRef)) {
            return false;
        }
        SourceRef that = (SourceRef) object;
        return Objects.equals(documentId, that.documentId)
            && Objects.equals(sourceUri, that.sourceUri)
            && Objects.equals(startOffset, that.startOffset)
            && Objects.equals(endOffset, that.endOffset);
    }

    @Override
    public int hashCode() {
        return Objects.hash(documentId, sourceUri, startOffset, endOffset);
    }
}
