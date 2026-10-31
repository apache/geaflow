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

/** Immutable, position-aware text segment produced by a chunking policy. */
public final class TextChunk {

    @SerializedName("chunkId")
    private final String chunkId;
    @SerializedName("documentId")
    private final String documentId;
    @SerializedName("chunkIndex")
    private final int chunkIndex;
    @SerializedName("startOffset")
    private final int startOffset;
    @SerializedName("endOffset")
    private final int endOffset;
    @SerializedName("tokenEstimate")
    private final int tokenEstimate;
    @SerializedName("text")
    private final String text;
    @SerializedName("policyVersion")
    private final String policyVersion;
    @SerializedName("textHash")
    private final String textHash;
    @SerializedName("sourceUri")
    private final String sourceUri;

    public TextChunk(String chunkId, String documentId, int chunkIndex, int startOffset,
                     int endOffset, int tokenEstimate, String text) {
        this(chunkId, documentId, chunkIndex, startOffset, endOffset, tokenEstimate, text,
            null, null, null);
    }

    public TextChunk(String chunkId, String documentId, int chunkIndex, int startOffset,
                     int endOffset, int tokenEstimate, String text,
                     String policyVersion, String textHash) {
        this(chunkId, documentId, chunkIndex, startOffset, endOffset, tokenEstimate, text,
            policyVersion, textHash, null);
    }

    public TextChunk(String chunkId, String documentId, int chunkIndex, int startOffset,
                     int endOffset, int tokenEstimate, String text,
                     String policyVersion, String textHash, String sourceUri) {
        this.chunkId = ModelValidation.required(chunkId, "chunkId");
        this.documentId = ModelValidation.required(documentId, "documentId");
        this.chunkIndex = ModelValidation.nonNegative(chunkIndex, "chunkIndex");
        this.startOffset = ModelValidation.nonNegative(startOffset, "startOffset");
        this.endOffset = ModelValidation.nonNegative(endOffset, "endOffset");
        if (endOffset < startOffset) {
            throw new RetrievalModelValidationException("endOffset must not be before startOffset");
        }
        this.tokenEstimate = ModelValidation.nonNegative(tokenEstimate, "tokenEstimate");
        this.text = ModelValidation.required(text, "text");
        this.policyVersion = ModelValidation.optionalNonBlank(policyVersion, "policyVersion");
        this.textHash = ModelValidation.optionalNonBlank(textHash, "textHash");
        this.sourceUri = ModelValidation.optionalNonBlank(sourceUri, "sourceUri");
    }

    public String getChunkId() {
        return chunkId;
    }

    public String getDocumentId() {
        return documentId;
    }

    public int getChunkIndex() {
        return chunkIndex;
    }

    public int getStartOffset() {
        return startOffset;
    }

    public int getEndOffset() {
        return endOffset;
    }

    public int getTokenEstimate() {
        return tokenEstimate;
    }

    public String getText() {
        return text;
    }

    public String getPolicyVersion() {
        return policyVersion;
    }

    public String getTextHash() {
        return textHash;
    }

    public String getSourceUri() {
        return sourceUri;
    }

    public TextChunk withSourceUri(String uri) {
        return new TextChunk(chunkId, documentId, chunkIndex, startOffset, endOffset,
            tokenEstimate, text, policyVersion, textHash, ModelValidation.required(uri, "sourceUri"));
    }

    public boolean sameIdentityAs(TextChunk other) {
        return other != null && Objects.equals(chunkId, other.chunkId)
            && Objects.equals(documentId, other.documentId)
            && chunkIndex == other.chunkIndex
            && startOffset == other.startOffset
            && endOffset == other.endOffset
            && Objects.equals(text, other.text)
            && Objects.equals(policyVersion, other.policyVersion)
            && Objects.equals(textHash, other.textHash)
            && Objects.equals(sourceUri, other.sourceUri);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof TextChunk)) {
            return false;
        }
        TextChunk that = (TextChunk) object;
        return chunkIndex == that.chunkIndex && startOffset == that.startOffset
            && endOffset == that.endOffset && tokenEstimate == that.tokenEstimate
            && Objects.equals(chunkId, that.chunkId)
            && Objects.equals(documentId, that.documentId)
            && Objects.equals(text, that.text)
            && Objects.equals(policyVersion, that.policyVersion)
            && Objects.equals(textHash, that.textHash)
            && Objects.equals(sourceUri, that.sourceUri);
    }

    @Override
    public int hashCode() {
        return Objects.hash(chunkId, documentId, chunkIndex, startOffset, endOffset,
            tokenEstimate, text, policyVersion, textHash, sourceUri);
    }
}
