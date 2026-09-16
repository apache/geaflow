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

package org.apache.geaflow.ai.provenance;

import java.util.Objects;

/**
 * Source lineage of an extracted fact: the document and chunk the fact came
 * from, the span within the chunk, and the extractor and schema versions that
 * produced it.
 *
 * <p>Provenance records reference source coordinates only. They must never
 * carry raw document text so they can be logged and persisted without leaking
 * private content.</p>
 */
public class ProvenanceRef {

    private final String documentId;
    private final String chunkId;
    private final SourceSpan sourceSpan;
    private final String extractorVersion;
    private final String schemaVersion;

    public ProvenanceRef(String documentId, String chunkId, SourceSpan sourceSpan,
                         String extractorVersion, String schemaVersion) {
        this.documentId = documentId;
        this.chunkId = chunkId;
        this.sourceSpan = sourceSpan;
        this.extractorVersion = extractorVersion;
        this.schemaVersion = schemaVersion;
    }

    public String getDocumentId() {
        return documentId;
    }

    public String getChunkId() {
        return chunkId;
    }

    /**
     * The span within the chunk this fact was extracted from, or null when the
     * fact is attributed to the chunk as a whole.
     */
    public SourceSpan getSourceSpan() {
        return sourceSpan;
    }

    public String getExtractorVersion() {
        return extractorVersion;
    }

    public String getSchemaVersion() {
        return schemaVersion;
    }

    /**
     * Validates that provenance is present for an extracted fact.
     *
     * @throws IllegalArgumentException when provenance is missing
     */
    public static void validate(ProvenanceRef provenance) {
        if (provenance == null) {
            throw new IllegalArgumentException("provenance is missing for extracted fact");
        }
        provenance.validate();
    }

    /**
     * Validates this reference, throwing {@link IllegalArgumentException} when
     * the document, chunk, extractor version or schema version is missing, or
     * when the optional span does not belong to the referenced chunk.
     */
    public void validate() {
        require(documentId, "document id");
        require(chunkId, "chunk id");
        require(extractorVersion, "extractor version");
        require(schemaVersion, "schema version");
        if (sourceSpan != null) {
            sourceSpan.validate();
            if (!Objects.equals(sourceSpan.getChunkId(), chunkId)) {
                throw new IllegalArgumentException("source span chunk id '" + sourceSpan.getChunkId()
                    + "' does not match provenance chunk id '" + chunkId + "'");
            }
        }
    }

    private static void require(String value, String name) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("provenance " + name + " is required");
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ProvenanceRef that = (ProvenanceRef) o;
        return Objects.equals(documentId, that.documentId)
            && Objects.equals(chunkId, that.chunkId)
            && Objects.equals(sourceSpan, that.sourceSpan)
            && Objects.equals(extractorVersion, that.extractorVersion)
            && Objects.equals(schemaVersion, that.schemaVersion);
    }

    @Override
    public int hashCode() {
        return Objects.hash(documentId, chunkId, sourceSpan, extractorVersion, schemaVersion);
    }

    @Override
    public String toString() {
        return "ProvenanceRef{documentId='" + documentId + "', chunkId='" + chunkId
            + "', sourceSpan=" + sourceSpan + ", extractorVersion='" + extractorVersion
            + "', schemaVersion='" + schemaVersion + "'}";
    }
}
