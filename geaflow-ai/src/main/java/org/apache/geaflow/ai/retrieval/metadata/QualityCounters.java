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

package org.apache.geaflow.ai.retrieval.metadata;

import java.util.Objects;

/** Non-negative quality measurements for a build. */
public final class QualityCounters {

    private final long documents;
    private final long chunks;
    private final long entities;
    private final long edges;
    private final long skippedRecords;
    private final long validationErrors;

    public QualityCounters(
        long documents,
        long chunks,
        long entities,
        long edges,
        long skippedRecords,
        long validationErrors) {
        MetadataValidation.nonNegative(documents, "documents");
        MetadataValidation.nonNegative(chunks, "chunks");
        MetadataValidation.nonNegative(entities, "entities");
        MetadataValidation.nonNegative(edges, "edges");
        MetadataValidation.nonNegative(skippedRecords, "skippedRecords");
        MetadataValidation.nonNegative(validationErrors, "validationErrors");
        this.documents = documents;
        this.chunks = chunks;
        this.entities = entities;
        this.edges = edges;
        this.skippedRecords = skippedRecords;
        this.validationErrors = validationErrors;
    }

    public long getDocuments() {
        return documents;
    }

    public long getChunks() {
        return chunks;
    }

    public long getEntities() {
        return entities;
    }

    public long getEdges() {
        return edges;
    }

    public long getSkippedRecords() {
        return skippedRecords;
    }

    public long getValidationErrors() {
        return validationErrors;
    }

    public long getDocumentCount() {
        return documents;
    }

    public long getChunkCount() {
        return chunks;
    }

    public long getEntityCount() {
        return entities;
    }

    public long getEdgeCount() {
        return edges;
    }

    public long getSkippedRecordCount() {
        return skippedRecords;
    }

    public long getValidationErrorCount() {
        return validationErrors;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof QualityCounters)) {
            return false;
        }
        QualityCounters that = (QualityCounters) object;
        return documents == that.documents
            && chunks == that.chunks
            && entities == that.entities
            && edges == that.edges
            && skippedRecords == that.skippedRecords
            && validationErrors == that.validationErrors;
    }

    @Override
    public int hashCode() {
        return Objects.hash(documents, chunks, entities, edges, skippedRecords, validationErrors);
    }
}
