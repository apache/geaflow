/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.geaflow.ai.index.vectorstore;

import java.util.List;

public interface VectorStore {

    /**
     * Insert or update a single vector record.
     * Upsert is idempotent: same vectorId overwrites the previous record.
     *
     * @throws VectorStoreException if record.embedding.length != metadata.dimension (DIMENSION_MISMATCH)
     * @throws VectorStoreException if required metadata fields are missing (METADATA_INCOMPLETE)
     */
    void upsert(VectorRecord record);

    /**
     * Batch insert/update. Atomic per record; partial failure does not
     * roll back already-committed records.
     */
    void upsertBatch(List<VectorRecord> records);

    /**
     * Search for nearest vectors using the configured distance metric.
     * Results are ordered by score descending (best match first).
     *
     * @return empty list if no results match, never null
     * @throws VectorStoreException if filterMetadata specifies
     *         a model_name different from the store's model_name (MODEL_MISMATCH)
     */
    List<VectorHit> search(VectorQuery query);

    /**
     * Soft-delete a vector by id. The record remains on disk but is
     * excluded from future search results.
     *
     * @throws VectorStoreException if vectorId does not exist (RECORD_NOT_FOUND)
     */
    void markDeleted(String vectorId);

    /**
     * Returns metadata about this store instance.
     */
    VectorStoreMetadata getMetadata();

    /**
     * Release resources. Implementations must flush pending writes before returning.
     */
    void close();
}
