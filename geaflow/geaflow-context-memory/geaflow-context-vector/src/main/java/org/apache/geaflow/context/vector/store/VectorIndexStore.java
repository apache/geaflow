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

package org.apache.geaflow.context.vector.store;

import java.util.List;

/**
 * Abstract interface for vector storage backends.
 * Implementations can use different vector databases (FAISS, Milvus, Weaviate, etc.)
 */
public interface VectorIndexStore {

    /**
     * Initialize the store.
     *
     * @throws Exception if initialization fails
     */
    void initialize() throws Exception;

    /**
     * Add or update a vector embedding.
     *
     * @param id Entity identifier
     * @param embedding Vector embedding
     * @param version Version/timestamp for temporal support
     * @throws Exception if operation fails
     */
    void addVector(String id, float[] embedding, long version) throws Exception;

    /**
     * Search for similar vectors.
     *
     * @param queryVector Query vector
     * @param topK Number of results
     * @param threshold Similarity threshold
     * @return List of search results
     * @throws Exception if operation fails
     */
    List<VectorSearchResult> search(float[] queryVector, int topK, double threshold) throws Exception;

    /**
     * Search with graph filter.
     *
     * @param queryVector Query vector
     * @param topK Number of results
     * @param threshold Similarity threshold
     * @param filter Graph-based filter (not fully implemented in Phase 1)
     * @return Filtered search results
     * @throws Exception if operation fails
     */
    List<VectorSearchResult> searchWithFilter(float[] queryVector, int topK, double threshold,
                                             VectorFilter filter) throws Exception;

    /**
     * Get vector for an entity.
     *
     * @param id Entity identifier
     * @return Vector embedding
     * @throws Exception if operation fails
     */
    float[] getVector(String id) throws Exception;

    /**
     * Delete a vector.
     *
     * @param id Entity identifier
     * @throws Exception if operation fails
     */
    void deleteVector(String id) throws Exception;

    /**
     * Get number of vectors in the store.
     *
     * @return Vector count
     */
    int size();

    /**
     * Close and cleanup resources.
     *
     * @throws Exception if close fails
     */
    void close() throws Exception;

    /**
     * Result from vector search.
     */
    class VectorSearchResult {

        private String id;
        private double similarity;
        private float[] vector;

        public VectorSearchResult(String id, double similarity) {
            this.id = id;
            this.similarity = similarity;
        }

        public VectorSearchResult(String id, double similarity, float[] vector) {
            this.id = id;
            this.similarity = similarity;
            this.vector = vector;
        }

        public String getId() {
            return id;
        }

        public double getSimilarity() {
            return similarity;
        }

        public float[] getVector() {
            return vector;
        }

        @Override
        public String toString() {
            return "VectorSearchResult{" +
                    "id='" + id + '\'' +
                    ", similarity=" + similarity +
                    '}';
        }
    }

    /**
     * Filter for vector search.
     */
    interface VectorFilter {

        /**
         * Check if entity passes the filter.
         *
         * @param id Entity identifier
         * @return True if entity passes filter
         */
        boolean passes(String id);
    }
}
