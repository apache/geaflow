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

package org.apache.geaflow.context.api.engine;

import java.io.Closeable;
import java.io.IOException;
import org.apache.geaflow.context.api.model.Episode;
import org.apache.geaflow.context.api.query.ContextQuery;
import org.apache.geaflow.context.api.result.ContextSearchResult;

/**
 * ContextMemoryEngine is the main interface for AI context memory operations.
 * It supports episode ingestion, hybrid retrieval, and temporal queries.
 */
public interface ContextMemoryEngine extends Closeable {

    /**
     * Initialize the context memory engine with configuration.
     *
     * @throws Exception if initialization fails
     */
    void initialize() throws Exception;

    /**
     * Ingest an episode into the context memory.
     *
     * @param episode The episode to ingest
     * @return Handle/ID for the ingested episode
     * @throws Exception if ingestion fails
     */
    String ingestEpisode(Episode episode) throws Exception;

    /**
     * Perform a hybrid retrieval query on the context memory.
     *
     * @param query The context query
     * @return Search results containing relevant entities and relations
     * @throws Exception if query fails
     */
    ContextSearchResult search(ContextQuery query) throws Exception;

    /**
     * Get context snapshot at a specific timestamp.
     *
     * @param timestamp The timestamp for the snapshot
     * @return Context snapshot
     * @throws Exception if snapshot retrieval fails
     */
    ContextSnapshot createSnapshot(long timestamp) throws Exception;

    /**
     * Get temporal graph for time range queries.
     *
     * @param filter Temporal filter for the query
     * @return Temporal graph data
     * @throws Exception if temporal graph retrieval fails
     */
    TemporalContextGraph getTemporalGraph(ContextQuery.TemporalFilter filter) throws Exception;

    /**
     * Get embedding index manager.
     *
     * @return Embedding index
     */
    EmbeddingIndex getEmbeddingIndex();

    /**
     * Shutdown the engine and cleanup resources.
     *
     * @throws IOException if shutdown fails
     */
    @Override
    void close() throws IOException;

    /**
     * ContextSnapshot represents a point-in-time snapshot of context memory.
     */
    interface ContextSnapshot {

        long getTimestamp();

        Object getVertices();

        Object getEdges();
    }

    /**
     * TemporalContextGraph represents graph data with temporal information.
     */
    interface TemporalContextGraph {

        Object getVertices();

        Object getEdges();

        long getStartTime();

        long getEndTime();
    }

    /**
     * EmbeddingIndex manages vector embeddings for entities.
     */
    interface EmbeddingIndex {

        /**
         * Add or update vector embedding for an entity.
         *
         * @param entityId Entity identifier
         * @param embedding Vector embedding
         */
        void addEmbedding(String entityId, float[] embedding) throws Exception;

        /**
         * Search similar entities by vector.
         *
         * @param queryVector Query vector
         * @param topK Number of results to return
         * @param threshold Similarity threshold
         * @return List of similar entity IDs with scores
         */
        java.util.List<EmbeddingSearchResult> search(float[] queryVector, int topK, double threshold) throws Exception;

        /**
         * Get embedding for an entity.
         *
         * @param entityId Entity identifier
         * @return Vector embedding
         */
        float[] getEmbedding(String entityId) throws Exception;
    }

    /**
     * Result from embedding similarity search.
     */
    class EmbeddingSearchResult {

        private String entityId;
        private double similarity;

        public EmbeddingSearchResult(String entityId, double similarity) {
            this.entityId = entityId;
            this.similarity = similarity;
        }

        public String getEntityId() {
            return entityId;
        }

        public double getSimilarity() {
            return similarity;
        }
    }
}
