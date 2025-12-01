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

package org.apache.geaflow.context.core.engine;

import org.apache.geaflow.context.api.engine.ContextMemoryEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Default implementation of EmbeddingIndex using in-memory storage.
 * Phase 1 implementation - suitable for development/testing.
 * Production deployments should use external vector databases like FAISS, Milvus, or Elasticsearch.
 */
public class DefaultEmbeddingIndex implements ContextMemoryEngine.EmbeddingIndex {

    private static final Logger logger = LoggerFactory.getLogger(DefaultEmbeddingIndex.class);

    private final Map<String, float[]> embeddings;

    public DefaultEmbeddingIndex() {
        this.embeddings = new ConcurrentHashMap<>();
    }

    /**
     * Initialize the index.
     *
     * @throws Exception if initialization fails
     */
    public void initialize() throws Exception {
        logger.info("Initializing DefaultEmbeddingIndex");
    }

    /**
     * Add or update vector embedding for an entity.
     *
     * @param entityId Entity identifier
     * @param embedding Vector embedding
     * @throws Exception if operation fails
     */
    @Override
    public void addEmbedding(String entityId, float[] embedding) throws Exception {
        if (embedding == null || embedding.length == 0) {
            throw new IllegalArgumentException("Embedding cannot be null or empty");
        }
        embeddings.put(entityId, embedding.clone());
        logger.debug("Added embedding for entity: {}", entityId);
    }

    /**
     * Search similar entities by vector using cosine similarity.
     *
     * @param queryVector Query vector
     * @param topK Number of results to return
     * @param threshold Similarity threshold (0.0 - 1.0)
     * @return List of similar entity IDs with scores
     * @throws Exception if search fails
     */
    @Override
    public List<EmbeddingSearchResult> search(float[] queryVector, int topK, double threshold) throws Exception {
        if (queryVector == null || queryVector.length == 0) {
            throw new IllegalArgumentException("Query vector cannot be null or empty");
        }

        List<EmbeddingSearchResult> results = new ArrayList<>();

        // Calculate similarity with all embeddings
        for (Map.Entry<String, float[]> entry : embeddings.entrySet()) {
            double similarity = cosineSimilarity(queryVector, entry.getValue());

            if (similarity >= threshold) {
                results.add(new EmbeddingSearchResult(entry.getKey(), similarity));
            }
        }

        // Sort by similarity descending and limit to topK
        results.sort((a, b) -> Double.compare(b.getSimilarity(), a.getSimilarity()));

        if (results.size() > topK) {
            results = results.subList(0, topK);
        }

        logger.debug("Vector search found {} results", results.size());
        return results;
    }

    /**
     * Get embedding for an entity.
     *
     * @param entityId Entity identifier
     * @return Vector embedding
     * @throws Exception if operation fails
     */
    @Override
    public float[] getEmbedding(String entityId) throws Exception {
        float[] embedding = embeddings.get(entityId);
        if (embedding == null) {
            return null;
        }
        return embedding.clone();
    }

    /**
     * Calculate cosine similarity between two vectors.
     *
     * @param vector1 First vector
     * @param vector2 Second vector
     * @return Cosine similarity (0.0 - 1.0)
     */
    private double cosineSimilarity(float[] vector1, float[] vector2) {
        if (vector1.length != vector2.length) {
            throw new IllegalArgumentException("Vectors must have same length");
        }

        double dotProduct = 0.0;
        double norm1 = 0.0;
        double norm2 = 0.0;

        for (int i = 0; i < vector1.length; i++) {
            dotProduct += vector1[i] * vector2[i];
            norm1 += vector1[i] * vector1[i];
            norm2 += vector2[i] * vector2[i];
        }

        if (norm1 == 0.0 || norm2 == 0.0) {
            return 0.0;
        }

        return dotProduct / (Math.sqrt(norm1) * Math.sqrt(norm2));
    }

    /**
     * Get number of indexed embeddings.
     *
     * @return Number of embeddings
     */
    public int size() {
        return embeddings.size();
    }

    /**
     * Clear all embeddings.
     */
    public void clear() {
        embeddings.clear();
        logger.info("DefaultEmbeddingIndex cleared");
    }

    /**
     * Close and cleanup.
     *
     * @throws Exception if close fails
     */
    public void close() throws Exception {
        clear();
        logger.info("DefaultEmbeddingIndex closed");
    }
}
