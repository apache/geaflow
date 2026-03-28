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

package org.apache.geaflow.context.nlp;

import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default embedding generator implementation using deterministic hash-based vectors.
 * For production use, integrate with real NLP models (BERT, GPT, etc.).
 * This is a Phase 2 baseline implementation.
 */
public class DefaultEmbeddingGenerator implements EmbeddingGenerator {

    private static final Logger logger = LoggerFactory.getLogger(
        DefaultEmbeddingGenerator.class);

    private static final int DEFAULT_EMBEDDING_DIMENSION = 768;
    private final int embeddingDimension;

    /**
     * Constructor with default dimension.
     */
    public DefaultEmbeddingGenerator() {
        this(DEFAULT_EMBEDDING_DIMENSION);
    }

    /**
     * Constructor with custom dimension.
     *
     * @param dimension Embedding dimension
     */
    public DefaultEmbeddingGenerator(int dimension) {
        this.embeddingDimension = dimension;
    }

    @Override
    public void initialize() throws Exception {
        logger.info("Initializing DefaultEmbeddingGenerator with dimension: {}",
            embeddingDimension);
    }

    @Override
    public float[] generateEmbedding(String text) throws Exception {
        if (text == null || text.isEmpty()) {
            throw new IllegalArgumentException("Text cannot be null or empty");
        }

        float[] embedding = new float[embeddingDimension];
        Random random = new Random(text.hashCode());

        // Generate deterministic embedding based on text hash
        for (int i = 0; i < embeddingDimension; i++) {
            embedding[i] = random.nextFloat() * 2.0f - 1.0f; // Range [-1, 1]
        }

        // Normalize vector to unit length
        normalizeVector(embedding);

        return embedding;
    }

    @Override
    public float[][] generateEmbeddings(String[] texts) throws Exception {
        if (texts == null || texts.length == 0) {
            throw new IllegalArgumentException("Texts array cannot be null or empty");
        }

        float[][] embeddings = new float[texts.length][];
        for (int i = 0; i < texts.length; i++) {
            embeddings[i] = generateEmbedding(texts[i]);
        }

        return embeddings;
    }

    @Override
    public int getEmbeddingDimension() {
        return embeddingDimension;
    }

    @Override
    public void close() throws Exception {
        logger.info("Closing DefaultEmbeddingGenerator");
    }

    /**
     * Normalize vector to unit length.
     *
     * @param vector Vector to normalize
     */
    private void normalizeVector(float[] vector) {
        double norm = 0.0;
        for (float v : vector) {
            norm += v * v;
        }
        norm = Math.sqrt(norm);

        if (norm > 0.0) {
            for (int i = 0; i < vector.length; i++) {
                vector[i] = (float) (vector[i] / norm);
            }
        }
    }
}
