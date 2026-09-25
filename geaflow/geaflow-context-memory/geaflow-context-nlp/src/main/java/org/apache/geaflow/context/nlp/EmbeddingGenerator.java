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

/**
 * Interface for embedding generation.
 * Implementations can use various NLP models (BERT, GPT, FastText, etc.).
 */
public interface EmbeddingGenerator {

    /**
     * Generate embedding for text.
     *
     * @param text Input text to embed
     * @return Float array representing the embedding vector
     * @throws Exception if embedding generation fails
     */
    float[] generateEmbedding(String text) throws Exception;

    /**
     * Generate embeddings for multiple texts.
     *
     * @param texts Array of input texts
     * @return Array of embedding vectors
     * @throws Exception if embedding generation fails
     */
    float[][] generateEmbeddings(String[] texts) throws Exception;

    /**
     * Get embedding dimension.
     *
     * @return Dimension of generated embeddings
     */
    int getEmbeddingDimension();

    /**
     * Initialize the embedding generator.
     *
     * @throws Exception if initialization fails
     */
    void initialize() throws Exception;

    /**
     * Close and cleanup resources.
     *
     * @throws Exception if close fails
     */
    void close() throws Exception;
}
