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

import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.infer.InferContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Production embedding generator using GeaFlow-Infer Python integration.
 * Supports Sentence-BERT and other transformer models.
 */
public class InferEmbeddingGenerator implements EmbeddingGenerator {

  private static final Logger LOGGER = LoggerFactory.getLogger(InferEmbeddingGenerator.class);

  private final Configuration config;
  private final int embeddingDimension;
  private InferContext<float[]> inferContext;
  private boolean initialized = false;

  public InferEmbeddingGenerator(Configuration config, int embeddingDimension) {
    this.config = config;
    this.embeddingDimension = embeddingDimension;
  }

  public InferEmbeddingGenerator(Configuration config) {
    this(config, 384);
  }

  @Override
  public void initialize() throws Exception {
    if (initialized) {
      return;
    }

    LOGGER.info("Initializing InferEmbeddingGenerator with dimension: {}", embeddingDimension);

    try {
      inferContext = new InferContext<>(config);
      initialized = true;
      LOGGER.info("InferEmbeddingGenerator initialized successfully");
    } catch (Exception e) {
      LOGGER.error("Failed to initialize InferEmbeddingGenerator", e);
      throw new RuntimeException("Embedding generator initialization failed", e);
    }
  }

  @Override
  public float[] generateEmbedding(String text) throws Exception {
    if (text == null || text.trim().isEmpty()) {
      throw new IllegalArgumentException("Text cannot be null or empty");
    }

    if (!initialized) {
      throw new IllegalStateException("Generator not initialized");
    }

    try {
      float[] embedding = inferContext.infer(text);

      if (embedding == null || embedding.length != embeddingDimension) {
        throw new RuntimeException(
            String.format("Invalid embedding dimension: expected %d, got %d",
                embeddingDimension, embedding != null ? embedding.length : 0));
      }

      return embedding;

    } catch (Exception e) {
      LOGGER.error("Failed to generate embedding for text: {}", text, e);
      throw e;
    }
  }

  @Override
  public float[][] generateEmbeddings(String[] texts) throws Exception {
    if (!initialized) {
      throw new IllegalStateException("Generator not initialized");
    }

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
    if (inferContext != null) {
      inferContext.close();
      inferContext = null;
    }
    initialized = false;
    LOGGER.info("InferEmbeddingGenerator closed");
  }

  public boolean isInitialized() {
    return initialized;
  }
}
