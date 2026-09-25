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

package org.apache.geaflow.context.vector.faiss;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.context.api.engine.ContextMemoryEngine;
import org.apache.geaflow.infer.InferContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Production-ready FAISS vector index using GeaFlow-Infer Python integration.
 * Supports IVF_FLAT, IVF_PQ, HNSW, and FLAT index types.
 */
public class FAISSVectorIndex implements ContextMemoryEngine.EmbeddingIndex {

  private static final Logger LOGGER = LoggerFactory.getLogger(FAISSVectorIndex.class);

  private final Configuration config;
  private final int vectorDimension;
  private final String indexType;
  private final Map<String, float[]> localCache;
  private InferContext<Object> inferContext;
  private boolean initialized = false;

  public FAISSVectorIndex(Configuration config, int vectorDimension, String indexType) {
    this.config = config;
    this.vectorDimension = vectorDimension;
    this.indexType = indexType != null ? indexType : "IVF_FLAT";
    this.localCache = new ConcurrentHashMap<>();
    LOGGER.info("FAISSVectorIndex created: dimension={}, indexType={}",
        vectorDimension, indexType);
  }

  public FAISSVectorIndex(Configuration config, int vectorDimension) {
    this(config, vectorDimension, "IVF_FLAT");
  }

  public void initialize() throws Exception {
    if (initialized) {
      return;
    }

    try {
      LOGGER.info("Initializing FAISS index via GeaFlow-Infer");
      inferContext = new InferContext<>(config);
      initialized = true;
      LOGGER.info("FAISS index initialized successfully");
    } catch (Exception e) {
      LOGGER.error("Failed to initialize FAISS index", e);
      throw new RuntimeException("FAISS initialization failed", e);
    }
  }

  @Override
  public void addEmbedding(String entityId, float[] embedding) throws Exception {
    if (embedding == null || embedding.length != vectorDimension) {
      throw new IllegalArgumentException(
          String.format("Embedding dimension mismatch: expected %d, got %d",
              vectorDimension, embedding != null ? embedding.length : 0));
    }

    if (!initialized) {
      throw new IllegalStateException("FAISS index not initialized");
    }

    try {
      Boolean result = (Boolean) inferContext.infer("add", entityId, embedding);
      if (result != null && result) {
        localCache.put(entityId, embedding.clone());
      }
    } catch (Exception e) {
      LOGGER.error("Failed to add embedding for entity: {}", entityId, e);
      throw e;
    }
  }

  @Override
  public List<ContextMemoryEngine.EmbeddingSearchResult> search(float[] queryVector, int topK,
      double threshold) throws Exception {
    if (queryVector == null || queryVector.length != vectorDimension) {
      throw new IllegalArgumentException(
          String.format("Query vector dimension mismatch: expected %d, got %d",
              vectorDimension, queryVector != null ? queryVector.length : 0));
    }

    if (!initialized) {
      throw new IllegalStateException("FAISS index not initialized");
    }

    try {
      @SuppressWarnings("unchecked")
      List<Object[]> rawResults = (List<Object[]>) inferContext.infer("search", queryVector, topK, threshold);
      
      List<ContextMemoryEngine.EmbeddingSearchResult> results = new ArrayList<>();
      if (rawResults != null) {
        for (Object[] item : rawResults) {
          if (item.length >= 2) {
            String entityId = (String) item[0];
            double similarity = ((Number) item[1]).doubleValue();
            results.add(new ContextMemoryEngine.EmbeddingSearchResult(entityId, similarity));
          }
        }
      }
      
      return results;
      
    } catch (Exception e) {
      LOGGER.error("FAISS search failed", e);
      throw e;
    }
  }

  @Override
  public float[] getEmbedding(String entityId) throws Exception {
    float[] cached = localCache.get(entityId);
    if (cached != null) {
      return cached.clone();
    }

    if (!initialized) {
      return null;
    }

    try {
      float[] result = (float[]) inferContext.infer("get", entityId);
      if (result != null) {
        localCache.put(entityId, result.clone());
      }
      return result;
    } catch (Exception e) {
      LOGGER.warn("Failed to get embedding for entity: {}", entityId, e);
      return null;
    }
  }

  public void deleteEmbedding(String entityId) throws Exception {
    if (!initialized) {
      throw new IllegalStateException("FAISS index not initialized");
    }

    try {
      inferContext.infer("delete", entityId);
      localCache.remove(entityId);
    } catch (Exception e) {
      LOGGER.error("Failed to delete embedding for entity: {}", entityId, e);
      throw e;
    }
  }

  public int size() throws Exception {
    if (!initialized) {
      return 0;
    }

    try {
      Integer result = (Integer) inferContext.infer("size");
      return result != null ? result : 0;
    } catch (Exception e) {
      LOGGER.error("Failed to get index size", e);
      return 0;
    }
  }

  public void train(float[][] vectors) throws Exception {
    if (!initialized) {
      throw new IllegalStateException("FAISS index not initialized");
    }

    try {
      LOGGER.info("Training FAISS index with {} vectors", vectors.length);
      inferContext.infer("train", (Object) vectors);
      LOGGER.info("FAISS index training complete");
    } catch (Exception e) {
      LOGGER.error("Failed to train FAISS index", e);
      throw e;
    }
  }

  public void close() throws Exception {
    if (inferContext != null) {
      inferContext.close();
      inferContext = null;
    }
    localCache.clear();
    initialized = false;
    LOGGER.info("FAISS index closed");
  }

  public int getVectorDimension() {
    return vectorDimension;
  }

  public String getIndexType() {
    return indexType;
  }

  public boolean isInitialized() {
    return initialized;
  }
}
