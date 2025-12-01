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

package org.apache.geaflow.context.core.api;

import java.util.HashMap;
import java.util.Map;
import org.apache.geaflow.context.api.engine.ContextMemoryEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for creating ContextMemoryEngine instances with configuration.
 * Supports pluggable storage, vector index, and embedding backends.
 */
public class ContextMemoryEngineFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(ContextMemoryEngineFactory.class);

  private ContextMemoryEngineFactory() {
    // Utility class
  }

  /**
   * Create a ContextMemoryEngine with default configuration.
   *
   * @return The created engine
   * @throws Exception if creation fails
   */
  public static ContextMemoryEngine createDefault() throws Exception {
    return create(new HashMap<>());
  }

  /**
   * Create a ContextMemoryEngine with custom configuration.
   *
   * @param config The configuration map
   * @return The created engine
   * @throws Exception if creation fails
   */
  public static ContextMemoryEngine create(Map<String, String> config) throws Exception {
    LOGGER.info("Creating ContextMemoryEngine with configuration: {}", config);

    // Prepare configuration
    Map<String, String> finalConfig = prepareConfig(config);

    // Create config object for engine
    org.apache.geaflow.context.core.engine.DefaultContextMemoryEngine.ContextMemoryConfig engineConfig 
        = new org.apache.geaflow.context.core.engine.DefaultContextMemoryEngine.ContextMemoryConfig();
    
    if (finalConfig.containsKey(ContextConfigKeys.VECTOR_DIMENSION)) {
      engineConfig.setEmbeddingDimension(
          Integer.parseInt(finalConfig.get(ContextConfigKeys.VECTOR_DIMENSION)));
    }

    // Create the engine
    ContextMemoryEngine engine = new org.apache.geaflow.context.core.engine.DefaultContextMemoryEngine(engineConfig);
    engine.initialize();

    LOGGER.info("ContextMemoryEngine created successfully");
    return engine;
  }

  /**
   * Prepare and validate configuration.
   *
   * @param config The input configuration
   * @return The prepared configuration
   */
  private static Map<String, String> prepareConfig(Map<String, String> config) {
    Map<String, String> finalConfig = new HashMap<>(config);

    // Set defaults if not specified
    finalConfig.putIfAbsent(ContextConfigKeys.STORAGE_TYPE, "rocksdb");
    finalConfig.putIfAbsent(ContextConfigKeys.VECTOR_INDEX_TYPE, "faiss");
    finalConfig.putIfAbsent(ContextConfigKeys.TEXT_INDEX_TYPE, "lucene");
    finalConfig.putIfAbsent(ContextConfigKeys.EMBEDDING_GENERATOR_TYPE, "default");
    finalConfig.putIfAbsent(ContextConfigKeys.ENTITY_EXTRACTOR_TYPE, "default");
    finalConfig.putIfAbsent(ContextConfigKeys.RELATION_EXTRACTOR_TYPE, "default");
    finalConfig.putIfAbsent(ContextConfigKeys.ENTITY_LINKER_TYPE, "default");

    return finalConfig;
  }

  /**
   * Configuration keys for ContextMemoryEngine.
   */
  public static class ContextConfigKeys {

    public static final String STORAGE_TYPE = "storage.type";
    public static final String STORAGE_PATH = "storage.path";

    public static final String VECTOR_INDEX_TYPE = "vector.index.type";
    public static final String VECTOR_DIMENSION = "vector.dimension";
    public static final String VECTOR_THRESHOLD = "vector.threshold";

    public static final String TEXT_INDEX_TYPE = "text.index.type";
    public static final String TEXT_INDEX_PATH = "text.index.path";

    public static final String EMBEDDING_GENERATOR_TYPE = "embedding.generator.type";
    public static final String EMBEDDING_MODEL = "embedding.model";

    public static final String ENTITY_EXTRACTOR_TYPE = "entity.extractor.type";
    public static final String RELATION_EXTRACTOR_TYPE = "relation.extractor.type";
    public static final String ENTITY_LINKER_TYPE = "entity.linker.type";

    public static final String LLM_PROVIDER_TYPE = "llm.provider.type";
    public static final String LLM_API_KEY = "llm.api.key";
    public static final String LLM_ENDPOINT = "llm.endpoint";

    public static final String DEFAULT_STORAGE_PATH = "/tmp/context-memory";
    public static final int DEFAULT_VECTOR_DIMENSION = 768;
    public static final double DEFAULT_VECTOR_THRESHOLD = 0.7;

    private ContextConfigKeys() {
      // Utility class
    }
  }
}
