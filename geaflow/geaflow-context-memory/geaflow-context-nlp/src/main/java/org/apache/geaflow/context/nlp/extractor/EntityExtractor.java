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

package org.apache.geaflow.context.nlp.extractor;

import java.util.List;
import org.apache.geaflow.context.nlp.entity.Entity;

/**
 * Interface for named entity recognition (NER) extraction.
 * Supports pluggable implementations for different NLP models.
 */
public interface EntityExtractor {

  /**
   * Initialize the extractor with specified model.
   *
   * @throws Exception if initialization fails
   */
  void initialize() throws Exception;

  /**
   * Extract entities from text.
   *
   * @param text The input text to extract entities from
   * @return A list of extracted entities
   * @throws Exception if extraction fails
   */
  List<Entity> extractEntities(String text) throws Exception;

  /**
   * Extract entities from multiple texts.
   *
   * @param texts The input texts to extract entities from
   * @return A list of entities extracted from all texts
   * @throws Exception if extraction fails
   */
  List<Entity> extractEntitiesBatch(String[] texts) throws Exception;

  /**
   * Get the supported entity types.
   *
   * @return A list of entity type labels
   */
  List<String> getSupportedEntityTypes();

  /**
   * Get the model name.
   *
   * @return The model name
   */
  String getModelName();

  /**
   * Close the extractor and release resources.
   *
   * @throws Exception if closing fails
   */
  void close() throws Exception;
}
