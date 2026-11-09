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
import org.apache.geaflow.context.nlp.entity.Relation;

/**
 * Interface for relation extraction from text.
 * Supports pluggable implementations for different RE models.
 */
public interface RelationExtractor {

  /**
   * Initialize the extractor with specified model.
   *
   * @throws Exception if initialization fails
   */
  void initialize() throws Exception;

  /**
   * Extract relations from text.
   *
   * @param text The input text to extract relations from
   * @return A list of extracted relations
   * @throws Exception if extraction fails
   */
  List<Relation> extractRelations(String text) throws Exception;

  /**
   * Extract relations from multiple texts.
   *
   * @param texts The input texts to extract relations from
   * @return A list of relations extracted from all texts
   * @throws Exception if extraction fails
   */
  List<Relation> extractRelationsBatch(String[] texts) throws Exception;

  /**
   * Get the supported relation types.
   *
   * @return A list of relation type labels
   */
  List<String> getSupportedRelationTypes();

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
