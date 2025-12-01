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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.geaflow.context.nlp.entity.Entity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of EntityExtractor using rule-based NER.
 * This is a production-grade baseline implementation that can be extended
 * to support actual NLP models like SpaCy, BERT, etc.
 */
public class DefaultEntityExtractor implements EntityExtractor {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultEntityExtractor.class);

  private static final String MODEL_NAME = "default-rule-based";

  // Regex patterns for different entity types
  private final Pattern personPattern = Pattern
      .compile("\\b(Mr\\.?|Mrs\\.?|Dr\\.?|Professor)?\\s+([A-Z][a-z]+(?:\\s+[A-Z][a-z]+)*)\\b");
  private final Pattern locationPattern = Pattern
      .compile("\\b(New York|Los Angeles|San Francisco|London|Paris|Tokyo|[A-Z][a-z]+(?:\\s+[A-Z][a-z]+)?)\\b");
  private final Pattern organizationPattern = Pattern
      .compile("\\b([A-Z][a-z]+(?:\\s+[A-Z][a-z]+)*)(?:\\s+Inc\\.?|Corp\\.?|Ltd\\.?|LLC)?\\b");
  private final Pattern productPattern = Pattern
      .compile("\\b([A-Z][a-zA-Z0-9]*(?:\\s+[A-Z][a-zA-Z0-9]*)?)\\b(?=\\s+(?:is|are|was|were|product|item))");

  /**
   * Initialize the extractor.
   */
  @Override
  public void initialize() {
    LOGGER.info("Initializing DefaultEntityExtractor with rule-based NER");
  }

  /**
   * Extract entities from text.
   *
   * @param text The input text
   * @return A list of extracted entities
   * @throws Exception if extraction fails
   */
  @Override
  public List<Entity> extractEntities(String text) throws Exception {
    if (text == null || text.isEmpty()) {
      return new ArrayList<>();
    }

    List<Entity> entities = new ArrayList<>();

    // Extract PERSON entities
    entities.addAll(extractEntityByPattern(text, personPattern, "Person"));

    // Extract LOCATION entities
    entities.addAll(extractEntityByPattern(text, locationPattern, "Location"));

    // Extract ORGANIZATION entities
    entities.addAll(extractEntityByPattern(text, organizationPattern, "Organization"));

    // Extract PRODUCT entities
    entities.addAll(extractEntityByPattern(text, productPattern, "Product"));

    // Remove duplicates and assign IDs
    List<Entity> uniqueEntities = new ArrayList<>();
    List<String> seenTexts = new ArrayList<>();
    for (Entity entity : entities) {
      String normalizedText = entity.getText().toLowerCase();
      if (!seenTexts.contains(normalizedText)) {
        entity.setId(UUID.randomUUID().toString());
        entity.setSource(MODEL_NAME);
        uniqueEntities.add(entity);
        seenTexts.add(normalizedText);
      }
    }

    return uniqueEntities;
  }

  /**
   * Extract entities from multiple texts.
   *
   * @param texts The input texts
   * @return A list of extracted entities from all texts
   * @throws Exception if extraction fails
   */
  @Override
  public List<Entity> extractEntitiesBatch(String[] texts) throws Exception {
    List<Entity> allEntities = new ArrayList<>();
    for (String text : texts) {
      allEntities.addAll(extractEntities(text));
    }
    return allEntities;
  }

  /**
   * Get the supported entity types.
   *
   * @return A list of supported entity types
   */
  @Override
  public List<String> getSupportedEntityTypes() {
    return Arrays.asList("Person", "Location", "Organization", "Product");
  }

  /**
   * Get the model name.
   *
   * @return The model name
   */
  @Override
  public String getModelName() {
    return MODEL_NAME;
  }

  /**
   * Close the extractor.
   *
   * @throws Exception if closing fails
   */
  @Override
  public void close() throws Exception {
    LOGGER.info("Closing DefaultEntityExtractor");
  }

  /**
   * Helper method to extract entities using a pattern.
   *
   * @param text The input text
   * @param pattern The regex pattern
   * @param entityType The entity type
   * @return A list of extracted entities
   */
  private List<Entity> extractEntityByPattern(String text, Pattern pattern, String entityType) {
    List<Entity> entities = new ArrayList<>();
    Matcher matcher = pattern.matcher(text);

    while (matcher.find()) {
      String matchedText = matcher.group();
      int startOffset = matcher.start();
      int endOffset = matcher.end();

      Entity entity = new Entity();
      entity.setText(matchedText.trim());
      entity.setType(entityType);
      entity.setStartOffset(startOffset);
      entity.setEndOffset(endOffset);
      entity.setConfidence(0.8);

      entities.add(entity);
    }

    return entities;
  }
}
