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
import org.apache.geaflow.context.nlp.entity.Relation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of RelationExtractor using rule-based patterns.
 * This is a production-grade baseline implementation that can be extended
 * to support actual RE models like OpenIE, REBEL, etc.
 */
public class DefaultRelationExtractor implements RelationExtractor {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultRelationExtractor.class);

  private static final String MODEL_NAME = "default-rule-based";

  // Relation patterns: (entity1) RELATION (entity2)
  private static final Pattern[] RELATION_PATTERNS = {
      // Pattern for "X prefers Y"
      Pattern.compile("([A-Z][a-z]+(?:\\s+[A-Z][a-z]+)*)\\s+(?:prefers|likes|loves)\\s+([A-Z][a-z]+(?:\\s+[A-Z][a-z]+)*)"),
      // Pattern for "X works for Y"
      Pattern.compile("([A-Z][a-z]+(?:\\s+[A-Z][a-z]+)*)\\s+(?:works\\s+for|works\\s+at|employed\\s+by)\\s+([A-Z][a-z]+(?:\\s+[A-Z][a-z]+)*)"),
      // Pattern for "X is a Y"
      Pattern.compile("([A-Z][a-z]+(?:\\s+[A-Z][a-z]+)*)\\s+is\\s+(?:a|an|the)\\s+([A-Z][a-z]+(?:\\s+[A-Z][a-z]+)*)"),
      // Pattern for "X located in Y"
      Pattern.compile("([A-Z][a-z]+(?:\\s+[A-Z][a-z]+)*)\\s+(?:is\\s+)?located\\s+(?:in|at)\\s+([A-Z][a-z]+(?:\\s+[A-Z][a-z]+)*)"),
      // Pattern for "X competes with Y"
      Pattern.compile("([A-Z][a-z]+(?:\\s+[A-Z][a-z]+)*)\\s+(?:competes\\s+with|rivals|compete\\s+against)\\s+([A-Z][a-z]+(?:\\s+[A-Z][a-z]+)*)"),
      // Pattern for "X founded by Y"
      Pattern.compile("([A-Z][a-z]+(?:\\s+[A-Z][a-z]+)*)\\s+(?:was\\s+)?founded\\s+(?:by|in)\\s+([A-Z][a-z]+(?:\\s+[A-Z][a-z]+)*)"),
  };

  /**
   * Initialize the extractor.
   */
  @Override
  public void initialize() {
    LOGGER.info("Initializing DefaultRelationExtractor with rule-based RE");
  }

  /**
   * Extract relations from text.
   *
   * @param text The input text
   * @return A list of extracted relations
   * @throws Exception if extraction fails
   */
  @Override
  public List<Relation> extractRelations(String text) throws Exception {
    if (text == null || text.isEmpty()) {
      return new ArrayList<>();
    }

    List<Relation> relations = new ArrayList<>();

    // Try each relation pattern
    for (int i = 0; i < RELATION_PATTERNS.length; i++) {
      Pattern pattern = RELATION_PATTERNS[i];
      String relationType = getRelationTypeForPattern(i);

      Matcher matcher = pattern.matcher(text);
      while (matcher.find()) {
        if (matcher.groupCount() >= 2) {
          String sourceEntity = matcher.group(1).trim();
          String targetEntity = matcher.group(2).trim();

          if (!sourceEntity.isEmpty() && !targetEntity.isEmpty()) {
            Relation relation = new Relation();
            relation.setSourceId(sourceEntity);
            relation.setTargetId(targetEntity);
            relation.setRelationType(relationType);
            relation.setId(UUID.randomUUID().toString());
            relation.setSource(MODEL_NAME);
            relation.setConfidence(0.75);
            relation.setRelationName(relationType);

            relations.add(relation);
          }
        }
      }
    }

    return relations;
  }

  /**
   * Extract relations from multiple texts.
   *
   * @param texts The input texts
   * @return A list of extracted relations from all texts
   * @throws Exception if extraction fails
   */
  @Override
  public List<Relation> extractRelationsBatch(String[] texts) throws Exception {
    List<Relation> allRelations = new ArrayList<>();
    for (String text : texts) {
      allRelations.addAll(extractRelations(text));
    }
    return allRelations;
  }

  /**
   * Get the supported relation types.
   *
   * @return A list of supported relation types
   */
  @Override
  public List<String> getSupportedRelationTypes() {
    return Arrays.asList(
        "prefers",
        "works_for",
        "is_a",
        "located_in",
        "competes_with",
        "founded_by"
    );
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
    LOGGER.info("Closing DefaultRelationExtractor");
  }

  /**
   * Get the relation type for a given pattern index.
   *
   * @param patternIndex The index of the pattern
   * @return The relation type
   */
  private String getRelationTypeForPattern(int patternIndex) {
    switch (patternIndex) {
      case 0:
        return "prefers";
      case 1:
        return "works_for";
      case 2:
        return "is_a";
      case 3:
        return "located_in";
      case 4:
        return "competes_with";
      case 5:
        return "founded_by";
      default:
        return "unknown";
    }
  }
}
