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

package org.apache.geaflow.context.nlp.linker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.geaflow.context.nlp.entity.Entity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of EntityLinker for entity disambiguation and deduplication.
 * Merges similar entities and links them to canonical forms in the knowledge base.
 */
public class DefaultEntityLinker implements EntityLinker {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultEntityLinker.class);

  private final Map<String, String> entityCanonicalForms;
  private final Map<String, Entity> knowledgeBase;
  private final double similarityThreshold;

  /**
   * Constructor with default similarity threshold.
   */
  public DefaultEntityLinker() {
    this(0.85);
  }

  /**
   * Constructor with custom similarity threshold.
   *
   * @param similarityThreshold The similarity threshold for entity merging
   */
  public DefaultEntityLinker(double similarityThreshold) {
    this.entityCanonicalForms = new HashMap<>();
    this.knowledgeBase = new HashMap<>();
    this.similarityThreshold = similarityThreshold;
  }

  /**
   * Initialize the linker.
   */
  @Override
  public void initialize() {
    LOGGER.info("Initializing DefaultEntityLinker with similarity threshold: " + similarityThreshold);
    // Load common entities from knowledge base (in production, load from external KB)
    loadCommonEntities();
  }

  /**
   * Link entities to canonical forms and merge duplicates.
   *
   * @param entities The extracted entities
   * @return A list of linked and deduplicated entities
   * @throws Exception if linking fails
   */
  @Override
  public List<Entity> linkEntities(List<Entity> entities) throws Exception {
    Map<String, Entity> linkedEntities = new HashMap<>();

    for (Entity entity : entities) {
      // Try to find canonical form in knowledge base
      String canonicalId = findCanonicalForm(entity);

      if (canonicalId != null) {
        // Entity found in knowledge base, update it
        Entity kbEntity = knowledgeBase.get(canonicalId);
        if (!linkedEntities.containsKey(canonicalId)) {
          linkedEntities.put(canonicalId, kbEntity);
        } else {
          // Merge with existing entity
          Entity existing = linkedEntities.get(canonicalId);
          mergeEntities(existing, entity);
        }
      } else {
        // Entity not in KB, try to find similar entities in current list
        String mergedKey = findSimilarEntity(linkedEntities, entity);
        if (mergedKey != null) {
          Entity similar = linkedEntities.get(mergedKey);
          mergeEntities(similar, entity);
        } else {
          // New entity
          linkedEntities.put(entity.getId(), entity);
        }
      }
    }

    return new ArrayList<>(linkedEntities.values());
  }

  /**
   * Get the similarity score between two entities.
   *
   * @param entity1 The first entity
   * @param entity2 The second entity
   * @return The similarity score (0.0 to 1.0)
   */
  @Override
  public double getEntitySimilarity(Entity entity1, Entity entity2) {
    // Type must match
    if (!entity1.getType().equals(entity2.getType())) {
      return 0.0;
    }

    // Calculate string similarity using Jaro-Winkler
    return jaroWinklerSimilarity(entity1.getText(), entity2.getText());
  }

  /**
   * Close the linker.
   *
   * @throws Exception if closing fails
   */
  @Override
  public void close() throws Exception {
    LOGGER.info("Closing DefaultEntityLinker");
  }

  /**
   * Load common entities from knowledge base.
   */
  private void loadCommonEntities() {
    // In production, this would load from external knowledge base
    // For now, load some common entities
    Entity e1 = new Entity();
    e1.setId("person-kendra");
    e1.setText("Kendra");
    e1.setType("Person");
    knowledgeBase.put("person-kendra", e1);

    Entity e2 = new Entity();
    e2.setId("org-apple");
    e2.setText("Apple");
    e2.setType("Organization");
    knowledgeBase.put("org-apple", e2);

    Entity e3 = new Entity();
    e3.setId("org-google");
    e3.setText("Google");
    e3.setType("Organization");
    knowledgeBase.put("org-google", e3);

    Entity e4 = new Entity();
    e4.setId("loc-new-york");
    e4.setText("New York");
    e4.setType("Location");
    knowledgeBase.put("loc-new-york", e4);

    Entity e5 = new Entity();
    e5.setId("loc-london");
    e5.setText("London");
    e5.setType("Location");
    knowledgeBase.put("loc-london", e5);
  }

  /**
   * Find canonical form for an entity in knowledge base.
   *
   * @param entity The entity to find
   * @return The canonical ID if found, null otherwise
   */
  private String findCanonicalForm(Entity entity) {
    for (Map.Entry<String, Entity> entry : knowledgeBase.entrySet()) {
      double similarity = getEntitySimilarity(entity, entry.getValue());
      if (similarity >= similarityThreshold) {
        entityCanonicalForms.put(entity.getId(), entry.getKey());
        return entry.getKey();
      }
    }
    return null;
  }

  /**
   * Find similar entity in the current linked entities map.
   *
   * @param linkedEntities The linked entities
   * @param entity The entity to find similar for
   * @return The key of the similar entity if found, null otherwise
   */
  private String findSimilarEntity(Map<String, Entity> linkedEntities, Entity entity) {
    for (Map.Entry<String, Entity> entry : linkedEntities.entrySet()) {
      double similarity = getEntitySimilarity(entity, entry.getValue());
      if (similarity >= similarityThreshold) {
        return entry.getKey();
      }
    }
    return null;
  }

  /**
   * Merge two entities, keeping the higher confidence one.
   *
   * @param target The target entity to merge into
   * @param source The source entity to merge from
   */
  private void mergeEntities(Entity target, Entity source) {
    // Keep higher confidence
    if (source.getConfidence() > target.getConfidence()) {
      target.setConfidence(source.getConfidence());
      target.setText(source.getText());
    }

    // Update confidence as average
    double avgConfidence = (target.getConfidence() + source.getConfidence()) / 2.0;
    target.setConfidence(avgConfidence);
  }

  /**
   * Calculate Jaro-Winkler similarity between two strings.
   *
   * @param str1 First string
   * @param str2 Second string
   * @return Similarity score between 0.0 and 1.0
   */
  private double jaroWinklerSimilarity(String str1, String str2) {
    str1 = str1.toLowerCase();
    str2 = str2.toLowerCase();

    int len1 = str1.length();
    int len2 = str2.length();

    if (len1 == 0 && len2 == 0) {
      return 1.0;
    }
    if (len1 == 0 || len2 == 0) {
      return 0.0;
    }

    // Calculate Jaro similarity
    int matchDistance = Math.max(len1, len2) / 2 - 1;
    matchDistance = Math.max(0, matchDistance);

    boolean[] str1Matches = new boolean[len1];
    boolean[] str2Matches = new boolean[len2];

    int matches = 0;
    int transpositions = 0;

    // Identify matches
    for (int i = 0; i < len1; i++) {
      int start = Math.max(0, i - matchDistance);
      int end = Math.min(i + matchDistance + 1, len2);

      for (int j = start; j < end; j++) {
        if (str2Matches[j] || str1.charAt(i) != str2.charAt(j)) {
          continue;
        }
        str1Matches[i] = true;
        str2Matches[j] = true;
        matches++;
        break;
      }
    }

    if (matches == 0) {
      return 0.0;
    }

    // Count transpositions
    int k = 0;
    for (int i = 0; i < len1; i++) {
      if (!str1Matches[i]) {
        continue;
      }
      while (!str2Matches[k]) {
        k++;
      }
      if (str1.charAt(i) != str2.charAt(k)) {
        transpositions++;
      }
      k++;
    }

    double jaro = (matches / (double) len1
        + matches / (double) len2
        + (matches - transpositions / 2.0) / matches) / 3.0;

    // Apply Winkler modification for prefix
    int prefixLen = 0;
    for (int i = 0; i < Math.min(len1, len2) && i < 4; i++) {
      if (str1.charAt(i) == str2.charAt(i)) {
        prefixLen++;
      } else {
        break;
      }
    }

    return jaro + prefixLen * 0.1 * (1.0 - jaro);
  }
}
