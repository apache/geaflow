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

package org.apache.geaflow.context.nlp.rules;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages extraction rules loaded from configuration files.
 */
public class RuleManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(RuleManager.class);

  private final Map<String, List<ExtractionRule>> entityRules = new HashMap<>();
  private final Map<String, ExtractionRule> relationRules = new HashMap<>();
  private final List<String> supportedEntityTypes = new ArrayList<>();
  private final List<String> supportedRelationTypes = new ArrayList<>();

  public void loadEntityRules(String configPath) throws Exception {
    LOGGER.info("Loading entity rules from: {}", configPath);

    Properties props = new Properties();
    try (InputStream is = getClass().getClassLoader().getResourceAsStream(configPath)) {
      if (is == null) {
        throw new IllegalArgumentException("Config file not found: " + configPath);
      }
      props.load(is);
    }

    String typesStr = props.getProperty("entity.types");
    if (typesStr != null) {
      for (String type : typesStr.split(",")) {
        String normalizedType = type.trim();
        supportedEntityTypes.add(normalizedType);
        entityRules.put(normalizedType, new ArrayList<>());
      }
    }

    for (String type : supportedEntityTypes) {
      String typeKey = "entity." + type.toLowerCase();
      double confidence = Double.parseDouble(
          props.getProperty(typeKey + ".confidence", "0.75"));

      int patternNum = 1;
      while (true) {
        String patternKey = typeKey + ".pattern." + patternNum;
        String pattern = props.getProperty(patternKey);
        if (pattern == null) {
          break;
        }

        ExtractionRule rule = new ExtractionRule(type, pattern, confidence, patternNum);
        entityRules.get(type).add(rule);
        patternNum++;
      }
    }

    LOGGER.info("Loaded {} entity types with {} total patterns",
        supportedEntityTypes.size(),
        entityRules.values().stream().mapToInt(List::size).sum());
  }

  public void loadRelationRules(String configPath) throws Exception {
    LOGGER.info("Loading relation rules from: {}", configPath);

    Properties props = new Properties();
    try (InputStream is = getClass().getClassLoader().getResourceAsStream(configPath)) {
      if (is == null) {
        throw new IllegalArgumentException("Config file not found: " + configPath);
      }
      props.load(is);
    }

    String typesStr = props.getProperty("relation.types");
    if (typesStr != null) {
      for (String type : typesStr.split(",")) {
        supportedRelationTypes.add(type.trim());
      }
    }

    for (String type : supportedRelationTypes) {
      String typeKey = "relation." + type;
      String pattern = props.getProperty(typeKey + ".pattern");
      double confidence = Double.parseDouble(
          props.getProperty(typeKey + ".confidence", "0.75"));

      if (pattern != null) {
        ExtractionRule rule = new ExtractionRule(type, pattern, confidence);
        relationRules.put(type, rule);
      }
    }

    LOGGER.info("Loaded {} relation types", supportedRelationTypes.size());
  }

  public List<ExtractionRule> getEntityRules(String entityType) {
    return entityRules.getOrDefault(entityType, new ArrayList<>());
  }

  public List<ExtractionRule> getAllEntityRules() {
    List<ExtractionRule> allRules = new ArrayList<>();
    for (List<ExtractionRule> rules : entityRules.values()) {
      allRules.addAll(rules);
    }
    return allRules;
  }

  public Map<String, ExtractionRule> getRelationRules() {
    return new HashMap<>(relationRules);
  }

  public List<String> getSupportedEntityTypes() {
    return new ArrayList<>(supportedEntityTypes);
  }

  public List<String> getSupportedRelationTypes() {
    return new ArrayList<>(supportedRelationTypes);
  }

  public void addEntityRule(String type, ExtractionRule rule) {
    entityRules.computeIfAbsent(type, k -> new ArrayList<>()).add(rule);
    if (!supportedEntityTypes.contains(type)) {
      supportedEntityTypes.add(type);
    }
  }

  public void addRelationRule(String type, ExtractionRule rule) {
    relationRules.put(type, rule);
    if (!supportedRelationTypes.contains(type)) {
      supportedRelationTypes.add(type);
    }
  }

  public void removeEntityRule(String type) {
    entityRules.remove(type);
    supportedEntityTypes.remove(type);
  }

  public void removeRelationRule(String type) {
    relationRules.remove(type);
    supportedRelationTypes.remove(type);
  }
}
