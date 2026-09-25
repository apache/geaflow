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
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import org.apache.geaflow.context.nlp.entity.Entity;
import org.apache.geaflow.context.nlp.rules.ExtractionRule;
import org.apache.geaflow.context.nlp.rules.RuleManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configurable entity extractor using rule-based NER.
 * Rules are loaded from external configuration files.
 */
public class DefaultEntityExtractor implements EntityExtractor {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultEntityExtractor.class);
  private static final String MODEL_NAME = "configurable-rule-based";
  private static final String DEFAULT_CONFIG_PATH = "rules/entity-patterns.properties";

  private final RuleManager ruleManager;
  private final String configPath;

  public DefaultEntityExtractor() {
    this(DEFAULT_CONFIG_PATH);
  }

  public DefaultEntityExtractor(String configPath) {
    this.configPath = configPath;
    this.ruleManager = new RuleManager();
  }

  @Override
  public void initialize() throws Exception {
    LOGGER.info("Initializing configurable entity extractor from: {}", configPath);
    ruleManager.loadEntityRules(configPath);
    LOGGER.info("Loaded {} entity types", ruleManager.getSupportedEntityTypes().size());
  }

  @Override
  public List<Entity> extractEntities(String text) throws Exception {
    if (text == null || text.isEmpty()) {
      return new ArrayList<>();
    }

    List<Entity> entities = new ArrayList<>();

    for (ExtractionRule rule : ruleManager.getAllEntityRules()) {
      entities.addAll(extractEntityByRule(text, rule));
    }

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

  @Override
  public List<Entity> extractEntitiesBatch(String[] texts) throws Exception {
    List<Entity> allEntities = new ArrayList<>();
    for (String text : texts) {
      allEntities.addAll(extractEntities(text));
    }
    return allEntities;
  }

  @Override
  public List<String> getSupportedEntityTypes() {
    return ruleManager.getSupportedEntityTypes();
  }

  @Override
  public String getModelName() {
    return MODEL_NAME;
  }

  @Override
  public void close() throws Exception {
    LOGGER.info("Closing configurable entity extractor");
  }

  private List<Entity> extractEntityByRule(String text, ExtractionRule rule) {
    List<Entity> entities = new ArrayList<>();
    Matcher matcher = rule.getPattern().matcher(text);

    while (matcher.find()) {
      String matchedText = matcher.group();
      int startOffset = matcher.start();
      int endOffset = matcher.end();

      Entity entity = new Entity();
      entity.setText(matchedText.trim());
      entity.setType(rule.getType());
      entity.setStartOffset(startOffset);
      entity.setEndOffset(endOffset);
      entity.setConfidence(rule.getConfidence());

      entities.add(entity);
    }

    return entities;
  }
}
