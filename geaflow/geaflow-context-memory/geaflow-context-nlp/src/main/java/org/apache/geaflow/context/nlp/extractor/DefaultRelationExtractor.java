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
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import org.apache.geaflow.context.nlp.entity.Relation;
import org.apache.geaflow.context.nlp.rules.ExtractionRule;
import org.apache.geaflow.context.nlp.rules.RuleManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configurable relation extractor using rule-based patterns.
 * Rules are loaded from external configuration files.
 */
public class DefaultRelationExtractor implements RelationExtractor {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultRelationExtractor.class);
  private static final String MODEL_NAME = "configurable-rule-based";
  private static final String DEFAULT_CONFIG_PATH = "rules/relation-patterns.properties";

  private final RuleManager ruleManager;
  private final String configPath;

  public DefaultRelationExtractor() {
    this(DEFAULT_CONFIG_PATH);
  }

  public DefaultRelationExtractor(String configPath) {
    this.configPath = configPath;
    this.ruleManager = new RuleManager();
  }

  @Override
  public void initialize() throws Exception {
    LOGGER.info("Initializing configurable relation extractor from: {}", configPath);
    ruleManager.loadRelationRules(configPath);
    LOGGER.info("Loaded {} relation types", ruleManager.getSupportedRelationTypes().size());
  }

  @Override
  public List<Relation> extractRelations(String text) throws Exception {
    if (text == null || text.isEmpty()) {
      return new ArrayList<>();
    }

    List<Relation> relations = new ArrayList<>();

    for (Map.Entry<String, ExtractionRule> entry : ruleManager.getRelationRules().entrySet()) {
      String relationType = entry.getKey();
      ExtractionRule rule = entry.getValue();

      Matcher matcher = rule.getPattern().matcher(text);
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
            relation.setConfidence(rule.getConfidence());
            relation.setRelationName(relationType);

            relations.add(relation);
          }
        }
      }
    }

    return relations;
  }

  @Override
  public List<Relation> extractRelationsBatch(String[] texts) throws Exception {
    List<Relation> allRelations = new ArrayList<>();
    for (String text : texts) {
      allRelations.addAll(extractRelations(text));
    }
    return allRelations;
  }

  @Override
  public List<String> getSupportedRelationTypes() {
    return ruleManager.getSupportedRelationTypes();
  }

  @Override
  public String getModelName() {
    return MODEL_NAME;
  }

  @Override
  public void close() throws Exception {
    LOGGER.info("Closing configurable relation extractor");
  }
}
