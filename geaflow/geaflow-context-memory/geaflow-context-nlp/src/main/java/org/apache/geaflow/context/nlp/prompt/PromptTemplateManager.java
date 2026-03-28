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

package org.apache.geaflow.context.nlp.prompt;

import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manager for prompt templates used in NLP/LLM operations.
 * Supports template registration, variable substitution, and optimization.
 */
public class PromptTemplateManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(PromptTemplateManager.class);

  private final Map<String, String> templates;
  private final Map<String, PromptOptimizer> optimizers;

  /**
   * Constructor to create the manager.
   */
  public PromptTemplateManager() {
    this.templates = new HashMap<>();
    this.optimizers = new HashMap<>();
    loadDefaultTemplates();
  }

  /**
   * Register a new prompt template.
   *
   * @param templateId The unique template ID
   * @param template The template string with placeholders {var}
   */
  public void registerTemplate(String templateId, String template) {
    templates.put(templateId, template);
    LOGGER.debug("Registered template: {}", templateId);
  }

  /**
   * Get a template by ID.
   *
   * @param templateId The template ID
   * @return The template string, or null if not found
   */
  public String getTemplate(String templateId) {
    return templates.get(templateId);
  }

  /**
   * Render a template by substituting variables.
   *
   * @param templateId The template ID
   * @param variables The variables to substitute
   * @return The rendered prompt
   */
  public String renderTemplate(String templateId, Map<String, String> variables) {
    String template = templates.get(templateId);
    if (template == null) {
      throw new IllegalArgumentException("Template not found: " + templateId);
    }

    String result = template;
    for (Map.Entry<String, String> entry : variables.entrySet()) {
      result = result.replace("{" + entry.getKey() + "}", entry.getValue());
    }

    return result;
  }

  /**
   * Optimize a prompt using registered optimizers.
   *
   * @param templateId The template ID
   * @param prompt The original prompt
   * @return The optimized prompt
   */
  public String optimizePrompt(String templateId, String prompt) {
    PromptOptimizer optimizer = optimizers.get(templateId);
    if (optimizer != null) {
      return optimizer.optimize(prompt);
    }
    return prompt;
  }

  /**
   * List all available templates.
   *
   * @return Array of template IDs
   */
  public String[] listTemplates() {
    return templates.keySet().toArray(new String[0]);
  }

  /**
   * Load default templates.
   */
  private void loadDefaultTemplates() {
    // Entity extraction template
    registerTemplate("entity_extraction",
        "Extract named entities from the following text. "
            + "Identify entities of types: Person, Organization, Location, Product.\n"
            + "Text: {text}\n"
            + "Output format: entity_type: entity_text (confidence)\n"
            + "Entities:");

    // Relation extraction template
    registerTemplate("relation_extraction",
        "Extract relationships between entities from the following text.\n"
            + "Text: {text}\n"
            + "Output format: subject -> relation_type -> object (confidence)\n"
            + "Relations:");

    // Entity linking template
    registerTemplate("entity_linking",
        "Link the following entities to their canonical forms in the knowledge base.\n"
            + "Entities: {entities}\n"
            + "Output format: extracted_entity -> canonical_form\n"
            + "Linked entities:");

    // Knowledge graph construction template
    registerTemplate("knowledge_graph",
        "Construct a knowledge graph from the following text. "
            + "Identify entities and their relationships.\n"
            + "Text: {text}\n"
            + "Output format: (entity1:type1) -[relationship]-> (entity2:type2)\n"
            + "Knowledge graph:");

    // Question answering template
    registerTemplate("question_answering",
        "Answer the following question based on the provided context.\n"
            + "Context: {context}\n"
            + "Question: {question}\n"
            + "Answer:");

    // Classification template
    registerTemplate("classification",
        "Classify the following text into one of these categories: {categories}\n"
            + "Text: {text}\n"
            + "Classification:");

    LOGGER.info("Loaded {} default templates", templates.size());
  }

  /**
   * Interface for prompt optimization strategies.
   */
  @FunctionalInterface
  public interface PromptOptimizer {

    /**
     * Optimize a prompt.
     *
     * @param prompt The original prompt
     * @return The optimized prompt
     */
    String optimize(String prompt);
  }

  /**
   * Register a prompt optimizer for a template.
   *
   * @param templateId The template ID
   * @param optimizer The optimizer function
   */
  public void registerOptimizer(String templateId, PromptOptimizer optimizer) {
    optimizers.put(templateId, optimizer);
    LOGGER.debug("Registered optimizer for template: {}", templateId);
  }
}
