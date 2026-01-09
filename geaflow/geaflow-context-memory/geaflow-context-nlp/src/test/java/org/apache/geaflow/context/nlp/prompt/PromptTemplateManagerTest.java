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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test cases for PromptTemplateManager.
 */
public class PromptTemplateManagerTest {

  private PromptTemplateManager manager;

  /**
   * Setup test fixtures.
   */
  @Before
  public void setUp() {
    manager = new PromptTemplateManager();
  }

  /**
   * Test template registration and retrieval.
   */
  @Test
  public void testRegisterAndGetTemplate() {
    String templateId = "test_template";
    String template = "This is a test template with {placeholder}";

    manager.registerTemplate(templateId, template);
    String retrieved = manager.getTemplate(templateId);

    Assert.assertNotNull(retrieved);
    Assert.assertEquals(template, retrieved);
  }

  /**
   * Test template rendering.
   */
  @Test
  public void testRenderTemplate() {
    String templateId = "greeting";
    String template = "Hello {name}, welcome to {place}!";

    manager.registerTemplate(templateId, template);

    Map<String, String> variables = new HashMap<>();
    variables.put("name", "John");
    variables.put("place", "GeaFlow");

    String rendered = manager.renderTemplate(templateId, variables);
    Assert.assertEquals("Hello John, welcome to GeaFlow!", rendered);
  }

  /**
   * Test default templates are loaded.
   */
  @Test
  public void testDefaultTemplatesLoaded() {
    String[] templates = manager.listTemplates();
    Assert.assertNotNull(templates);
    Assert.assertTrue(templates.length > 0);
  }

  /**
   * Test retrieving a default template.
   */
  @Test
  public void testGetDefaultTemplate() {
    String template = manager.getTemplate("entity_extraction");
    Assert.assertNotNull(template);
    Assert.assertTrue(template.contains("Extract"));
  }

  /**
   * Test template not found.
   */
  @Test
  public void testTemplateNotFound() {
    String template = manager.getTemplate("nonexistent_template");
    Assert.assertNull(template);
  }

  /**
   * Test rendering nonexistent template fails.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testRenderNonexistentTemplate() {
    Map<String, String> variables = new HashMap<>();
    manager.renderTemplate("nonexistent", variables);
  }

  /**
   * Test optimizer registration and optimization.
   */
  @Test
  public void testOptimizerRegistration() {
    String templateId = "optimize_test";
    manager.registerTemplate(templateId, "Original prompt");

    manager.registerOptimizer(templateId, prompt -> prompt.toUpperCase());

    String optimized = manager.optimizePrompt(templateId, "test prompt");
    Assert.assertEquals("TEST PROMPT", optimized);
  }

  /**
   * Test list templates returns all registered templates.
   */
  @Test
  public void testListTemplates() {
    String[] templates = manager.listTemplates();
    Assert.assertNotNull(templates);
    Assert.assertTrue(templates.length > 0);

    // Should contain at least the default templates
    boolean hasEntity = false;
    for (String t : templates) {
      if ("entity_extraction".equals(t)) {
        hasEntity = true;
        break;
      }
    }
    Assert.assertTrue(hasEntity);
  }
}
