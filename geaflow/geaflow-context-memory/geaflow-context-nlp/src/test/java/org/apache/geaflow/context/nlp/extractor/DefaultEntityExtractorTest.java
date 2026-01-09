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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test cases for DefaultEntityExtractor.
 */
public class DefaultEntityExtractorTest {

  private EntityExtractor extractor;

  /**
   * Setup test fixtures.
   *
   * @throws Exception if setup fails
   */
  @Before
  public void setUp() throws Exception {
    extractor = new DefaultEntityExtractor();
    extractor.initialize();
  }

  /**
   * Test entity extraction from text.
   *
   * @throws Exception if test fails
   */
  @Test
  public void testExtractEntities() throws Exception {
    String text = "John Smith works for Apple in San Francisco.";
    List<Entity> entities = extractor.extractEntities(text);

    Assert.assertNotNull(entities);
    Assert.assertTrue(entities.size() > 0);

    // Check that entities have required fields
    for (Entity entity : entities) {
      Assert.assertNotNull(entity.getText());
      Assert.assertNotNull(entity.getType());
      Assert.assertNotNull(entity.getId());
      Assert.assertTrue(entity.getConfidence() > 0);
    }
  }

  /**
   * Test empty text handling.
   *
   * @throws Exception if test fails
   */
  @Test
  public void testEmptyText() throws Exception {
    List<Entity> entities = extractor.extractEntities("");
    Assert.assertNotNull(entities);
    Assert.assertEquals(0, entities.size());
  }

  /**
   * Test null text handling.
   *
   * @throws Exception if test fails
   */
  @Test
  public void testNullText() throws Exception {
    List<Entity> entities = extractor.extractEntities(null);
    Assert.assertNotNull(entities);
    Assert.assertEquals(0, entities.size());
  }

  /**
   * Test batch extraction.
   *
   * @throws Exception if test fails
   */
  @Test
  public void testBatchExtraction() throws Exception {
    String[] texts = {
        "Apple is a technology company.",
        "London is located in England.",
        "Dr. Smith works at Google."
    };

    List<Entity> allEntities = extractor.extractEntitiesBatch(texts);
    Assert.assertNotNull(allEntities);
    Assert.assertTrue(allEntities.size() > 0);
  }

  /**
   * Test supported entity types.
   *
   * @throws Exception if test fails
   */
  @Test
  public void testSupportedEntityTypes() throws Exception {
    List<String> types = extractor.getSupportedEntityTypes();
    Assert.assertNotNull(types);
    Assert.assertTrue(types.size() > 0);
    Assert.assertTrue(types.contains("Person"));
    Assert.assertTrue(types.contains("Organization"));
    Assert.assertTrue(types.contains("Location"));
  }

  /**
   * Test model name.
   *
   * @throws Exception if test fails
   */
  @Test
  public void testModelName() throws Exception {
    String modelName = extractor.getModelName();
    Assert.assertNotNull(modelName);
    Assert.assertFalse(modelName.isEmpty());
  }

  /**
   * Cleanup test fixtures.
   *
   * @throws Exception if cleanup fails
   */
  @Test
  public void testClose() throws Exception {
    extractor.close();
    // Should complete without error
  }
}
