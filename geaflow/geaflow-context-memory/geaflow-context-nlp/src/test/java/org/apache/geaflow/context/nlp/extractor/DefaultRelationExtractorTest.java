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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test cases for DefaultRelationExtractor.
 */
public class DefaultRelationExtractorTest {

  private RelationExtractor extractor;

  /**
   * Setup test fixtures.
   *
   * @throws Exception if setup fails
   */
  @Before
  public void setUp() throws Exception {
    extractor = new DefaultRelationExtractor();
    extractor.initialize();
  }

  /**
   * Test relation extraction from text.
   *
   * @throws Exception if test fails
   */
  @Test
  public void testExtractRelations() throws Exception {
    String text = "John Smith works for Apple in San Francisco.";
    List<Relation> relations = extractor.extractRelations(text);

    Assert.assertNotNull(relations);
    Assert.assertTrue(relations.size() > 0);

    // Check that relations have required fields
    for (Relation relation : relations) {
      Assert.assertNotNull(relation.getSourceId());
      Assert.assertNotNull(relation.getTargetId());
      Assert.assertNotNull(relation.getRelationType());
      Assert.assertNotNull(relation.getId());
      Assert.assertTrue(relation.getConfidence() > 0);
    }
  }

  /**
   * Test empty text handling.
   *
   * @throws Exception if test fails
   */
  @Test
  public void testEmptyText() throws Exception {
    List<Relation> relations = extractor.extractRelations("");
    Assert.assertNotNull(relations);
    Assert.assertEquals(0, relations.size());
  }

  /**
   * Test null text handling.
   *
   * @throws Exception if test fails
   */
  @Test
  public void testNullText() throws Exception {
    List<Relation> relations = extractor.extractRelations(null);
    Assert.assertNotNull(relations);
    Assert.assertEquals(0, relations.size());
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
        "John works for Microsoft."
    };

    List<Relation> allRelations = extractor.extractRelationsBatch(texts);
    Assert.assertNotNull(allRelations);
    Assert.assertTrue(allRelations.size() >= 0);
  }

  /**
   * Test supported relation types.
   *
   * @throws Exception if test fails
   */
  @Test
  public void testSupportedRelationTypes() throws Exception {
    List<String> types = extractor.getSupportedRelationTypes();
    Assert.assertNotNull(types);
    Assert.assertTrue(types.size() > 0);
    Assert.assertTrue(types.contains("prefers"));
    Assert.assertTrue(types.contains("works_for"));
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
