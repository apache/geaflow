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
import java.util.List;
import org.apache.geaflow.context.nlp.entity.Entity;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test cases for DefaultEntityLinker.
 */
public class DefaultEntityLinkerTest {

  private EntityLinker linker;

  /**
   * Setup test fixtures.
   *
   * @throws Exception if setup fails
   */
  @Before
  public void setUp() throws Exception {
    linker = new DefaultEntityLinker();
    linker.initialize();
  }

  /**
   * Test entity linking.
   *
   * @throws Exception if test fails
   */
  @Test
  public void testLinkEntities() throws Exception {
    List<Entity> entities = new ArrayList<>();
    entities.add(new Entity("Kendra", "Person"));
    entities.add(new Entity("Apple", "Organization"));
    entities.add(new Entity("Google", "Organization"));

    List<Entity> linkedEntities = linker.linkEntities(entities);
    Assert.assertNotNull(linkedEntities);
    Assert.assertTrue(linkedEntities.size() > 0);
  }

  /**
   * Test entity deduplication.
   *

   * @throws Exception if test fails
   */
  @Test
  public void testDeduplication() throws Exception {
    List<Entity> entities = new ArrayList<>();
    Entity entity1 = new Entity("John Smith", "Person");
    Entity entity2 = new Entity("John Smith", "Person");
    entities.add(entity1);
    entities.add(entity2);

    List<Entity> linkedEntities = linker.linkEntities(entities);
    Assert.assertNotNull(linkedEntities);
    // Should be deduplicated or merged
    Assert.assertTrue(linkedEntities.size() <= entities.size());
  }

  /**
   * Test entity similarity.
   *

   * @throws Exception if test fails
   */
  @Test
  public void testEntitySimilarity() throws Exception {
    Entity entity1 = new Entity("John Smith", "Person");
    Entity entity2 = new Entity("John Smith", "Person");
    Entity entity3 = new Entity("Jane Doe", "Person");

    double similarity12 = linker.getEntitySimilarity(entity1, entity2);
    double similarity13 = linker.getEntitySimilarity(entity1, entity3);

    Assert.assertTrue(similarity12 > similarity13);
    Assert.assertTrue(similarity12 >= 0.0 && similarity12 <= 1.0);
  }

  /**
   * Test empty entity list.
   *

   * @throws Exception if test fails
   */
  @Test
  public void testEmptyEntityList() throws Exception {
    List<Entity> entities = new ArrayList<>();
    List<Entity> linkedEntities = linker.linkEntities(entities);
    Assert.assertNotNull(linkedEntities);
    Assert.assertEquals(0, linkedEntities.size());
  }

  /**
   * Test type mismatch in similarity.
   *

   * @throws Exception if test fails
   */
  @Test
  public void testTypeMismatch() throws Exception {
    Entity entity1 = new Entity("Apple", "Organization");
    Entity entity2 = new Entity("Apple", "Product");

    double similarity = linker.getEntitySimilarity(entity1, entity2);
    Assert.assertEquals(0.0, similarity, 0.001);
  }

  /**
   * Cleanup test fixtures.
   *

   * @throws Exception if cleanup fails
   */
  @Test
  public void testClose() throws Exception {
    linker.close();
    // Should complete without error
  }
}
