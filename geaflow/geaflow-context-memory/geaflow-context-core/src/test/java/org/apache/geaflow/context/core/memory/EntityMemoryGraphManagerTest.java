/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file to
 * you under the Apache License, Version 2.0 (the
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

package org.apache.geaflow.context.core.memory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.geaflow.common.config.Configuration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * 实体记忆图谱管理器测试
 */
public class EntityMemoryGraphManagerTest {

  private EntityMemoryGraphManager manager;
  private Configuration config;

  @Before
  public void setUp() throws Exception {
    config = new Configuration();
    config.put("entity.memory.base_decay", "0.6");
    config.put("entity.memory.noise_threshold", "0.2");
    config.put("entity.memory.max_edges_per_node", "30");
    config.put("entity.memory.prune_interval", "1000");

    // 使用Mock版本避免启动真实Python进程
    manager = new MockEntityMemoryGraphManager(config);
    manager.initialize();
  }

  @After
  public void tearDown() throws Exception {
    if (manager != null) {
      manager.close();
    }
  }

  @Test
  public void testInitialization() {
    Assert.assertNotNull(manager);
  }

  @Test
  public void testAddEntities() throws Exception {
    List<String> entities = Arrays.asList("entity1", "entity2", "entity3");
    manager.addEntities(entities);
    // 验证不抛出异常
  }

  @Test
  public void testAddEmptyEntities() throws Exception {
    manager.addEntities(Arrays.asList());
    // 应该不抛出异常
  }

  @Test(expected = IllegalStateException.class)
  public void testAddEntitiesWithoutInit() throws Exception {
    EntityMemoryGraphManager uninitManager = new MockEntityMemoryGraphManager(config);
    uninitManager.addEntities(Arrays.asList("entity1"));
  }

  @Test
  public void testExpandEntities() throws Exception {
    // 先添加一些实体
    manager.addEntities(Arrays.asList("entity1", "entity2", "entity3"));
    manager.addEntities(Arrays.asList("entity2", "entity3", "entity4"));

    // 扩展实体
    List<EntityMemoryGraphManager.ExpandedEntity> expanded =
        manager.expandEntities(Arrays.asList("entity1"), 5);

    Assert.assertNotNull(expanded);
  }

  @Test
  public void testExpandEmptySeeds() throws Exception {
    List<EntityMemoryGraphManager.ExpandedEntity> expanded =
        manager.expandEntities(Arrays.asList(), 5);

    Assert.assertNotNull(expanded);
    Assert.assertEquals(0, expanded.size());
  }

  @Test
  public void testGetStats() throws Exception {
    manager.addEntities(Arrays.asList("entity1", "entity2"));

    Map<String, Number> stats = manager.getStats();
    Assert.assertNotNull(stats);
  }

  @Test
  public void testClear() throws Exception {
    manager.addEntities(Arrays.asList("entity1", "entity2"));
    manager.clear();

    Map<String, Number> stats = manager.getStats();
    Assert.assertNotNull(stats);
  }

  @Test
  public void testExpandedEntityClass() {
    EntityMemoryGraphManager.ExpandedEntity entity =
        new EntityMemoryGraphManager.ExpandedEntity("test_entity", 0.85);

    Assert.assertEquals("test_entity", entity.getEntityId());
    Assert.assertEquals(0.85, entity.getActivationStrength(), 0.001);
    Assert.assertNotNull(entity.toString());
  }

  @Test
  public void testMultipleAddAndExpand() throws Exception {
    // 添加多组实体
    manager.addEntities(Arrays.asList("A", "B", "C"));
    manager.addEntities(Arrays.asList("B", "C", "D"));
    manager.addEntities(Arrays.asList("C", "D", "E"));

    // 从 A 扩展
    List<EntityMemoryGraphManager.ExpandedEntity> expanded =
        manager.expandEntities(Arrays.asList("A"), 10);

    Assert.assertNotNull(expanded);
  }
}
