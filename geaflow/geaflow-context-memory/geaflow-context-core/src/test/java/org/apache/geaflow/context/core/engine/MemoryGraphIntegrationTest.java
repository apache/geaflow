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

package org.apache.geaflow.context.core.engine;

import java.util.Arrays;
import org.apache.geaflow.context.api.model.Episode;
import org.apache.geaflow.context.api.query.ContextQuery;
import org.apache.geaflow.context.api.result.ContextSearchResult;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * 实体记忆图谱集成测试
 * 
 * <p>演示如何启用和使用基于 PMI 的实体记忆扩散策略
 */
public class MemoryGraphIntegrationTest {

  private DefaultContextMemoryEngine engine;
  private DefaultContextMemoryEngine.ContextMemoryConfig config;

  @Before
  public void setUp() throws Exception {
    config = new DefaultContextMemoryEngine.ContextMemoryConfig();
    
    // 注意：测试环境下不启用实体记忆图谱，因为需要真实Python环境
    // 生产环境中启用时，需要配置GeaFlow-Infer环境
    config.setEnableMemoryGraph(false);
    config.setMemoryGraphBaseDecay(0.6);
    config.setMemoryGraphNoiseThreshold(0.2);
    config.setMemoryGraphMaxEdges(30);
    config.setMemoryGraphPruneInterval(1000);
    
    engine = new DefaultContextMemoryEngine(config);
    engine.initialize();
  }

  @After
  public void tearDown() throws Exception {
    if (engine != null) {
      engine.close();
    }
  }

  @Test
  public void testMemoryGraphDisabled() throws Exception {
    // 测试未启用记忆图谱的情况
    DefaultContextMemoryEngine.ContextMemoryConfig disabledConfig = 
        new DefaultContextMemoryEngine.ContextMemoryConfig();
    disabledConfig.setEnableMemoryGraph(false);
    
    DefaultContextMemoryEngine disabledEngine = new DefaultContextMemoryEngine(disabledConfig);
    disabledEngine.initialize();
    
    // 添加数据
    Episode episode = new Episode("ep_001", "Test", System.currentTimeMillis(), "test");
    Episode.Entity entity = new Episode.Entity("entity1", "Entity1", "Type1");
    episode.setEntities(Arrays.asList(entity));
    disabledEngine.ingestEpisode(episode);
    
    // 使用 MEMORY_GRAPH 策略应该退回到关键词搜索
    ContextQuery query = new ContextQuery.Builder()
        .queryText("Entity1")
        .strategy(ContextQuery.RetrievalStrategy.MEMORY_GRAPH)
        .build();
    
    ContextSearchResult result = disabledEngine.search(query);
    Assert.assertNotNull(result);
    
    disabledEngine.close();
  }

  @Test
  public void testMemoryGraphStrategyBasic() throws Exception {
    // 注意：此测试由于未启用Memory Graph，实际会退回到关键词搜索
    // 添加多个Episode，建立实体共现关系
    Episode ep1 = new Episode("ep_001", "Episode 1", System.currentTimeMillis(), "content 1");
    ep1.setEntities(Arrays.asList(
        new Episode.Entity("alice", "Alice", "Person"),
        new Episode.Entity("bob", "Bob", "Person"),
        new Episode.Entity("company", "TechCorp", "Organization")
    ));
    engine.ingestEpisode(ep1);

    Episode ep2 = new Episode("ep_002", "Episode 2", System.currentTimeMillis(), "content 2");
    ep2.setEntities(Arrays.asList(
        new Episode.Entity("bob", "Bob", "Person"),
        new Episode.Entity("company", "TechCorp", "Organization"),
        new Episode.Entity("product", "ProductX", "Product")
    ));
    engine.ingestEpisode(ep2);

    Episode ep3 = new Episode("ep_003", "Episode 3", System.currentTimeMillis(), "content 3");
    ep3.setEntities(Arrays.asList(
        new Episode.Entity("alice", "Alice", "Person"),
        new Episode.Entity("product", "ProductX", "Product")
    ));
    engine.ingestEpisode(ep3);

    // 使用 MEMORY_GRAPH 策略搜索（会退回到关键词搜索）
    ContextQuery query = new ContextQuery.Builder()
        .queryText("Alice")
        .strategy(ContextQuery.RetrievalStrategy.MEMORY_GRAPH)
        .topK(10)
        .build();

    ContextSearchResult result = engine.search(query);

    Assert.assertNotNull(result);
    Assert.assertTrue(result.getExecutionTime() >= 0);
    // 应该返回 Alice 相关实体
    Assert.assertTrue(result.getEntities().size() > 0);
  }

  @Test
  public void testMemoryGraphVsKeywordSearch() throws Exception {
    // 准备测试数据
    Episode ep1 = new Episode("ep_001", "Test", System.currentTimeMillis(), "content");
    ep1.setEntities(Arrays.asList(
        new Episode.Entity("e1", "Java", "Language"),
        new Episode.Entity("e2", "Spring", "Framework")
    ));
    engine.ingestEpisode(ep1);

    Episode ep2 = new Episode("ep_002", "Test", System.currentTimeMillis(), "content");
    ep2.setEntities(Arrays.asList(
        new Episode.Entity("e2", "Spring", "Framework"),
        new Episode.Entity("e3", "Hibernate", "Framework")
    ));
    engine.ingestEpisode(ep2);

    // 关键词搜索
    ContextQuery keywordQuery = new ContextQuery.Builder()
        .queryText("Java")
        .strategy(ContextQuery.RetrievalStrategy.KEYWORD_ONLY)
        .build();
    ContextSearchResult keywordResult = engine.search(keywordQuery);

    // 记忆图谱搜索（实际会退回到关键词搜索）
    ContextQuery memoryQuery = new ContextQuery.Builder()
        .queryText("Java")
        .strategy(ContextQuery.RetrievalStrategy.MEMORY_GRAPH)
        .topK(10)
        .build();
    ContextSearchResult memoryResult = engine.search(memoryQuery);

    Assert.assertNotNull(keywordResult);
    Assert.assertNotNull(memoryResult);

    // 由于未启用Memory Graph，两者结果应该相同
    Assert.assertTrue(keywordResult.getEntities().size() > 0);
    Assert.assertEquals(keywordResult.getEntities().size(), memoryResult.getEntities().size());
  }

  @Test
  public void testMemoryGraphWithEmptyQuery() throws Exception {
    ContextQuery query = new ContextQuery.Builder()
        .queryText("")
        .strategy(ContextQuery.RetrievalStrategy.MEMORY_GRAPH)
        .build();

    ContextSearchResult result = engine.search(query);
    Assert.assertNotNull(result);
  }

  @Test
  public void testMemoryGraphConfiguration() {
    // 测试配置参数
    Assert.assertFalse(config.isEnableMemoryGraph());
    Assert.assertEquals(0.6, config.getMemoryGraphBaseDecay(), 0.001);
    Assert.assertEquals(0.2, config.getMemoryGraphNoiseThreshold(), 0.001);
    Assert.assertEquals(30, config.getMemoryGraphMaxEdges());
    Assert.assertEquals(1000, config.getMemoryGraphPruneInterval());
  }
}
