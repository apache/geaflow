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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.geaflow.common.config.Configuration;

/**
 * Mock版本的实体记忆图谱管理器，用于测试
 * 
 * <p>不启动真实的Python进程，而是使用内存中的简化实现
 */
public class MockEntityMemoryGraphManager extends EntityMemoryGraphManager {

  private final Map<String, Set<String>> cooccurrences;
  private boolean mockInitialized = false;

  public MockEntityMemoryGraphManager(Configuration config) {
    super(config);
    this.cooccurrences = new HashMap<>();
  }

  @Override
  public void initialize() throws Exception {
    // 不启动真实的InferContext，仅设置标志
    mockInitialized = true;
  }

  @Override
  public void addEntities(List<String> entityIds) throws Exception {
    if (!mockInitialized) {
      throw new IllegalStateException("实体记忆图谱未初始化");
    }

    if (entityIds == null || entityIds.isEmpty()) {
      return;
    }

    // 记录共现关系
    for (int i = 0; i < entityIds.size(); i++) {
      for (int j = i + 1; j < entityIds.size(); j++) {
        String id1 = entityIds.get(i);
        String id2 = entityIds.get(j);
        
        cooccurrences.computeIfAbsent(id1, k -> new HashSet<>()).add(id2);
        cooccurrences.computeIfAbsent(id2, k -> new HashSet<>()).add(id1);
      }
    }
  }

  @Override
  public List<ExpandedEntity> expandEntities(List<String> seedEntityIds, int topK)
      throws Exception {
    if (!mockInitialized) {
      throw new IllegalStateException("实体记忆图谱未初始化");
    }

    if (seedEntityIds == null || seedEntityIds.isEmpty()) {
      return new ArrayList<>();
    }

    List<ExpandedEntity> result = new ArrayList<>();
    Set<String> visited = new HashSet<>(seedEntityIds);

    // 简单模拟：返回与种子实体共现过的实体
    for (String seedId : seedEntityIds) {
      Set<String> neighbors = cooccurrences.get(seedId);
      if (neighbors != null) {
        for (String neighbor : neighbors) {
          if (!visited.contains(neighbor)) {
            visited.add(neighbor);
            // Mock强度值
            result.add(new ExpandedEntity(neighbor, 0.8));
            if (result.size() >= topK) {
              return result;
            }
          }
        }
      }
    }

    return result;
  }

  @Override
  public Map<String, Number> getStats() throws Exception {
    if (!mockInitialized) {
      throw new IllegalStateException("实体记忆图谱未初始化");
    }

    Map<String, Number> stats = new HashMap<>();
    stats.put("nodes", cooccurrences.size());
    
    int edges = 0;
    for (Set<String> neighbors : cooccurrences.values()) {
      edges += neighbors.size();
    }
    stats.put("edges", edges / 2); // 无向图，除以2

    return stats;
  }

  @Override
  public void clear() throws Exception {
    if (!mockInitialized) {
      return;
    }
    cooccurrences.clear();
  }

  @Override
  public void close() throws Exception {
    if (!mockInitialized) {
      return;
    }
    clear();
    mockInitialized = false;
  }
}
