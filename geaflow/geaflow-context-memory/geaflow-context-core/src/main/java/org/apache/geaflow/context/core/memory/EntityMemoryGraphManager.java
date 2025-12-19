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
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.geaflow.context.core.memory;

import static org.apache.geaflow.common.config.keys.FrameworkConfigKeys.INFER_ENV_ENABLE;
import static org.apache.geaflow.common.config.keys.FrameworkConfigKeys.INFER_ENV_USER_TRANSFORM_CLASSNAME;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.infer.InferContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 实体记忆图谱管理器 - 生产可用版本
 * 
 * <p>基于 PMI (Pointwise Mutual Information) 和 NetworkX 的实体记忆扩散。
 * 参考：https://github.com/undertaker86001/higress/pull/1
 * 
 * <p>核心特性：
 * <ul>
 *   <li>Python集成：通过GeaFlow-Infer调用entity_memory_graph.py</li>
 *   <li>动态 PMI 权重计算：基于实体共现频率和边缘概率</li>
 *   <li>记忆扩散：模拟海马体的记忆激活扩散机制</li>
 *   <li>自适应裁剪：动态调整噪声阈值，移除低权重连接</li>
 *   <li>生产可用：完整错误处理和日志记录</li>
 * </ul>
 */
public class EntityMemoryGraphManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(EntityMemoryGraphManager.class);
  
  private static final String TRANSFORM_CLASS_NAME = "TransFormFunction";

  private final Configuration config;
  private InferContext<Object> inferContext;
  private boolean initialized = false;

  private final double baseDecay;
  private final double noiseThreshold;
  private final int maxEdgesPerNode;
  private final int pruneInterval;

  public EntityMemoryGraphManager(Configuration config) {
    this.config = config;
    this.baseDecay = Double.parseDouble(
        config.getString("entity.memory.base_decay", "0.6"));
    this.noiseThreshold = Double.parseDouble(
        config.getString("entity.memory.noise_threshold", "0.2"));
    this.maxEdgesPerNode = Integer.parseInt(
        config.getString("entity.memory.max_edges_per_node", "30"));
    this.pruneInterval = Integer.parseInt(
        config.getString("entity.memory.prune_interval", "1000"));
  }

  /**
   * 初始化实体记忆图谱
   */
  public void initialize() throws Exception {
    if (initialized) {
      return;
    }

    LOGGER.info("正在初始化实体记忆图谱...");

    try {
      // 配置GeaFlow-Infer环境
      config.put(INFER_ENV_ENABLE, "true");
      config.put(INFER_ENV_USER_TRANSFORM_CLASSNAME, TRANSFORM_CLASS_NAME);
      
      // 创建InferContext连接Python进程
      inferContext = new InferContext<>(config);
      
      // 调用Python初始化方法
      Boolean result = (Boolean) inferContext.infer(
          "init", baseDecay, noiseThreshold, maxEdgesPerNode, pruneInterval);
      
      if (result == null || !result) {
        throw new RuntimeException("Python图谱初始化失败");
      }

      LOGGER.info("实体记忆图谱初始化成功: decay={}, noise={}, max_edges={}, prune_interval={}",
          baseDecay, noiseThreshold, maxEdgesPerNode, pruneInterval);

      initialized = true;

    } catch (Exception e) {
      LOGGER.error("实体记忆图谱初始化失败", e);
      throw e;
    }
  }

  /**
   * 添加实体到记忆图谱
   * 
   * <p>将在同一 Episode 中共现的实体添加到图谱，
   * 系统会自动计算它们之间的 PMI 权重。
   * 
   * @param entityIds Episode 中的实体 ID 列表
   * @throws Exception 如果添加失败
   */
  public void addEntities(List<String> entityIds) throws Exception {
    if (!initialized) {
      throw new IllegalStateException("实体记忆图谱未初始化");
    }

    if (entityIds == null || entityIds.isEmpty()) {
      LOGGER.warn("实体列表为空，跳过添加");
      return;
    }

    try {
      // 调用Python图谱添加实体
      Boolean result = (Boolean) inferContext.infer("add", entityIds);
      
      if (result == null || !result) {
        LOGGER.error("添加实体失败: {}", entityIds);
        throw new RuntimeException("Python添加实体失败");
      }

      LOGGER.debug("已添加 {} 个实体到记忆图谱", entityIds.size());

    } catch (Exception e) {
      LOGGER.error("添加实体到图谱失败: {}", entityIds, e);
      throw e;
    }
  }

  /**
   * 扩展实体 - 记忆扩散
   * 
   * <p>从种子实体开始，通过高权重边扩散到相关实体。
   * 模拟海马体的记忆激活机制。
   * 
   * @param seedEntityIds 种子实体 ID 列表
   * @param topK 返回的扩展实体数量
   * @return 扩展的实体列表，按激活强度降序排列
   * @throws Exception 如果扩展失败
   */
  @SuppressWarnings("unchecked")
  public List<ExpandedEntity> expandEntities(List<String> seedEntityIds, int topK)
      throws Exception {
    if (!initialized) {
      throw new IllegalStateException("实体记忆图谱未初始化");
    }

    if (seedEntityIds == null || seedEntityIds.isEmpty()) {
      LOGGER.warn("种子实体列表为空");
      return new ArrayList<>();
    }

    try {
      // 调用Python图谱扩展实体
      // Python返回: List<List<Object>> = [[entity_id, strength], ...]
      List<List<Object>> pythonResult = (List<List<Object>>) inferContext.infer(
          "expand", seedEntityIds, topK);

      List<ExpandedEntity> expandedEntities = new ArrayList<>();

      if (pythonResult != null) {
        for (List<Object> item : pythonResult) {
          if (item.size() >= 2) {
            String entityId = (String) item.get(0);
            double activationStrength = ((Number) item.get(1)).doubleValue();
            expandedEntities.add(new ExpandedEntity(entityId, activationStrength));
          }
        }
      }

      LOGGER.info("从 {} 个种子实体扩展得到 {} 个相关实体",
          seedEntityIds.size(), expandedEntities.size());

      return expandedEntities;

    } catch (Exception e) {
      LOGGER.error("实体扩散失败: seeds={}", seedEntityIds, e);
      throw e;
    }
  }

  /**
   * 获取图谱统计信息
   * 
   * @return 统计信息 Map
   * @throws Exception 如果获取失败
   */
  @SuppressWarnings("unchecked")
  public Map<String, Number> getStats() throws Exception {
    if (!initialized) {
      throw new IllegalStateException("实体记忆图谱未初始化");
    }

    try {
      Map<String, Number> stats = (Map<String, Number>) inferContext.infer("stats");
      return stats != null ? stats : new HashMap<>();

    } catch (Exception e) {
      LOGGER.error("获取图谱统计失败", e);
      throw e;
    }
  }

  /**
   * 清空图谱
   * 
   * @throws Exception 如果清空失败
   */
  public void clear() throws Exception {
    if (!initialized) {
      return;
    }

    try {
      inferContext.infer("clear");
      LOGGER.info("实体记忆图谱已清空");

    } catch (Exception e) {
      LOGGER.error("清空图谱失败", e);
      throw e;
    }
  }

  /**
   * 关闭管理器
   * 
   * @throws Exception 如果关闭失败
   */
  public void close() throws Exception {
    if (!initialized) {
      return;
    }

    try {
      clear();
      if (inferContext != null) {
        inferContext.close();
      }
      initialized = false;
      LOGGER.info("实体记忆图谱管理器已关闭");

    } catch (Exception e) {
      LOGGER.error("关闭管理器失败", e);
      throw e;
    }
  }

  /**
   * 扩展实体结果
   */
  public static class ExpandedEntity {
    private final String entityId;
    private final double activationStrength;

    public ExpandedEntity(String entityId, double activationStrength) {
      this.entityId = entityId;
      this.activationStrength = activationStrength;
    }

    public String getEntityId() {
      return entityId;
    }

    public double getActivationStrength() {
      return activationStrength;
    }

    @Override
    public String toString() {
      return String.format("ExpandedEntity{id='%s', strength=%.4f}",
          entityId, activationStrength);
    }
  }
}
