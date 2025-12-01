#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Entity Memory Graph - 基于 PMI 和 NetworkX 的实体记忆图谱
参考：https://github.com/undertaker86001/higress/pull/1
"""

import networkx as nx
import heapq
import math
import logging
from typing import Set, List, Dict, Tuple
from itertools import combinations

logger = logging.getLogger(__name__)


class EntityMemoryGraph:
    """
    实体记忆图谱 - 使用 PMI (Pointwise Mutual Information) 计算实体关联强度
    
    核心特性：
    1. 动态 PMI 权重：基于共现频率和边缘概率计算实体关联
    2. 记忆扩散：模拟海马体的记忆激活扩散机制
    3. 自适应裁剪：动态调整噪声阈值，移除低权重连接
    4. 度数限制：防止超级节点过度连接
    """
    
    def __init__(
        self,
        base_decay: float = 0.6,
        noise_threshold: float = 0.2,
        max_edges_per_node: int = 30,
        prune_interval: int = 1000
    ):
        """
        初始化实体记忆图谱
        
        Args:
            base_decay: 基础衰减率（记忆扩散时的衰减）
            noise_threshold: 噪声阈值（低于此值的边会被裁剪）
            max_edges_per_node: 每个节点的最大边数
            prune_interval: 裁剪间隔（添加多少节点后进行裁剪）
        """
        self._graph = nx.Graph()
        self._base_decay = base_decay
        self._noise_threshold = noise_threshold
        self._max_edges_per_node = max_edges_per_node
        self._prune_interval = prune_interval
        
        self._need_update_noise = True
        self._add_cnt = 0
        
        logger.info(
            f"实体记忆图谱初始化: decay={base_decay}, "
            f"noise={noise_threshold}, max_edges={max_edges_per_node}"
        )
    
    def add_entities(self, entity_ids: List[str]) -> None:
        """
        添加实体到图谱
        
        Args:
            entity_ids: 实体ID列表（在同一 Episode 中共现的实体）
        """
        entities = set(entity_ids)
        
        # 添加节点
        current_nodes = self._graph.number_of_nodes()
        self._graph.add_nodes_from(entities)
        after_nodes = self._graph.number_of_nodes()
        
        # 更新边权重（PMI计算）
        self._update_edges_with_pmi(entities)
        
        # 定期裁剪
        self._add_cnt += after_nodes - current_nodes
        if self._add_cnt >= self._prune_interval:
            self._prune_graph()
            self._add_cnt = 0
    
    def _update_edges_with_pmi(self, entities: Set[str]) -> None:
        """
        使用 PMI 更新边权重
        
        PMI(i, j) = log2(P(i,j) / (P(i) * P(j)))
        其中：
        - P(i,j) = cooccurrence / total_edges
        - P(i) = degree_i / total_edges
        - P(j) = degree_j / total_edges
        """
        edges_to_update = []
        
        # 统计共现次数
        for i, j in combinations(entities, 2):
            if self._graph.has_edge(i, j):
                self._graph[i][j]['cooccurrence'] += 1
            else:
                self._graph.add_edge(i, j, cooccurrence=1)
            edges_to_update.append((i, j))
        
        # 计算 PMI 权重
        total_edges = max(1, self._graph.number_of_edges())
        
        for i, j in edges_to_update:
            degree_i = max(1, dict(self._graph.degree())[i])
            degree_j = max(1, dict(self._graph.degree())[j])
            cooccurrence = self._graph[i][j]['cooccurrence']
            
            # PMI 公式
            pmi = math.log2((cooccurrence * total_edges) / (degree_i * degree_j))
            
            # 平滑和归一化
            smoothing = 0.2
            norm_factor = math.log2(total_edges) + smoothing
            
            # 频率因子（防止低频共现的 PMI 过高）
            freq_factor = math.log2(1 + cooccurrence) / math.log2(total_edges)
            
            # PMI 归一化
            pmi_factor = (pmi + smoothing) / norm_factor
            
            # 综合权重 = alpha * PMI + (1-alpha) * 频率
            alpha = 0.7
            weight = alpha * pmi_factor + (1 - alpha) * freq_factor
            weight = max(0.01, min(1.0, weight))
            
            self._graph[i][j]['weight'] = weight
        
        self._need_update_noise = True
    
    def expand_entities(
        self,
        seed_entities: List[str],
        max_depth: int = 3,
        top_k: int = 20
    ) -> List[Tuple[str, float]]:
        """
        记忆扩散 - 基于种子实体扩展相关实体
        
        模拟海马体的记忆激活扩散机制：
        1. 从种子实体开始
        2. 通过高权重边扩散到相邻实体
        3. 强度随深度衰减
        4. 返回激活强度最高的实体
        
        Args:
            seed_entities: 种子实体列表
            max_depth: 最大扩散深度
            top_k: 返回的实体数量
            
        Returns:
            [(entity_id, activation_strength), ...] 按强度降序排列
        """
        valid_seeds = {e for e in seed_entities if self._graph.has_node(e)}
        if not valid_seeds:
            logger.warning("种子实体无效，无法扩散")
            return []
        
        self._update_noise_threshold()
        
        # 优先队列: (-strength, entity, depth, from_entity)
        need_search = []
        for entity in valid_seeds:
            heapq.heappush(need_search, (-1.0, entity, 0, entity))
        
        activated: Dict[str, float] = {}
        max_strength: Dict[str, float] = {}
        
        while need_search:
            neg_strength, curr, depth, from_entity = heapq.heappop(need_search)
            curr_strength = -neg_strength
            
            # 强度太低或深度太深，停止扩散
            if curr_strength <= self._noise_threshold or depth >= max_depth:
                continue
            
            # 如果当前强度不是最大，跳过
            if curr_strength <= max_strength.get(curr, 0):
                continue
            
            max_strength[curr] = curr_strength
            activated[curr] = curr_strength
            
            # 获取邻居
            neighbors = self._get_valid_neighbors(curr)
            if not neighbors:
                continue
            
            # 计算平均权重（用于动态衰减）
            avg_weight = sum(
                self._get_edge_weight(curr, n) for n in neighbors
            ) / len(neighbors)
            
            # 向邻居扩散
            for neighbor in neighbors:
                edge_weight = self._get_edge_weight(curr, neighbor)
                
                # 动态衰减率
                weight_ratio = edge_weight / max(0.01, avg_weight)
                dynamic_decay = self._base_decay + 0.2 * min(1.0, weight_ratio)
                dynamic_decay = min(dynamic_decay, 0.8)
                
                # 计算新强度
                decay_rate = dynamic_decay ** depth
                new_strength = curr_strength * decay_rate * edge_weight
                
                if new_strength > self._noise_threshold:
                    heapq.heappush(
                        need_search,
                        (-new_strength, neighbor, depth + 1, curr)
                    )
        
        # 归一化并排序
        if activated:
            max_value = max(activated.values())
            normalized = {
                k: v / max_value
                for k, v in activated.items()
                if k not in valid_seeds  # 排除种子实体
            }
            sorted_entities = sorted(
                normalized.items(),
                key=lambda x: -x[1]
            )
            return sorted_entities[:top_k]
        
        return []
    
    def _prune_graph(self) -> None:
        """
        图谱裁剪 - 移除低权重边和孤立节点
        """
        self._update_noise_threshold()
        
        # 移除低权重边
        edges_to_remove = [
            (u, v) for u, v, data in self._graph.edges(data=True)
            if data['weight'] < self._noise_threshold
        ]
        self._graph.remove_edges_from(edges_to_remove)
        
        # 限制节点边数
        self._limit_node_edges()
        
        # 移除孤立节点
        isolated = [
            node for node, degree in dict(self._graph.degree()).items()
            if degree == 0
        ]
        self._graph.remove_nodes_from(isolated)
        
        self._need_update_noise = True
        
        logger.info(
            f"图谱裁剪完成: nodes={self._graph.number_of_nodes()}, "
            f"edges={self._graph.number_of_edges()}, "
            f"avg_degree={self._get_avg_degree():.2f}"
        )
    
    def _update_noise_threshold(self) -> None:
        """动态调整噪声阈值"""
        if not self._need_update_noise:
            return
        
        self._need_update_noise = False
        
        if self._graph.number_of_edges() == 0:
            self._noise_threshold = 0.2
            return
        
        # 使用下四分位数作为阈值
        weights = [
            data['weight']
            for _, _, data in self._graph.edges(data=True)
        ]
        
        if weights:
            import numpy as np
            lower_quartile = np.percentile(weights, 25)
            avg_degree = self._get_avg_degree()
            
            # 根据平均度数动态调整最大阈值
            max_threshold = 0.4 + 0.1 * math.log2(avg_degree) if avg_degree > 0 else 0.4
            self._noise_threshold = max(0.1, min(max_threshold, lower_quartile))
    
    def _limit_node_edges(self) -> None:
        """限制每个节点的最大边数"""
        for node in self._graph.nodes():
            edges = list(self._graph.edges(node, data=True))
            if len(edges) > self._max_edges_per_node:
                # 按权重排序，保留权重最高的边
                edges = sorted(edges, key=lambda x: -x[2]['weight'])
                for edge in edges[self._max_edges_per_node:]:
                    self._graph.remove_edge(edge[0], edge[1])
    
    def _get_valid_neighbors(self, entity: str) -> List[str]:
        """获取有效邻居（权重高于阈值的邻居）"""
        if not self._graph.has_node(entity):
            return []
        
        edges = self._graph.edges(entity, data=True)
        sorted_edges = sorted(
            edges,
            key=lambda x: -x[2]['weight']
        )[:self._max_edges_per_node]
        
        valid_edges = [
            edge for edge in sorted_edges
            if edge[2]['weight'] > self._noise_threshold
        ]
        
        return [edge[1] for edge in valid_edges]
    
    def _get_edge_weight(self, entity1: str, entity2: str) -> float:
        """获取边权重"""
        if self._graph.has_edge(entity1, entity2):
            return self._graph[entity1][entity2]['weight']
        return 0.0
    
    def _get_avg_degree(self) -> float:
        """计算平均度数"""
        if self._graph.number_of_nodes() == 0:
            return 0.0
        return sum(
            dict(self._graph.degree()).values()
        ) / self._graph.number_of_nodes()
    
    def get_stats(self) -> Dict[str, float]:
        """获取图谱统计信息"""
        return {
            "num_nodes": self._graph.number_of_nodes(),
            "num_edges": self._graph.number_of_edges(),
            "avg_degree": self._get_avg_degree(),
            "noise_threshold": self._noise_threshold
        }
    
    def clear(self) -> None:
        """清空图谱"""
        self._graph.clear()
        self._add_cnt = 0
        self._need_update_noise = True
        logger.info("实体记忆图谱已清空")


# GeaFlow-Infer 集成接口
class TransFormFunction:
    """GeaFlow-Infer transform function 基类"""
    
    def __init__(self, input_size):
        self.input_size = input_size
    
    def open(self):
        pass
    
    def process(self, *inputs):
        raise NotImplementedError
    
    def close(self):
        pass


class EntityMemoryTransformFunction(TransFormFunction):
    """
    实体记忆图谱 Transform Function
    用于 GeaFlow-Infer Python 集成
    """
    
    def __init__(self):
        super().__init__(1)
        self.graph = EntityMemoryGraph()
    
    def open(self):
        """初始化"""
        logger.info("EntityMemoryTransformFunction opened")
    
    def process(self, *inputs):
        """
        处理操作
        
        Args:
            inputs: (operation, *args)
                operation: 操作类型
                - "add": 添加实体，args = (entity_ids: List[str],)
                - "expand": 扩展实体，args = (seed_entities: List[str], top_k: int)
                - "stats": 获取统计，args = ()
                - "clear": 清空图谱，args = ()
        """
        operation = inputs[0]
        args = inputs[1:] if len(inputs) > 1 else ()
        try:
            if operation == "add":
                entity_ids = args[0]
                self.graph.add_entities(entity_ids)
                return True
            
            elif operation == "expand":
                seed_entities = args[0]
                top_k = args[1] if len(args) > 1 else 20
                result = self.graph.expand_entities(seed_entities, top_k=top_k)
                return result
            
            elif operation == "stats":
                return self.graph.get_stats()
            
            elif operation == "clear":
                self.graph.clear()
                return True
            
            else:
                logger.error(f"Unknown operation: {operation}")
                return None
                
        except Exception as e:
            logger.error(f"Process error: {e}", exc_info=True)
            return None
    
    def close(self):
        """清理资源"""
        self.graph.clear()
        logger.info("EntityMemoryTransformFunction closed")
