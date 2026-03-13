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

package org.apache.geaflow.context.core.retriever;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 混合检索融合器 - 支持多种融合策略
 * 
 * <p>实现了常见的检索结果融合算法：
 * <ul>
 *   <li>RRF (Reciprocal Rank Fusion): 基于排序位置的融合</li>
 *   <li>加权融合: 基于分数的加权平均</li>
 *   <li>归一化融合: 先归一化再加权</li>
 * </ul>
 */
public class HybridFusion {

    private static final Logger LOGGER = LoggerFactory.getLogger(HybridFusion.class);

    /**
     * 融合策略
     */
    public enum FusionStrategy {
        RRF,              // Reciprocal Rank Fusion
        WEIGHTED,         // 加权融合
        NORMALIZED        // 归一化融合
    }

    /**
     * 融合结果
     */
    public static class FusionResult implements Comparable<FusionResult> {
        private final String id;
        private final double score;
        private final Map<String, Double> sourceScores;  // 来自各个检索器的原始分数

        public FusionResult(String id, double score) {
            this.id = id;
            this.score = score;
            this.sourceScores = new HashMap<>();
        }

        public void addSourceScore(String source, double score) {
            sourceScores.put(source, score);
        }

        public String getId() {
            return id;
        }

        public double getScore() {
            return score;
        }

        public Map<String, Double> getSourceScores() {
            return sourceScores;
        }

        @Override
        public int compareTo(FusionResult other) {
            return Double.compare(other.score, this.score);  // 降序
        }
    }

    /**
     * RRF融合 (Reciprocal Rank Fusion)
     * 
     * <p>公式: RRF(d) = Σ 1/(k + rank_i(d))
     * <p>其中 k 是常数（通常为60），rank_i(d) 是文档d在第i个检索器中的排名
     * 
     * <p>优点：
     * - 不需要归一化分数
     * - 对排名位置敏感
     * - 鲁棒性强
     * 
     * @param rankedLists 多个检索器的排序结果列表
     * @param k RRF常数（默认60）
     * @param topK 返回top K结果
     * @return 融合后的结果
     */
    public static List<FusionResult> rrfFusion(
            Map<String, List<String>> rankedLists, 
            int k, 
            int topK) {
        
        Map<String, Double> scores = new HashMap<>();
        Map<String, FusionResult> results = new HashMap<>();
        
        // 对每个检索器的结果进行RRF计算
        for (Map.Entry<String, List<String>> entry : rankedLists.entrySet()) {
            String source = entry.getKey();
            List<String> rankedList = entry.getValue();
            
            for (int rank = 0; rank < rankedList.size(); rank++) {
                String docId = rankedList.get(rank);
                double rrfScore = 1.0 / (k + rank + 1);  // rank从0开始
                
                scores.put(docId, scores.getOrDefault(docId, 0.0) + rrfScore);
                
                // 记录来源分数
                FusionResult result = results.computeIfAbsent(docId, 
                    id -> new FusionResult(id, 0.0));
                result.addSourceScore(source, rrfScore);
            }
        }
        
        // 更新最终分数
        for (Map.Entry<String, Double> entry : scores.entrySet()) {
            FusionResult result = results.get(entry.getKey());
            results.put(entry.getKey(), new FusionResult(entry.getKey(), entry.getValue()));
            results.get(entry.getKey()).sourceScores.putAll(result.sourceScores);
        }
        
        // 排序并返回top K
        List<FusionResult> sortedResults = new ArrayList<>(results.values());
        Collections.sort(sortedResults);
        
        if (sortedResults.size() > topK) {
            sortedResults = sortedResults.subList(0, topK);
        }
        
        LOGGER.info("RRF融合完成: {} 个检索器, 返回 {} 个结果", 
            rankedLists.size(), sortedResults.size());
        
        return sortedResults;
    }

    /**
     * 加权融合
     * 
     * <p>简单的加权平均：score = Σ w_i * score_i
     * 
     * @param scoredResults 多个检索器的评分结果
     * @param weights 权重Map（source -> weight）
     * @param topK 返回top K结果
     * @return 融合后的结果
     */
    public static List<FusionResult> weightedFusion(
            Map<String, Map<String, Double>> scoredResults,
            Map<String, Double> weights,
            int topK) {
        
        Map<String, FusionResult> results = new HashMap<>();
        
        // 对每个检索器的结果进行加权
        for (Map.Entry<String, Map<String, Double>> entry : scoredResults.entrySet()) {
            String source = entry.getKey();
            Map<String, Double> scores = entry.getValue();
            double weight = weights.getOrDefault(source, 1.0);
            
            for (Map.Entry<String, Double> scoreEntry : scores.entrySet()) {
                String docId = scoreEntry.getKey();
                double score = scoreEntry.getValue() * weight;
                
                FusionResult result = results.computeIfAbsent(docId, 
                    id -> new FusionResult(id, 0.0));
                
                results.put(docId, new FusionResult(docId, result.score + score));
                results.get(docId).addSourceScore(source, scoreEntry.getValue());
            }
        }
        
        // 排序并返回top K
        List<FusionResult> sortedResults = new ArrayList<>(results.values());
        Collections.sort(sortedResults);
        
        if (sortedResults.size() > topK) {
            sortedResults = sortedResults.subList(0, topK);
        }
        
        LOGGER.info("加权融合完成: {} 个检索器, 返回 {} 个结果", 
            scoredResults.size(), sortedResults.size());
        
        return sortedResults;
    }

    /**
     * 归一化融合
     * 
     * <p>先对每个检索器的分数进行归一化，再加权融合
     * <p>归一化方法：score_norm = (score - min) / (max - min)
     * 
     * @param scoredResults 多个检索器的评分结果
     * @param weights 权重Map
     * @param topK 返回top K结果
     * @return 融合后的结果
     */
    public static List<FusionResult> normalizedFusion(
            Map<String, Map<String, Double>> scoredResults,
            Map<String, Double> weights,
            int topK) {
        
        // 1. 归一化每个检索器的分数
        Map<String, Map<String, Double>> normalizedResults = new HashMap<>();
        
        for (Map.Entry<String, Map<String, Double>> entry : scoredResults.entrySet()) {
            String source = entry.getKey();
            Map<String, Double> scores = entry.getValue();
            
            if (scores.isEmpty()) {
                continue;
            }
            
            // 找到最大最小值
            double minScore = Collections.min(scores.values());
            double maxScore = Collections.max(scores.values());
            double range = maxScore - minScore;
            
            // 归一化
            Map<String, Double> normalized = new HashMap<>();
            for (Map.Entry<String, Double> scoreEntry : scores.entrySet()) {
                double normScore = range > 0 ? 
                    (scoreEntry.getValue() - minScore) / range : 0.5;
                normalized.put(scoreEntry.getKey(), normScore);
            }
            
            normalizedResults.put(source, normalized);
        }
        
        // 2. 加权融合归一化后的分数
        return weightedFusion(normalizedResults, weights, topK);
    }

    /**
     * 便捷方法：默认参数的RRF融合
     */
    public static List<FusionResult> rrfFusion(
            Map<String, List<String>> rankedLists, 
            int topK) {
        return rrfFusion(rankedLists, 60, topK);
    }

    /**
     * 便捷方法：等权重加权融合
     */
    public static List<FusionResult> weightedFusion(
            Map<String, Map<String, Double>> scoredResults,
            int topK) {
        Map<String, Double> equalWeights = new HashMap<>();
        for (String source : scoredResults.keySet()) {
            equalWeights.put(source, 1.0);
        }
        return weightedFusion(scoredResults, equalWeights, topK);
    }
}
