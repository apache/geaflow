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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.geaflow.context.api.model.Episode;
import org.apache.geaflow.context.api.query.ContextQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BM25检索器 - 基于概率排序的文本检索算法
 * 
 * <p>BM25 (Best Matching 25) 是一种用于信息检索的排序函数，
 * 用于估计文档与给定搜索查询的相关性。
 * 
 * <p>核心公式：
 * <pre>
 * score(D,Q) = Σ IDF(qi) * (f(qi,D) * (k1 + 1)) / (f(qi,D) + k1 * (1 - b + b * |D|/avgdl))
 * 
 * 其中：
 * - IDF(qi) = log((N - n(qi) + 0.5) / (n(qi) + 0.5) + 1)
 * - f(qi,D) = qi在文档D中的词频
 * - |D| = 文档D的长度
 * - avgdl = 平均文档长度
 * - k1, b = 调优参数
 * </pre>
 */
public class BM25Retriever implements Retriever {

    private static final Logger LOGGER = LoggerFactory.getLogger(BM25Retriever.class);

    // BM25参数
    private final double k1;  // 词频饱和度参数 (通常1.2-2.0)
    private final double b;   // 长度归一化参数 (通常0.75)

    // 文档统计
    private Map<String, Document> documents;
    private Map<String, Integer> termDocFreq;  // 词项文档频率
    private int totalDocs;
    private double avgDocLength;
    
    // 实体存储引用（用于获取完整实体信息）
    private Map<String, Episode.Entity> entityStore;
    
    /**
     * 设置实体存储引用
     */
    public void setEntityStore(Map<String, Episode.Entity> entityStore) {
        this.entityStore = entityStore;
    }
    
    @Override
    public List<RetrievalResult> retrieve(ContextQuery query, int topK) throws Exception {
        if (query.getQueryText() == null || query.getQueryText().isEmpty()) {
            return Collections.emptyList();
        }
        
        // 使用内部search方法
        List<BM25Result> bm25Results = search(
            query.getQueryText(), 
            entityStore != null ? entityStore : new HashMap<>(), 
            topK
        );
        
        // 转换为统一的RetrievalResult
        List<RetrievalResult> results = new ArrayList<>();
        for (BM25Result bm25Result : bm25Results) {
            results.add(new RetrievalResult(
                bm25Result.getDocId(),
                bm25Result.getScore(),
                bm25Result  // 保留原始BM25结果作为元数据
            ));
        }
        
        return results;
    }
    
    @Override
    public String getName() {
        return "BM25";
    }
    
    @Override
    public boolean isAvailable() {
        return documents != null && !documents.isEmpty();
    }

    /**
     * 文档包装类
     */
    public static class Document {
        String docId;
        String content;
        Map<String, Integer> termFreqs;  // 词频
        int length;  // 文档长度（词数）

        public Document(String docId, String content) {
            this.docId = docId;
            this.content = content;
            this.termFreqs = new HashMap<>();
            this.length = 0;
            processContent();
        }

        private void processContent() {
            if (content == null || content.isEmpty()) {
                return;
            }

            // 简单分词（空格分割 + 小写化）
            // 生产环境应使用专业分词器（如Lucene Analyzer）
            String[] terms = content.toLowerCase()
                .replaceAll("[^a-z0-9\\s\\u4e00-\\u9fa5]", " ")
                .split("\\s+");

            for (String term : terms) {
                if (!term.isEmpty()) {
                    termFreqs.put(term, termFreqs.getOrDefault(term, 0) + 1);
                    length++;
                }
            }
        }

        public Map<String, Integer> getTermFreqs() {
            return termFreqs;
        }

        public int getLength() {
            return length;
        }
    }

    /**
     * BM25检索结果
     */
    public static class BM25Result implements Comparable<BM25Result> {
        private final String docId;
        private final double score;
        private final Episode.Entity entity;

        public BM25Result(String docId, double score, Episode.Entity entity) {
            this.docId = docId;
            this.score = score;
            this.entity = entity;
        }

        public String getDocId() {
            return docId;
        }

        public double getScore() {
            return score;
        }

        public Episode.Entity getEntity() {
            return entity;
        }

        @Override
        public int compareTo(BM25Result other) {
            return Double.compare(other.score, this.score);  // 降序
        }
    }

    /**
     * 构造函数（使用默认参数）
     */
    public BM25Retriever() {
        this(1.5, 0.75);
    }

    /**
     * 构造函数（自定义参数）
     * 
     * @param k1 词频饱和度参数 (推荐1.2-2.0)
     * @param b 长度归一化参数 (推荐0.75)
     */
    public BM25Retriever(double k1, double b) {
        this.k1 = k1;
        this.b = b;
        this.documents = new HashMap<>();
        this.termDocFreq = new HashMap<>();
        this.totalDocs = 0;
        this.avgDocLength = 0.0;
    }

    /**
     * 索引实体集合
     * 
     * @param entities 实体Map（entityId -> Entity）
     */
    public void indexEntities(Map<String, Episode.Entity> entities) {
        LOGGER.info("开始索引 {} 个实体", entities.size());
        
        documents.clear();
        termDocFreq.clear();
        
        long totalLength = 0;
        
        // 构建文档并统计词频
        for (Map.Entry<String, Episode.Entity> entry : entities.entrySet()) {
            Episode.Entity entity = entry.getValue();
            
            // 组合实体名称和类型作为文档内容
            String content = (entity.getName() != null ? entity.getName() : "") + 
                           " " + 
                           (entity.getType() != null ? entity.getType() : "");
            
            Document doc = new Document(entity.getId(), content);
            documents.put(entity.getId(), doc);
            totalLength += doc.getLength();
            
            // 统计词项文档频率
            for (String term : doc.getTermFreqs().keySet()) {
                termDocFreq.put(term, termDocFreq.getOrDefault(term, 0) + 1);
            }
        }
        
        totalDocs = documents.size();
        avgDocLength = totalDocs > 0 ? (double) totalLength / totalDocs : 0.0;
        
        LOGGER.info("索引完成: {} 个文档, 平均长度: {}, 词典大小: {}", 
            totalDocs, avgDocLength, termDocFreq.size());
    }

    /**
     * BM25检索
     * 
     * @param query 查询文本
     * @param entities 实体集合（用于返回完整实体信息）
     * @param topK 返回top K结果
     * @return BM25检索结果列表（按分数降序）
     */
    public List<BM25Result> search(String query, Map<String, Episode.Entity> entities, int topK) {
        if (query == null || query.isEmpty()) {
            return Collections.emptyList();
        }
        
        // 查询分词
        String[] queryTerms = query.toLowerCase()
            .replaceAll("[^a-z0-9\\s\\u4e00-\\u9fa5]", " ")
            .split("\\s+");
        
        Set<String> uniqueTerms = new HashSet<>();
        for (String term : queryTerms) {
            if (!term.isEmpty()) {
                uniqueTerms.add(term);
            }
        }
        
        if (uniqueTerms.isEmpty()) {
            return Collections.emptyList();
        }
        
        LOGGER.debug("查询分词结果: {} 个唯一词项", uniqueTerms.size());
        
        // 计算每个文档的BM25分数
        List<BM25Result> results = new ArrayList<>();
        
        for (Map.Entry<String, Document> entry : documents.entrySet()) {
            String docId = entry.getKey();
            Document doc = entry.getValue();
            
            double score = calculateBM25Score(uniqueTerms, doc);
            
            if (score > 0) {
                Episode.Entity entity = entities.get(docId);
                if (entity != null) {
                    results.add(new BM25Result(docId, score, entity));
                }
            }
        }
        
        // 排序并返回top K
        Collections.sort(results);
        
        if (results.size() > topK) {
            results = results.subList(0, topK);
        }
        
        LOGGER.info("BM25检索完成: 查询='{}', 返回 {} 个结果", query, results.size());
        
        return results;
    }

    /**
     * 计算单个文档的BM25分数
     */
    private double calculateBM25Score(Set<String> queryTerms, Document doc) {
        double score = 0.0;
        
        for (String term : queryTerms) {
            // 计算IDF
            int docFreq = termDocFreq.getOrDefault(term, 0);
            if (docFreq == 0) {
                continue;  // 词项不在任何文档中
            }
            
            double idf = Math.log((totalDocs - docFreq + 0.5) / (docFreq + 0.5) + 1.0);
            
            // 获取词频
            int termFreq = doc.getTermFreqs().getOrDefault(term, 0);
            if (termFreq == 0) {
                continue;  // 词项不在当前文档中
            }
            
            // 计算BM25分数
            double docLen = doc.getLength();
            double normDocLen = 1.0 - b + b * (docLen / avgDocLength);
            double tfComponent = (termFreq * (k1 + 1.0)) / (termFreq + k1 * normDocLen);
            
            score += idf * tfComponent;
        }
        
        return score;
    }

    /**
     * 获取统计信息
     */
    public Map<String, Object> getStats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("total_docs", totalDocs);
        stats.put("avg_doc_length", avgDocLength);
        stats.put("vocab_size", termDocFreq.size());
        stats.put("k1", k1);
        stats.put("b", b);
        return stats;
    }
}
