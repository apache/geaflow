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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.context.api.engine.ContextMemoryEngine;
import org.apache.geaflow.context.api.model.Episode;
import org.apache.geaflow.context.api.query.ContextQuery;
import org.apache.geaflow.context.api.result.ContextSearchResult;
import org.apache.geaflow.context.core.memory.EntityMemoryGraphManager;
import org.apache.geaflow.context.core.retriever.BM25Retriever;
import org.apache.geaflow.context.core.retriever.HybridFusion;
import org.apache.geaflow.context.core.retriever.KeywordRetriever;
import org.apache.geaflow.context.core.retriever.Retriever;
import org.apache.geaflow.context.core.storage.InMemoryStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of ContextMemoryEngine.
 * This is a Phase 1 implementation with in-memory storage.
 * Production deployments should extend this with persistent storage and vector indexes.
 */
public class DefaultContextMemoryEngine implements ContextMemoryEngine {

    private static final Logger logger = LoggerFactory.getLogger(DefaultContextMemoryEngine.class);

    private final ContextMemoryConfig config;
    private final InMemoryStore store;
    private final DefaultEmbeddingIndex embeddingIndex;
    private EntityMemoryGraphManager memoryGraphManager;  // 可选的实体记忆图谱
    private boolean initialized = false;
    private boolean enableMemoryGraph = false;  // 是否启用记忆图谱
    
    // Retriever抽象层（可扩展的检索器）
    private final Map<String, Retriever> retrievers;  // 检索器注册表
    private BM25Retriever bm25Retriever;  // BM25检索器
    private KeywordRetriever keywordRetriever;  // 关键词检索器

    /**
     * Constructor with configuration.
     *
     * @param config The configuration for the engine
     */
    public DefaultContextMemoryEngine(ContextMemoryConfig config) {
        this.config = config;
        this.store = new InMemoryStore();
        this.embeddingIndex = new DefaultEmbeddingIndex();
        this.retrievers = new HashMap<>();  // 初始化检索器注册表
    }

    /**
     * Initialize the engine.
     *
     * @throws Exception if initialization fails
     */
    @Override
    public void initialize() throws Exception {
        logger.info("Initializing DefaultContextMemoryEngine with config: {}", config);
        store.initialize();
        embeddingIndex.initialize();
        
        // 初始化检索器（Retriever抽象层）
        initializeRetrievers();
        
        // 初始化实体记忆图谱（如果启用）
        if (config.isEnableMemoryGraph()) {
            try {
                Configuration memoryGraphConfig = new Configuration();
                memoryGraphConfig.put("entity.memory.base_decay", 
                    String.valueOf(config.getMemoryGraphBaseDecay()));
                memoryGraphConfig.put("entity.memory.noise_threshold", 
                    String.valueOf(config.getMemoryGraphNoiseThreshold()));
                memoryGraphConfig.put("entity.memory.max_edges_per_node", 
                    String.valueOf(config.getMemoryGraphMaxEdges()));
                memoryGraphConfig.put("entity.memory.prune_interval", 
                    String.valueOf(config.getMemoryGraphPruneInterval()));
                
                memoryGraphManager = new EntityMemoryGraphManager(memoryGraphConfig);
                memoryGraphManager.initialize();
                enableMemoryGraph = true;
                logger.info("Entity memory graph enabled");
            } catch (Exception e) {
                logger.warn("Failed to initialize entity memory graph: {}", e.getMessage());
                enableMemoryGraph = false;
            }
        }
        
        initialized = true;
        logger.info("DefaultContextMemoryEngine initialized successfully");
    }
    
    /**
     * 初始化检索器（Retriever抽象层）
     */
    private void initializeRetrievers() {
        // 1. 初始化BM25检索器
        bm25Retriever = new BM25Retriever(
            config.getBm25K1(),
            config.getBm25B()
        );
        bm25Retriever.setEntityStore(store.getEntities());
        bm25Retriever.indexEntities(store.getEntities());
        registerRetriever("bm25", bm25Retriever);
        
        // 2. 初始化关键词检索器
        keywordRetriever = new KeywordRetriever(store.getEntities());
        registerRetriever("keyword", keywordRetriever);
        
        logger.info("Initialized {} retrievers", retrievers.size());
    }
    
    /**
     * 注册检索器（支持用户自定义扩展）
     */
    public void registerRetriever(String name, Retriever retriever) {
        retrievers.put(name, retriever);
        logger.debug("Registered retriever: {}", name);
    }
    
    /**
     * 获取检索器
     */
    public Retriever getRetriever(String name) {
        return retrievers.get(name);
    }

    /**
     * Ingest an episode into the context memory.
     *
     * @param episode The episode to ingest
     * @return Episode ID
     * @throws Exception if ingestion fails
     */
    @Override
    public String ingestEpisode(Episode episode) throws Exception {
        if (!initialized) {
            throw new IllegalStateException("Engine not initialized");
        }

        // Generate episode ID if not present
        if (episode.getEpisodeId() == null) {
            episode.setEpisodeId(UUID.randomUUID().toString());
        }

        long startTime = System.currentTimeMillis();

        try {
            // Store episode
            store.addEpisode(episode);

            // Index entities and relations
            if (episode.getEntities() != null) {
                for (Episode.Entity entity : episode.getEntities()) {
                    store.addEntity(entity.getId(), entity);
                }
            }

            if (episode.getRelations() != null) {
                for (Episode.Relation relation : episode.getRelations()) {
                    store.addRelation(relation.getSourceId() + "->" + relation.getTargetId(), relation);
                }
            }
            
            // 更新实体记忆图谱（如果启用）
            if (enableMemoryGraph && episode.getEntities() != null && !episode.getEntities().isEmpty()) {
                try {
                    List<String> entityIds = episode.getEntities().stream()
                        .map(Episode.Entity::getId)
                        .collect(Collectors.toList());
                    memoryGraphManager.addEntities(entityIds);
                    logger.debug("Added {} entities to memory graph", entityIds.size());
                } catch (Exception e) {
                    logger.warn("Failed to update memory graph: {}", e.getMessage());
                }
            }

            long elapsedTime = System.currentTimeMillis() - startTime;
            logger.info("Episode ingested successfully: {} (took {} ms)", episode.getEpisodeId(), elapsedTime);

            return episode.getEpisodeId();
        } catch (Exception e) {
            logger.error("Error ingesting episode", e);
            throw e;
        }
    }

    /**
     * Perform a hybrid retrieval query.
     *
     * @param query The context query
     * @return Search results
     * @throws Exception if query fails
     */
    @Override
    public ContextSearchResult search(ContextQuery query) throws Exception {
        if (!initialized) {
            throw new IllegalStateException("Engine not initialized");
        }

        long startTime = System.currentTimeMillis();
        ContextSearchResult result = new ContextSearchResult();

        try {
            switch (query.getStrategy()) {
                case VECTOR_ONLY:
                    vectorSearch(query, result);
                    break;
                case GRAPH_ONLY:
                    graphSearch(query, result);
                    break;
                case KEYWORD_ONLY:
                    keywordSearch(query, result);
                    break;
                case MEMORY_GRAPH:
                    memoryGraphSearch(query, result);
                    break;
                case BM25:
                    bm25Search(query, result);
                    break;
                case HYBRID_BM25_VECTOR:
                    hybridBM25VectorSearch(query, result);
                    break;
                case HYBRID_BM25_GRAPH:
                    hybridBM25GraphSearch(query, result);
                    break;
                case HYBRID:
                default:
                    hybridSearch(query, result);
                    break;
            }

            result.setExecutionTime(System.currentTimeMillis() - startTime);
            logger.info("Search completed: {} results in {} ms",
                    result.getEntities().size(), result.getExecutionTime());

            return result;
        } catch (Exception e) {
            logger.error("Error performing search", e);
            throw e;
        }
    }

    /**
     * Vector-only search.
     */
    private void vectorSearch(ContextQuery query, ContextSearchResult result) throws Exception {
        // In Phase 1, this is a placeholder
        // Real implementation would use vector similarity search
        logger.debug("Performing vector-only search");
    }

    /**
     * Graph-only search via traversal.
     */
    private void graphSearch(ContextQuery query, ContextSearchResult result) throws Exception {
        // In Phase 1, this is a placeholder
        // Real implementation would traverse the knowledge graph
        logger.debug("Performing graph-only search");
    }

    /**
     * Keyword search.
     */
    private void keywordSearch(ContextQuery query, ContextSearchResult result) throws Exception {
        // In Phase 1, simple keyword matching
        String queryText = query.getQueryText().toLowerCase();

        for (Map.Entry<String, Episode.Entity> entry : store.getEntities().entrySet()) {
            Episode.Entity entity = entry.getValue();
            if (entity.getName() != null && entity.getName().toLowerCase().contains(queryText)) {
                ContextSearchResult.ContextEntity contextEntity = new ContextSearchResult.ContextEntity(
                        entity.getId(), entity.getName(), entity.getType(), 0.5);
                result.addEntity(contextEntity);
            }
        }

        logger.debug("Keyword search found {} entities", result.getEntities().size());
    }
    
    /**
     * 实体记忆图谱搜索（基于 PMI 的记忆扩散）
     */
    private void memoryGraphSearch(ContextQuery query, ContextSearchResult result) throws Exception {
        if (!enableMemoryGraph) {
            logger.warn("Memory graph is not enabled, falling back to keyword search");
            keywordSearch(query, result);
            return;
        }
        
        // 1. 首先进行关键词搜索，获取种子实体
        List<String> seedEntityIds = new ArrayList<>();
        String queryText = query.getQueryText().toLowerCase();
        
        for (Map.Entry<String, Episode.Entity> entry : store.getEntities().entrySet()) {
            Episode.Entity entity = entry.getValue();
            if (entity.getName() != null && entity.getName().toLowerCase().contains(queryText)) {
                seedEntityIds.add(entity.getId());
            }
        }
        
        if (seedEntityIds.isEmpty()) {
            logger.debug("No seed entities found for query: {}", queryText);
            return;
        }
        
        // 2. 使用记忆图谱扩展相关实体
        try {
            int topK = query.getTopK() > 0 ? query.getTopK() : 10;
            List<EntityMemoryGraphManager.ExpandedEntity> expandedEntities = 
                memoryGraphManager.expandEntities(seedEntityIds, topK);
            
            logger.debug("Expanded from {} seeds to {} related entities", 
                seedEntityIds.size(), expandedEntities.size());
            
            // 3. 将扩展的实体转换为搜索结果
            for (EntityMemoryGraphManager.ExpandedEntity expanded : expandedEntities) {
                Episode.Entity entity = store.getEntities().get(expanded.getEntityId());
                if (entity != null) {
                    ContextSearchResult.ContextEntity contextEntity = 
                        new ContextSearchResult.ContextEntity(
                            entity.getId(), 
                            entity.getName(), 
                            entity.getType(), 
                            expanded.getActivationStrength()
                        );
                    result.addEntity(contextEntity);
                }
            }
            
            // 4. 添加种子实体（最高激活度）
            for (String seedId : seedEntityIds) {
                Episode.Entity entity = store.getEntities().get(seedId);
                if (entity != null) {
                    ContextSearchResult.ContextEntity contextEntity = 
                        new ContextSearchResult.ContextEntity(
                            entity.getId(), 
                            entity.getName(), 
                            entity.getType(), 
                            1.0  // 种子实体激活度最高
                        );
                    result.addEntity(contextEntity);
                }
            }
            
            logger.info("Memory graph search completed: {} total entities", 
                result.getEntities().size());
            
        } catch (Exception e) {
            logger.error("Memory graph search failed: {}", e.getMessage(), e);
            // 失败时退回到关键词搜索
            keywordSearch(query, result);
        }
    }

    /**
     * Hybrid search combining multiple strategies.
     */
    private void hybridSearch(ContextQuery query, ContextSearchResult result) throws Exception {
        // Start with keyword search in Phase 1
        keywordSearch(query, result);

        // Graph expansion (limited to maxHops)
        if (query.getMaxHops() > 0) {
            expandResultsViaGraph(result, query.getMaxHops());
        }
    }

    /**
     * Expand results by traversing graph relationships.
     */
    private void expandResultsViaGraph(ContextSearchResult result, int maxHops) {
        // In Phase 1, this is a placeholder
        logger.debug("Expanding results via graph traversal with maxHops={}", maxHops);
    }
    
    /**
     * BM25检索（使用Retriever抽象）
     */
    private void bm25Search(ContextQuery query, ContextSearchResult result) throws Exception {
        if (bm25Retriever == null || !bm25Retriever.isAvailable()) {
            logger.warn("BM25 retriever not available, falling back to keyword search");
            keywordSearch(query, result);
            return;
        }
        
        int topK = query.getTopK() > 0 ? query.getTopK() : 10;
        List<Retriever.RetrievalResult> retrievalResults = bm25Retriever.retrieve(query, topK);
        
        for (Retriever.RetrievalResult retrievalResult : retrievalResults) {
            Episode.Entity entity = store.getEntities().get(retrievalResult.getEntityId());
            if (entity != null) {
                result.addEntity(new ContextSearchResult.ContextEntity(
                    entity.getId(),
                    entity.getName(),
                    entity.getType(),
                    retrievalResult.getScore()
                ));
            }
        }
        
        logger.info("BM25 search: {} results", result.getEntities().size());
    }
    
    /**
     * BM25 + 向量混合检索 (RRF融合)
     */
    private void hybridBM25VectorSearch(ContextQuery query, ContextSearchResult result) throws Exception {
        int topK = query.getTopK() > 0 ? query.getTopK() : 10;
        
        // 1. BM25检索
        List<Retriever.RetrievalResult> bm25Results = 
            bm25Retriever != null ? bm25Retriever.retrieve(query, topK * 2) : new ArrayList<>();
        
        // 2. 向量检索 (Phase 1为占位符，使用关键词代替)
        List<Retriever.RetrievalResult> vectorResults = 
            keywordRetriever != null ? keywordRetriever.retrieve(query, topK * 2) : new ArrayList<>();
        
        // 3. 构建排名列表用于RRF融合
        Map<String, List<String>> rankedLists = new HashMap<>();
        
        List<String> bm25Ranked = bm25Results.stream()
            .map(Retriever.RetrievalResult::getEntityId)
            .collect(Collectors.toList());
        rankedLists.put("bm25", bm25Ranked);
        
        List<String> vectorRanked = vectorResults.stream()
            .map(Retriever.RetrievalResult::getEntityId)
            .collect(Collectors.toList());
        rankedLists.put("vector", vectorRanked);
        
        // 4. RRF融合
        List<HybridFusion.FusionResult> fusionResults = 
            HybridFusion.rrfFusion(rankedLists, 60, topK);
        
        // 5. 转换为搜索结果
        for (HybridFusion.FusionResult fusionResult : fusionResults) {
            Episode.Entity entity = store.getEntities().get(fusionResult.getId());
            if (entity != null) {
                result.addEntity(new ContextSearchResult.ContextEntity(
                    entity.getId(),
                    entity.getName(),
                    entity.getType(),
                    fusionResult.getScore()
                ));
            }
        }
        
        logger.info("Hybrid BM25+Vector search: {} results", result.getEntities().size());
    }
    
    /**
     * BM25 + 记忆图谱混合检索
     */
    private void hybridBM25GraphSearch(ContextQuery query, ContextSearchResult result) throws Exception {
        int topK = query.getTopK() > 0 ? query.getTopK() : 10;
        
        // 1. BM25检索获取候选实体
        List<Retriever.RetrievalResult> bm25Results = 
            bm25Retriever != null ? bm25Retriever.retrieve(query, topK) : new ArrayList<>();
        
        if (bm25Results.isEmpty() || !enableMemoryGraph) {
            // 退回到BM25单独检索
            bm25Search(query, result);
            return;
        }
        
        // 2. 提取top种子实体
        List<String> seedEntityIds = bm25Results.stream()
            .limit(5)  // 取top 5作为种子
            .map(Retriever.RetrievalResult::getEntityId)
            .collect(Collectors.toList());
        
        // 3. 记忆图谱扩散
        List<EntityMemoryGraphManager.ExpandedEntity> expandedEntities = 
            memoryGraphManager.expandEntities(seedEntityIds, topK);
        
        // 4. 归一化融合
        Map<String, Map<String, Double>> scoredResults = new HashMap<>();
        
        // BM25分数
        Map<String, Double> bm25Scores = new HashMap<>();
        for (Retriever.RetrievalResult r : bm25Results) {
            bm25Scores.put(r.getEntityId(), r.getScore());
        }
        scoredResults.put("bm25", bm25Scores);
        
        // 记忆图谱分数
        Map<String, Double> graphScores = new HashMap<>();
        for (EntityMemoryGraphManager.ExpandedEntity e : expandedEntities) {
            graphScores.put(e.getEntityId(), e.getActivationStrength());
        }
        scoredResults.put("graph", graphScores);
        
        // 5. 归一化加权融合 (BM25权重0.6, 图谱权重0.4)
        Map<String, Double> weights = new HashMap<>();
        weights.put("bm25", config.getBm25Weight());
        weights.put("graph", config.getGraphWeight());
        
        List<HybridFusion.FusionResult> fusionResults = 
            HybridFusion.normalizedFusion(scoredResults, weights, topK);
        
        // 6. 转换为搜索结果
        for (HybridFusion.FusionResult fusionResult : fusionResults) {
            Episode.Entity entity = store.getEntities().get(fusionResult.getId());
            if (entity != null) {
                result.addEntity(new ContextSearchResult.ContextEntity(
                    entity.getId(),
                    entity.getName(),
                    entity.getType(),
                    fusionResult.getScore()
                ));
            }
        }
        
        logger.info("Hybrid BM25+Graph search: {} results", result.getEntities().size());
    }

    /**
     * Create a context snapshot at specific timestamp.
     *
     * @param timestamp The timestamp
     * @return Context snapshot
     * @throws Exception if snapshot creation fails
     */
    @Override
    public ContextSnapshot createSnapshot(long timestamp) throws Exception {
        if (!initialized) {
            throw new IllegalStateException("Engine not initialized");
        }

        return new DefaultContextSnapshot(timestamp, store.getEntities(), store.getRelations());
    }

    /**
     * Get temporal graph for time range.
     *
     * @param filter Temporal filter
     * @return Temporal context graph
     * @throws Exception if query fails
     */
    @Override
    public TemporalContextGraph getTemporalGraph(ContextQuery.TemporalFilter filter) throws Exception {
        if (!initialized) {
            throw new IllegalStateException("Engine not initialized");
        }

        return new DefaultTemporalContextGraph(
                store.getEntities(),
                store.getRelations(),
                filter.getStartTime(),
                filter.getEndTime()
        );
    }

    /**
     * Get embedding index.
     *
     * @return Embedding index
     */
    @Override
    public EmbeddingIndex getEmbeddingIndex() {
        return embeddingIndex;
    }

    /**
     * Close and cleanup resources.
     *
     * @throws IOException if close fails
     */
    @Override
    public void close() throws IOException {
        logger.info("Closing DefaultContextMemoryEngine");
        
        // 关闭实体记忆图谱
        if (memoryGraphManager != null) {
            try {
                memoryGraphManager.close();
            } catch (Exception e) {
                logger.error("Error closing memory graph manager", e);
            }
        }
        
        if (store != null) {
            try {
                store.close();
            } catch (Exception e) {
                logger.error("Error closing store", e);
                if (e instanceof IOException) {
                    throw (IOException) e;
                }
            }
        }
        if (embeddingIndex != null) {
            try {
                embeddingIndex.close();
            } catch (Exception e) {
                logger.error("Error closing embedding index", e);
                if (e instanceof IOException) {
                    throw (IOException) e;
                }
            }
        }
        initialized = false;
        logger.info("DefaultContextMemoryEngine closed");
    }

    /**
     * Configuration for the context memory engine.
     */
    public static class ContextMemoryConfig {

        private String storageType = "memory";
        private String vectorIndexType = "memory";
        private int maxEpisodes = 10000;
        private int embeddingDimension = 768;
        
        // 实体记忆图谱配置
        private boolean enableMemoryGraph = false;
        private double memoryGraphBaseDecay = 0.6;
        private double memoryGraphNoiseThreshold = 0.2;
        private int memoryGraphMaxEdges = 30;
        private int memoryGraphPruneInterval = 1000;
        
        // BM25参数配置
        private double bm25K1 = 1.5;
        private double bm25B = 0.75;
        
        // 混合检索权重配置
        private double bm25Weight = 0.6;
        private double graphWeight = 0.4;

        public ContextMemoryConfig() {
        }

        public String getStorageType() {
            return storageType;
        }

        public void setStorageType(String storageType) {
            this.storageType = storageType;
        }

        public String getVectorIndexType() {
            return vectorIndexType;
        }

        public void setVectorIndexType(String vectorIndexType) {
            this.vectorIndexType = vectorIndexType;
        }

        public int getMaxEpisodes() {
            return maxEpisodes;
        }

        public void setMaxEpisodes(int maxEpisodes) {
            this.maxEpisodes = maxEpisodes;
        }

        public int getEmbeddingDimension() {
            return embeddingDimension;
        }

        public void setEmbeddingDimension(int embeddingDimension) {
            this.embeddingDimension = embeddingDimension;
        }
        
        public boolean isEnableMemoryGraph() {
            return enableMemoryGraph;
        }
        
        public void setEnableMemoryGraph(boolean enableMemoryGraph) {
            this.enableMemoryGraph = enableMemoryGraph;
        }
        
        public double getMemoryGraphBaseDecay() {
            return memoryGraphBaseDecay;
        }
        
        public void setMemoryGraphBaseDecay(double memoryGraphBaseDecay) {
            this.memoryGraphBaseDecay = memoryGraphBaseDecay;
        }
        
        public double getMemoryGraphNoiseThreshold() {
            return memoryGraphNoiseThreshold;
        }
        
        public void setMemoryGraphNoiseThreshold(double memoryGraphNoiseThreshold) {
            this.memoryGraphNoiseThreshold = memoryGraphNoiseThreshold;
        }
        
        public int getMemoryGraphMaxEdges() {
            return memoryGraphMaxEdges;
        }
        
        public void setMemoryGraphMaxEdges(int memoryGraphMaxEdges) {
            this.memoryGraphMaxEdges = memoryGraphMaxEdges;
        }
        
        public int getMemoryGraphPruneInterval() {
            return memoryGraphPruneInterval;
        }
        
        public void setMemoryGraphPruneInterval(int memoryGraphPruneInterval) {
            this.memoryGraphPruneInterval = memoryGraphPruneInterval;
        }
        
        public double getBm25K1() {
            return bm25K1;
        }
        
        public void setBm25K1(double bm25K1) {
            this.bm25K1 = bm25K1;
        }
        
        public double getBm25B() {
            return bm25B;
        }
        
        public void setBm25B(double bm25B) {
            this.bm25B = bm25B;
        }
        
        public double getBm25Weight() {
            return bm25Weight;
        }
        
        public void setBm25Weight(double bm25Weight) {
            this.bm25Weight = bm25Weight;
        }
        
        public double getGraphWeight() {
            return graphWeight;
        }
        
        public void setGraphWeight(double graphWeight) {
            this.graphWeight = graphWeight;
        }

        @Override
        public String toString() {
            return new StringBuilder()
                    .append("ContextMemoryConfig{")
                    .append("storageType='")
                    .append(storageType)
                    .append("', vectorIndexType='")
                    .append(vectorIndexType)
                    .append("', maxEpisodes=")
                    .append(maxEpisodes)
                    .append(", embeddingDimension=")
                    .append(embeddingDimension)
                    .append("}")
                    .toString();
        }
    }

    /**
     * Default implementation of ContextSnapshot.
     */
    public static class DefaultContextSnapshot implements ContextSnapshot {

        private final long timestamp;
        private final Map<String, Episode.Entity> entities;
        private final Map<String, Episode.Relation> relations;

        public DefaultContextSnapshot(long timestamp, Map<String, Episode.Entity> entities,
                                     Map<String, Episode.Relation> relations) {
            this.timestamp = timestamp;
            this.entities = new HashMap<>(entities);
            this.relations = new HashMap<>(relations);
        }

        @Override
        public long getTimestamp() {
            return timestamp;
        }

        @Override
        public Object getVertices() {
            return entities;
        }

        @Override
        public Object getEdges() {
            return relations;
        }
    }

    /**
     * Default implementation of TemporalContextGraph.
     */
    public static class DefaultTemporalContextGraph implements TemporalContextGraph {

        private final Map<String, Episode.Entity> entities;
        private final Map<String, Episode.Relation> relations;
        private final long startTime;
        private final long endTime;

        public DefaultTemporalContextGraph(Map<String, Episode.Entity> entities,
                                          Map<String, Episode.Relation> relations,
                                          long startTime, long endTime) {
            this.entities = entities;
            this.relations = relations;
            this.startTime = startTime;
            this.endTime = endTime;
        }

        @Override
        public Object getVertices() {
            return entities;
        }

        @Override
        public Object getEdges() {
            return relations;
        }

        @Override
        public long getStartTime() {
            return startTime;
        }

        @Override
        public long getEndTime() {
            return endTime;
        }
    }
}
