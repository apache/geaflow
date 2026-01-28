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

package org.apache.geaflow.ai.index;

import com.google.gson.Gson;
import java.io.*;
import java.nio.charset.Charset;
import java.util.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.geaflow.ai.common.config.Constants;
import org.apache.geaflow.ai.common.model.EmbeddingService;
import org.apache.geaflow.ai.common.model.ModelConfig;
import org.apache.geaflow.ai.common.model.ModelUtils;
import org.apache.geaflow.ai.graph.GraphAccessor;
import org.apache.geaflow.ai.graph.GraphEdge;
import org.apache.geaflow.ai.graph.GraphEntity;
import org.apache.geaflow.ai.graph.GraphVertex;
import org.apache.geaflow.ai.index.vector.EmbeddingVector;
import org.apache.geaflow.ai.index.vector.IVector;
import org.apache.geaflow.ai.verbalization.VerbalizationFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EmbeddingIndexStore implements IndexStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(EmbeddingIndexStore.class);

    private GraphAccessor graphAccessor;
    private VerbalizationFunction verbFunc;
    private String indexFilePath;
    private ModelConfig modelConfig;
    private Map<GraphEntity, List<EmbeddingService.EmbeddingResult>> indexStoreMap;
    private Map<String, EmbeddingService.EmbeddingResult> embeddingResultCache;
    private List<String> flushBuffer = new ArrayList<>();
    private EmbeddingService service = new EmbeddingService();

    public void initStore(GraphAccessor graphAccessor, VerbalizationFunction func,
                          String indexFilePath, ModelConfig modelInfo) {
        this.graphAccessor = graphAccessor;
        this.verbFunc = func;
        this.indexFilePath = indexFilePath;
        this.modelConfig = modelInfo;
        this.indexStoreMap = new HashMap<>();
        this.embeddingResultCache = new HashMap<>();

        //Read index items from indexFilePath
        Map<String, GraphEntity> key2EntityMap = new HashMap<>();
        for (Iterator<GraphVertex> itV = this.graphAccessor.scanVertex(); itV.hasNext(); ) {
            GraphVertex vertex = itV.next();
            key2EntityMap.put(ModelUtils.getGraphEntityKey(vertex), vertex);
            for (Iterator<GraphEdge> itE = this.graphAccessor.scanEdge(vertex); itE.hasNext(); ) {
                GraphEdge edge = itE.next();
                key2EntityMap.put(ModelUtils.getGraphEntityKey(edge), edge);
            }
        }
        LOGGER.info("Success to scan entities. total entities num: " + key2EntityMap.size());

        try {
            File indexFile = new File(this.indexFilePath);

            if (!indexFile.exists()) {
                File parentDir = indexFile.getParentFile();
                if (parentDir != null && !parentDir.exists()) {
                    parentDir.mkdirs();
                }
                indexFile.createNewFile();
                LOGGER.info("Success to create new index store file. Path: " + this.indexFilePath);
            }
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }


        long count = 0;
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(
                        new FileInputStream(this.indexFilePath),
                        Charset.defaultCharset()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (StringUtils.isBlank(line)) {
                    continue;
                }
                try {
                    EmbeddingService.EmbeddingResult embedding =
                            new Gson().fromJson(line, EmbeddingService.EmbeddingResult.class);
                    String key = embedding.input;
                    GraphEntity entity = key2EntityMap.get(key);
                    if (entity != null) {
                        this.indexStoreMap.computeIfAbsent(entity, k -> new ArrayList<>()).add(embedding);
                    } else {
                        this.embeddingResultCache.put(key, embedding);
                    }
                    count++;
                } catch (Throwable e) {
                    LOGGER.info("Cannot parse embedding item: " + line);
                }
            }
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }

        LOGGER.info("Success to read index store file. items num: " + count);
        LOGGER.info("Success to rebuild index with file. index num: " + this.indexStoreMap.size());
        LOGGER.info("Success to rebuild embedding cache with file. index num: " + this.embeddingResultCache.size());


        //Scan entities in the graph, make new index items
        service.setModelConfig(this.modelConfig);

        final int BATCH_SIZE = Constants.EMBEDDING_INDEX_STORE_BATCH_SIZE;
        List<GraphEntity> pendingEntities = new ArrayList<>(BATCH_SIZE);
        Set<GraphEntity> batchEntitiesBuffer = new HashSet<>(BATCH_SIZE);
        final int REPORT_SIZE = Constants.EMBEDDING_INDEX_STORE_REPORT_SIZE;
        long reportedCount = this.indexStoreMap.size();
        long addedCount = this.indexStoreMap.size();
        for (Iterator<GraphVertex> itV = graphAccessor.scanVertex(); itV.hasNext(); ) {
            GraphVertex vertex = itV.next();

            // Scan vertices or edges, skip already indexed data,
            // add un-indexed data to batch processing collection
            List<String> verVertex = verbFunc.verbalize(vertex);
            if (verVertex.stream().allMatch(s -> embeddingResultCache.containsKey(s))) {
                for (String str : verVertex) {
                    this.indexStoreMap.computeIfAbsent(vertex, k -> new ArrayList<>())
                        .add(embeddingResultCache.get(str));
                }
            } else if (!indexStoreMap.containsKey(vertex) && !batchEntitiesBuffer.contains(vertex)) {
                batchEntitiesBuffer.add(vertex);
                pendingEntities.add(vertex);
                if (pendingEntities.size() >= BATCH_SIZE) {
                    flushBuffer.addAll(indexBatch(pendingEntities));
                    flushBatchIndex(false);
                    pendingEntities.clear();
                    batchEntitiesBuffer.clear();
                    addedCount += BATCH_SIZE;
                }
            }

            for (Iterator<GraphEdge> itE = graphAccessor.scanEdge(vertex); itE.hasNext(); ) {
                GraphEdge edge = itE.next();
                List<String> verEdge = verbFunc.verbalize(edge);
                if (verEdge.stream().allMatch(s -> embeddingResultCache.containsKey(s))) {
                    for (String str : verEdge) {
                        this.indexStoreMap.computeIfAbsent(edge, k -> new ArrayList<>())
                            .add(embeddingResultCache.get(str));
                    }
                } else if (!indexStoreMap.containsKey(edge) && !batchEntitiesBuffer.contains(edge)) {
                    batchEntitiesBuffer.add(edge);
                    pendingEntities.add(edge);
                    if (pendingEntities.size() >= BATCH_SIZE) {
                        flushBuffer.addAll(indexBatch(pendingEntities));
                        flushBatchIndex(false);
                        pendingEntities.clear();
                        batchEntitiesBuffer.clear();
                        addedCount += BATCH_SIZE;
                    }
                }
            }
            if (addedCount - reportedCount > REPORT_SIZE) {
                LOGGER.info("added batch index. added num: " + addedCount);
                reportedCount = addedCount;
            }
        }
        if (pendingEntities.size() > 0) {
            flushBuffer.addAll(indexBatch(pendingEntities));
            flushBatchIndex(true);
            addedCount += pendingEntities.size();
            pendingEntities.clear();
            batchEntitiesBuffer.clear();
        }

        LOGGER.info("Successfully added {} new index items. Total indexed: {}",
                addedCount, indexStoreMap.size());
    }

    private List<String> indexBatch(List<GraphEntity> pendingEntities) {
        if (pendingEntities == null || service == null || pendingEntities.isEmpty()) {
            return new ArrayList<>();
        }
        List<String> pendingTexts = new ArrayList<>(pendingEntities.size());
        Map<GraphEntity, Pair<Integer, Integer>> entity2StartEndPair = new HashMap<>();
        for (GraphEntity e : pendingEntities) {
            Integer start = pendingTexts.size();
            pendingTexts.addAll(ModelUtils.splitLongText(
                Constants.EMBEDDING_INDEX_STORE_SPLIT_TEXT_CHUNK_SIZE,
                    verbFunc.verbalize(e).toArray(new String[0])));
            Integer end = pendingTexts.size();
            entity2StartEndPair.put(e, Pair.of(start, end));
        }

        Gson gson = new Gson();
        int batchSize = pendingEntities.size();
        List<String> result = new ArrayList<>();
        List<String> pendingTextsList = new ArrayList<>(pendingTexts);

        for (int i = 0; i < pendingTextsList.size(); i += batchSize) {
            int end = Math.min(i + batchSize, pendingTextsList.size());
            List<String> batch = pendingTextsList.subList(i, end);
            String[] textsArray = batch.toArray(new String[0]);
            String embeddingResultStr = service.embedding(textsArray);
            List<String> splitResults = Arrays.asList(embeddingResultStr.trim().split("\n"));
            result.addAll(splitResults);

        }

        List<String> formatResult = new ArrayList<>();
        for (Map.Entry<GraphEntity, Pair<Integer, Integer>> entry : entity2StartEndPair.entrySet()) {
            GraphEntity e = entry.getKey();
            List<EmbeddingService.EmbeddingResult> embeddings = new ArrayList<>();
            for (int i = entry.getValue().getLeft(); i < entry.getValue().getRight(); i++) {
                if (StringUtils.isNotBlank(result.get(i))) {
                    EmbeddingService.EmbeddingResult res = gson.fromJson(result.get(i),
                        EmbeddingService.EmbeddingResult.class);
                    res.input = ModelUtils.getGraphEntityKey(e);
                    formatResult.add(gson.toJson(res));
                    embeddings.add(res);
                }
            }
            indexStoreMap.put(e, embeddings);
        }
        return formatResult;
    }

    private void flushBatchIndex(boolean force) {
        final int WRITE_SIZE = Constants.EMBEDDING_INDEX_STORE_FLUSH_WRITE_SIZE;
        if (force || flushBuffer.size() >= WRITE_SIZE) {
            try (FileWriter fw = new FileWriter(this.indexFilePath, true);
                 BufferedWriter writer = new BufferedWriter(fw);
                 PrintWriter out = new PrintWriter(writer)) {
                for (String item : flushBuffer) {
                    out.println(item);
                }
                LOGGER.info("Success to append " + flushBuffer.size() + " new index items to file.");
            } catch (IOException e) {
                throw new RuntimeException("Failed to append to index file: " + this.indexFilePath, e);
            }
            flushBuffer.clear();
        }
    }

    @Override
    public List<IVector> getEntityIndex(GraphEntity entity) {
        if (entity != null && indexStoreMap.get(entity) != null) {
            List<EmbeddingService.EmbeddingResult> resultList = indexStoreMap.get(entity);
            List<IVector> result = new ArrayList<>();
            for (EmbeddingService.EmbeddingResult res : resultList) {
                double[] embedding = res.embedding;
                result.add(new EmbeddingVector(embedding));
            }
            return result;
        }
        return Collections.emptyList();
    }

    @Override
    public List<IVector> getStringIndex(String str) {
        if (embeddingResultCache.containsKey(str)) {
            EmbeddingService.EmbeddingResult embedding = embeddingResultCache.get(str);
            List<IVector> result = new ArrayList<>();
            result.add(new EmbeddingVector(embedding.embedding));
            return result;
        }
        String embeddingResultStr = service.embedding(str);
        EmbeddingService.EmbeddingResult embedding =
            new Gson().fromJson(embeddingResultStr, EmbeddingService.EmbeddingResult.class);
        embeddingResultCache.put(str, embedding);
        flushBuffer.add(embeddingResultStr);
        flushBatchIndex(true);
        List<IVector> result = new ArrayList<>();
        result.add(new EmbeddingVector(embedding.embedding));
        return result;
    }
}
