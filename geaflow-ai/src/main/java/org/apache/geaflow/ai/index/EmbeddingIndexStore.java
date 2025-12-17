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
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.geaflow.ai.common.model.ChatRobot;
import org.apache.geaflow.ai.common.model.ModelInfo;
import org.apache.geaflow.ai.common.model.ModelUtils;
import org.apache.geaflow.ai.graph.GraphAccessor;
import org.apache.geaflow.ai.graph.GraphEdge;
import org.apache.geaflow.ai.graph.GraphEntity;
import org.apache.geaflow.ai.graph.GraphVertex;
import org.apache.geaflow.ai.index.vector.EmbeddingVector;
import org.apache.geaflow.ai.index.vector.IVector;
import org.apache.geaflow.ai.verbalization.VerbalizationFunction;

import java.io.*;
import java.nio.charset.Charset;
import java.util.*;

public class EmbeddingIndexStore implements IndexStore {

    private GraphAccessor graphAccessor;
    private VerbalizationFunction verbFunc;
    private String indexFilePath;
    private ModelInfo modelInfo;
    private Map<GraphEntity, List<ChatRobot.EmbeddingResult>> indexStoreMap;

    public void initStore(GraphAccessor graphAccessor, VerbalizationFunction func,
                          String indexFilePath, ModelInfo modelInfo) {
        this.graphAccessor = graphAccessor;
        this.verbFunc = func;
        this.indexFilePath = indexFilePath;
        this.modelInfo = modelInfo;
        this.indexStoreMap = new HashMap<>();

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
        System.out.println("Success to scan entities. total entities num: " + key2EntityMap.size());

        try {
            File indexFile = new File(this.indexFilePath);

            if (!indexFile.exists()) {
                File parentDir = indexFile.getParentFile();
                if (parentDir != null && !parentDir.exists()) {
                    parentDir.mkdirs();
                }
                indexFile.createNewFile();
                System.out.println("Success to create new index store file. Path: " + this.indexFilePath);
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
                if (line.isEmpty()) {
                    continue;
                }
                try {
                    ChatRobot.EmbeddingResult embedding =
                            new Gson().fromJson(line, ChatRobot.EmbeddingResult.class);
                    String key = embedding.input;
                    GraphEntity entity = key2EntityMap.get(key);
                    if (entity != null) {
                        this.indexStoreMap.computeIfAbsent(entity, k -> new ArrayList<>()).add(embedding);
                    }
                    count++;
                } catch (Throwable e) {
                    System.out.println("Cannot parse embedding item: " + line);
                }
            }
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }

        System.out.println("Success to read index store file. items num: " + count);
        System.out.println("Success to rebuild index with file. index num: " + this.indexStoreMap.size());


        //Scan entities in the graph, make new index items
        ChatRobot chatRobot = new ChatRobot();
        chatRobot.setModelInfo(modelInfo);

        final int BATCH_SIZE = 32;
        List<GraphEntity> pendingEntities = new ArrayList<>(BATCH_SIZE);
        Set<GraphEntity> batchEntitiesBuffer = new HashSet<>(BATCH_SIZE);
        List<String> result = new ArrayList<>();
        final int REPORT_SIZE = 100;
        long reportedCount = this.indexStoreMap.size();
        long addedCount = this.indexStoreMap.size();
        for (Iterator<GraphVertex> itV = graphAccessor.scanVertex(); itV.hasNext();) {
            GraphVertex vertex = itV.next();

            // Scan vertices or edges, skip already indexed data,
            // add un-indexed data to batch processing collection
            if (!indexStoreMap.containsKey(vertex) && !batchEntitiesBuffer.contains(vertex)) {
                batchEntitiesBuffer.add(vertex);
                pendingEntities.add(vertex);
                if (pendingEntities.size() >= BATCH_SIZE) {
                    result.addAll(indexBatch(chatRobot, pendingEntities));
                    flushBatchIndex(result, false);
                    pendingEntities.clear();
                    batchEntitiesBuffer.clear();
                    addedCount += BATCH_SIZE;
                }
            }

            for (Iterator<GraphEdge> itE = graphAccessor.scanEdge(vertex); itE.hasNext();) {
                GraphEdge edge = itE.next();
                if (!indexStoreMap.containsKey(edge) && !batchEntitiesBuffer.contains(edge)) {
                    batchEntitiesBuffer.add(edge);
                    pendingEntities.add(edge);
                    if (pendingEntities.size() >= BATCH_SIZE) {
                        result.addAll(indexBatch(chatRobot, pendingEntities));
                        flushBatchIndex(result, false);
                        pendingEntities.clear();
                        batchEntitiesBuffer.clear();
                        addedCount += BATCH_SIZE;
                    }
                }
            }
            if (addedCount - reportedCount > REPORT_SIZE) {
                System.out.println("added batch index. added num: " + addedCount);
                reportedCount = addedCount;
            }
        }
        if (pendingEntities.size() > 0) {
            result.addAll(indexBatch(chatRobot, pendingEntities));
            flushBatchIndex(result, true);
            addedCount += pendingEntities.size();
            pendingEntities.clear();
            batchEntitiesBuffer.clear();
        }

        System.out.printf("Successfully added %d new index items. Total indexed: %d%n",
                addedCount, indexStoreMap.size());
    }

    private List<String> indexBatch(ChatRobot chatRobot, List<GraphEntity> pendingEntities) {
        if (pendingEntities == null || chatRobot == null || pendingEntities.isEmpty()) {
            return new ArrayList<>();
        }
        List<String> pendingTexts = new ArrayList<>(pendingEntities.size());
        Map<GraphEntity, Pair<Integer, Integer>> entity2StartEndPair = new HashMap<>();
        for (GraphEntity e : pendingEntities) {
            Integer start = pendingTexts.size();
            pendingTexts.addAll(ModelUtils.splitLongText(128,
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
            String embeddingResultStr = chatRobot.embedding(textsArray);
            List<String> splitResults = Arrays.asList(embeddingResultStr.trim().split("\n"));
            result.addAll(splitResults);

        }

        List<String> formatResult = new ArrayList<>();
        for (Map.Entry<GraphEntity, Pair<Integer, Integer>> entry : entity2StartEndPair.entrySet()) {
            GraphEntity e = entry.getKey();
            List<ChatRobot.EmbeddingResult> embeddings = new ArrayList<>();
            for (int i = entry.getValue().getLeft(); i < entry.getValue().getRight(); i++) {
                if (StringUtils.isNotBlank(result.get(i))) {
                    ChatRobot.EmbeddingResult res = gson.fromJson(result.get(i),
                            ChatRobot.EmbeddingResult.class);
                    res.input = ModelUtils.getGraphEntityKey(e);
                    formatResult.add(gson.toJson(res));
                    embeddings.add(res);
                }
            }
            indexStoreMap.put(e, embeddings);
        }
        return formatResult;
    }

    private void flushBatchIndex(List<String> newItemStrings, boolean force) {
        final int WRITE_SIZE = 1024;
        if (force || newItemStrings.size() >= WRITE_SIZE) {
            try (FileWriter fw = new FileWriter(this.indexFilePath, true);
                 BufferedWriter writer = new BufferedWriter(fw);
                 PrintWriter out = new PrintWriter(writer)) {
                for (String item : newItemStrings) {
                    out.println(item);
                }
                System.out.println("Success to append " + newItemStrings.size() + " new index items to file.");
            } catch (IOException e) {
                throw new RuntimeException("Failed to append to index file: " + this.indexFilePath, e);
            }
            newItemStrings.clear();
        }
    }

    @Override
    public List<IVector> getEntityIndex(GraphEntity entity) {
        if (entity != null && indexStoreMap.get(entity) != null) {
            List<ChatRobot.EmbeddingResult> resultList = indexStoreMap.get(entity);
            List<IVector> result = new ArrayList<>();
            for (ChatRobot.EmbeddingResult res : resultList) {
                double[] embedding = res.embedding;
                result.add(new EmbeddingVector(embedding));
            }
            return result;
        }
        return Collections.emptyList();
    }
}
