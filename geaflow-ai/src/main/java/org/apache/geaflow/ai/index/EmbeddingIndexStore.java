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
    /**
     * Keyed by {@link ModelUtils#getGraphEntityKey}, the same string the index file is keyed by, so
     * that one notion of which entity a vector belongs to serves both. Keying it by the entity would
     * hand that decision to {@code GraphVertex.equals}, which answers on the id and the label and
     * would take the index with it if it ever came to answer on the values as well.
     *
     * <p>Built aside and assigned once, so a reader during initStore sees the index it had before
     * rather than one half way through being built, and empty until there has been one.
     */
    private volatile Map<String, List<EmbeddingService.EmbeddingResult>> indexStoreMap =
            Collections.emptyMap();

    public void initStore(GraphAccessor graphAccessor, VerbalizationFunction func,
                          String indexFilePath, ModelConfig modelInfo) {
        this.graphAccessor = graphAccessor;
        this.verbFunc = func;
        this.indexFilePath = indexFilePath;
        this.modelConfig = modelInfo;
        Map<String, List<EmbeddingService.EmbeddingResult>> loading = new HashMap<>();

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


        // A record is accepted only when its fingerprint matches the text the entity would be
        // embedded from now. The key tells which entity a vector belongs to but nothing about the
        // value it came from, so without this an entity that kept its id and changed its value
        // would count as indexed and keep the vector of the value it no longer has.
        long count = 0;
        long staleRecords = 0;
        long unversionedRecords = 0;
        Map<String, String> key2Fingerprint = new HashMap<>();
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
                EmbeddingService.EmbeddingResult embedding;
                try {
                    embedding = new Gson().fromJson(line, EmbeddingService.EmbeddingResult.class);
                } catch (Throwable e) {
                    // Only the parse is guarded. What follows verbalises the entity, and a
                    // verbaliser that throws must not be reported as a malformed file.
                    LOGGER.info("Cannot parse embedding item: " + line);
                    continue;
                }
                String key = embedding == null ? null : embedding.input;
                GraphEntity entity = key == null ? null : key2EntityMap.get(key);
                if (entity != null) {
                    if (embedding.contentHash == null) {
                        unversionedRecords++;
                    } else if (embedding.contentHash.equals(
                            currentFingerprint(key, entity, key2Fingerprint))) {
                        loading.computeIfAbsent(key, k -> new ArrayList<>()).add(embedding);
                    } else {
                        staleRecords++;
                    }
                }
                count++;
            }
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }

        LOGGER.info("Success to read index store file. items num: " + count);
        if (staleRecords > 0) {
            LOGGER.info("{} records were produced from text their entity no longer has. Not "
                    + "loading them, so those entities are embedded again.", staleRecords);
        }
        if (unversionedRecords > 0) {
            // Written before the fingerprint existed, so there is no way to tell whether they match
            // the current value. Trusting them would keep exactly the staleness this check is for.
            LOGGER.warn("{} records carry no fingerprint, so the text they were produced from "
                    + "cannot be established. Not loading them, which costs one round of embedding, "
                    + "after which they carry one.", unversionedRecords);
        }
        LOGGER.info("Success to rebuild index with file. index num: " + loading.size());


        //Scan entities in the graph, make new index items
        EmbeddingService embeddingService = new EmbeddingService();
        embeddingService.setModelConfig(modelInfo);

        final int BATCH_SIZE = Constants.EMBEDDING_INDEX_STORE_BATCH_SIZE;
        List<GraphEntity> pendingEntities = new ArrayList<>(BATCH_SIZE);
        Set<String> batchEntitiesBuffer = new HashSet<>(BATCH_SIZE);
        List<String> result = new ArrayList<>();
        final int REPORT_SIZE = Constants.EMBEDDING_INDEX_STORE_REPORT_SIZE;
        long reportedCount = loading.size();
        long addedCount = loading.size();
        for (Iterator<GraphVertex> itV = graphAccessor.scanVertex(); itV.hasNext(); ) {
            GraphVertex vertex = itV.next();

            // Scan vertices or edges, skip already indexed data,
            // add un-indexed data to batch processing collection
            String vertexKey = ModelUtils.getGraphEntityKey(vertex);
            if (!loading.containsKey(vertexKey) && !batchEntitiesBuffer.contains(vertexKey)) {
                batchEntitiesBuffer.add(vertexKey);
                pendingEntities.add(vertex);
                if (pendingEntities.size() >= BATCH_SIZE) {
                    result.addAll(indexBatch(embeddingService, pendingEntities, loading));
                    flushBatchIndex(result, false);
                    pendingEntities.clear();
                    batchEntitiesBuffer.clear();
                    addedCount += BATCH_SIZE;
                }
            }

            for (Iterator<GraphEdge> itE = graphAccessor.scanEdge(vertex); itE.hasNext(); ) {
                GraphEdge edge = itE.next();
                String edgeKey = ModelUtils.getGraphEntityKey(edge);
                if (!loading.containsKey(edgeKey) && !batchEntitiesBuffer.contains(edgeKey)) {
                    batchEntitiesBuffer.add(edgeKey);
                    pendingEntities.add(edge);
                    if (pendingEntities.size() >= BATCH_SIZE) {
                        result.addAll(indexBatch(embeddingService, pendingEntities, loading));
                        flushBatchIndex(result, false);
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
            result.addAll(indexBatch(embeddingService, pendingEntities, loading));
            flushBatchIndex(result, true);
            addedCount += pendingEntities.size();
            pendingEntities.clear();
            batchEntitiesBuffer.clear();
        }

        this.indexStoreMap = loading;
        LOGGER.info("Successfully added {} new index items. Total indexed: {}",
                addedCount, loading.size());
    }

    /**
     * The fingerprint of what the entity would be embedded from now, worked out once per entity
     * since one file holds every chunk of it as a record of its own. This takes
     * {@link VerbalizationFunction#verbalize(GraphEntity)} to be a function of the entity alone: one
     * that answers differently for an unchanged entity would have its vectors produced again on
     * every run.
     */
    private String currentFingerprint(String key, GraphEntity entity, Map<String, String> memo) {
        String fingerprint = memo.get(key);
        if (fingerprint == null) {
            fingerprint = ModelUtils.getEmbeddedTextFingerprint(embeddedChunks(this.verbFunc, entity));
            memo.put(key, fingerprint);
        }
        return fingerprint;
    }

    /** The texts an entity is embedded from, which is also what its fingerprint covers. */
    private static List<String> embeddedChunks(VerbalizationFunction func, GraphEntity entity) {
        return ModelUtils.splitLongText(Constants.EMBEDDING_INDEX_STORE_SPLIT_TEXT_CHUNK_SIZE,
                func.verbalize(entity).toArray(new String[0]));
    }

    private List<String> indexBatch(EmbeddingService service, List<GraphEntity> pendingEntities,
                                    Map<String, List<EmbeddingService.EmbeddingResult>> loading) {
        if (pendingEntities == null || service == null || pendingEntities.isEmpty()) {
            return new ArrayList<>();
        }
        List<String> pendingTexts = new ArrayList<>(pendingEntities.size());
        Map<GraphEntity, Pair<Integer, Integer>> entity2StartEndPair = new HashMap<>();
        Map<GraphEntity, String> entity2Fingerprint = new HashMap<>();
        for (GraphEntity e : pendingEntities) {
            Integer start = pendingTexts.size();
            List<String> chunks = embeddedChunks(verbFunc, e);
            pendingTexts.addAll(chunks);
            Integer end = pendingTexts.size();
            entity2StartEndPair.put(e, Pair.of(start, end));
            entity2Fingerprint.put(e, ModelUtils.getEmbeddedTextFingerprint(chunks));
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
            String key = ModelUtils.getGraphEntityKey(e);
            List<EmbeddingService.EmbeddingResult> embeddings = new ArrayList<>();
            for (int i = entry.getValue().getLeft(); i < entry.getValue().getRight(); i++) {
                if (StringUtils.isNotBlank(result.get(i))) {
                    EmbeddingService.EmbeddingResult res = gson.fromJson(result.get(i),
                        EmbeddingService.EmbeddingResult.class);
                    res.input = key;
                    res.contentHash = entity2Fingerprint.get(e);
                    formatResult.add(gson.toJson(res));
                    embeddings.add(res);
                }
            }
            loading.put(key, embeddings);
        }
        return formatResult;
    }

    private void flushBatchIndex(List<String> newItemStrings, boolean force) {
        final int WRITE_SIZE = Constants.EMBEDDING_INDEX_STORE_FLUSH_WRITE_SIZE;
        if (force || newItemStrings.size() >= WRITE_SIZE) {
            try (FileWriter fw = new FileWriter(this.indexFilePath, true);
                 BufferedWriter writer = new BufferedWriter(fw);
                 PrintWriter out = new PrintWriter(writer)) {
                for (String item : newItemStrings) {
                    out.println(item);
                }
                LOGGER.info("Success to append " + newItemStrings.size() + " new index items to file.");
            } catch (IOException e) {
                throw new RuntimeException("Failed to append to index file: " + this.indexFilePath, e);
            }
            newItemStrings.clear();
        }
    }

    @Override
    public List<IVector> getEntityIndex(GraphEntity entity) {
        if (entity != null) {
            // Read once: the field is replaced wholesale when an index is built.
            List<EmbeddingService.EmbeddingResult> resultList =
                    indexStoreMap.get(ModelUtils.getGraphEntityKey(entity));
            if (resultList != null) {
                List<IVector> result = new ArrayList<>();
                for (EmbeddingService.EmbeddingResult res : resultList) {
                    double[] embedding = res.embedding;
                    result.add(new EmbeddingVector(embedding));
                }
                return result;
            }
        }
        return Collections.emptyList();
    }
}
