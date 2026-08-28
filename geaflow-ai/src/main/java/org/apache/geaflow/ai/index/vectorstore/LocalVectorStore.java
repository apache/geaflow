/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.geaflow.ai.index.vectorstore;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.CRC32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalVectorStore implements VectorStore {
    private static final Logger LOGGER = LoggerFactory.getLogger(LocalVectorStore.class);

    private final VectorStoreMetadata metadata;
    private final Path jsonlPath;
    private final Path quarantinePath;
    private final Gson gson = new Gson();
    private final ConcurrentHashMap<String, VectorRecord> store = new ConcurrentHashMap<>();
    private final Set<String> deletedIds = ConcurrentHashMap.newKeySet();
    private BufferedWriter writer;

    public LocalVectorStore(VectorStoreMetadata metadata, Path jsonlPath) {
        this.metadata = Objects.requireNonNull(metadata);
        this.jsonlPath = Objects.requireNonNull(jsonlPath);
        this.quarantinePath = jsonlPath.resolveSibling(jsonlPath.getFileName() + ".quarantine");
        init();
    }

    private void init() {
        if (Files.exists(jsonlPath)) {
            try (BufferedReader reader = Files.newBufferedReader(jsonlPath, StandardCharsets.UTF_8)) {
                String line;
                while ((line = reader.readLine()) != null) {
                    processLine(line);
                }
            } catch (IOException e) {
                throw new VectorStoreException(VectorStoreException.ErrorCode.PERSISTENCE_ERROR, "Failed to read store file", e);
            }
        }
        try {
            this.writer = Files.newBufferedWriter(jsonlPath, StandardCharsets.UTF_8, 
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new VectorStoreException(VectorStoreException.ErrorCode.PERSISTENCE_ERROR, "Failed to open store file for writing", e);
        }
    }

    private void processLine(String line) {
        try {
            JsonObject json = new JsonParser().parse(line).getAsJsonObject();
            String expectedChecksum = json.get("__checksum").getAsString();
            
            JsonObject dataForChecksum = new JsonParser().parse(line).getAsJsonObject();
            dataForChecksum.remove("__checksum");
            String actualChecksum = computeChecksum(dataForChecksum.toString());
            
            if (!expectedChecksum.equals(actualChecksum)) {
                quarantineLine(line);
                return;
            }

            String vectorId = json.get("vectorId").getAsString();
            if (json.has("__deleted") && json.get("__deleted").getAsBoolean()) {
                store.remove(vectorId);
                deletedIds.add(vectorId);
            } else {
                VectorRecord record = gson.fromJson(dataForChecksum, VectorRecord.class);
                store.put(vectorId, record);
                deletedIds.remove(vectorId);
            }
        } catch (Exception e) {
            quarantineLine(line);
        }
    }

    private void quarantineLine(String line) {
        try {
            Files.write(quarantinePath, (line + System.lineSeparator()).getBytes(StandardCharsets.UTF_8), 
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            LOGGER.warn("Ignoring quarantine IOException");
        }
    }

    private String computeChecksum(String data) {
        CRC32 crc32 = new CRC32();
        crc32.update(data.getBytes(StandardCharsets.UTF_8));
        return Long.toHexString(crc32.getValue());
    }

    private void appendLine(JsonObject json) {
        String dataStr = json.toString();
        String checksum = computeChecksum(dataStr);
        json.addProperty("__checksum", checksum);
        try {
            writer.write(json.toString());
            writer.newLine();
        } catch (IOException e) {
            throw new VectorStoreException(VectorStoreException.ErrorCode.PERSISTENCE_ERROR, "Failed to write record", e);
        }
    }

    @Override
    public void upsert(VectorRecord record) {
        if (record.getEmbedding().length != metadata.getDimension()) {
            throw new VectorStoreException(VectorStoreException.ErrorCode.DIMENSION_MISMATCH,
                    "Expected dimension " + metadata.getDimension() + ", got " + record.getEmbedding().length);
        }
        JsonObject json = gson.toJsonTree(record).getAsJsonObject();
        appendLine(json);
        
        store.put(record.getVectorId(), record);
        deletedIds.remove(record.getVectorId());

        try {
            writer.flush();
        } catch (IOException e) {
            throw new VectorStoreException(VectorStoreException.ErrorCode.PERSISTENCE_ERROR, "Failed to flush record", e);
        }
    }

    @Override
    public void upsertBatch(List<VectorRecord> records) {
        for (VectorRecord record : records) {
            if (record.getEmbedding().length != metadata.getDimension()) {
                throw new VectorStoreException(VectorStoreException.ErrorCode.DIMENSION_MISMATCH,
                        "Expected dimension " + metadata.getDimension() + ", got " + record.getEmbedding().length);
            }
            JsonObject json = gson.toJsonTree(record).getAsJsonObject();
            appendLine(json);
            
            store.put(record.getVectorId(), record);
            deletedIds.remove(record.getVectorId());
        }
        try {
            writer.flush();
        } catch (IOException e) {
            throw new VectorStoreException(VectorStoreException.ErrorCode.PERSISTENCE_ERROR, "Failed to flush batch", e);
        }
    }

    @Override
    public List<VectorHit> search(VectorQuery query) {
        if (query.getFilterMetadata().containsKey("model_name")) {
            String filterModel = query.getFilterMetadata().get("model_name");
            if (!Objects.equals(filterModel, metadata.getModelName())) {
                throw new VectorStoreException(VectorStoreException.ErrorCode.MODEL_MISMATCH,
                        "Expected model " + metadata.getModelName() + ", got " + filterModel);
            }
        }

        PriorityQueue<VectorHit> hits = new PriorityQueue<>(query.getTopK(), Comparator.comparingDouble(VectorHit::getScore));

        for (VectorRecord record : store.values()) {
            if (!deletedIds.contains(record.getVectorId())) {
                boolean match = true;
                for (Map.Entry<String, String> entry : query.getFilterMetadata().entrySet()) {
                    if (entry.getKey().equals("model_name")) {
                        continue;
                    }
                    if (!Objects.equals(record.getMetadata().get(entry.getKey()), entry.getValue()) && !Objects.equals(record.getSourceType(), entry.getValue())) {
                        match = false;
                        break;
                    }
                }
                if (match) {
                    double score = DistanceUtils.compute(query.getQueryVector(), record.getEmbedding(), metadata.getDistance());
                    if (hits.size() < query.getTopK()) {
                        hits.add(new VectorHit(record.getVectorId(), score, record));
                    } else if (!hits.isEmpty() && score > hits.peek().getScore()) {
                        hits.poll();
                        hits.add(new VectorHit(record.getVectorId(), score, record));
                    }
                }
            }
        }

        List<VectorHit> topKHits = new ArrayList<>();
        while (!hits.isEmpty()) {
            topKHits.add(hits.poll());
        }
        
        Collections.reverse(topKHits);
        return topKHits;
    }

    @Override
    public void markDeleted(String vectorId) {
        if (!store.containsKey(vectorId)) {
            throw new VectorStoreException(VectorStoreException.ErrorCode.RECORD_NOT_FOUND,
                    "Record not found: " + vectorId);
        }
        JsonObject json = new JsonObject();
        json.addProperty("vectorId", vectorId);
        json.addProperty("__deleted", true);
        appendLine(json);
        
        try {
            writer.flush();
        } catch (IOException e) {
            throw new VectorStoreException(VectorStoreException.ErrorCode.PERSISTENCE_ERROR, "Failed to flush deletion", e);
        }
        
        store.remove(vectorId);
        deletedIds.add(vectorId);
    }

    @Override
    public VectorStoreMetadata getMetadata() {
        return metadata;
    }

    @Override
    public void close() {
        try {
            if (writer != null) {
                writer.close();
            }
        } catch (IOException e) {
            LOGGER.warn("Failed to close VectorStore cleanly", e);
        }
        
    }
}
