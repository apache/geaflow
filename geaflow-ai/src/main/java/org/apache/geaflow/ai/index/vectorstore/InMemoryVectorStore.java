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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryVectorStore implements VectorStore {
    private final VectorStoreMetadata metadata;
    private final ConcurrentHashMap<String, VectorRecord> store = new ConcurrentHashMap<>();
    private final Set<String> deletedIds = ConcurrentHashMap.newKeySet();

    public InMemoryVectorStore(VectorStoreMetadata metadata) {
        this.metadata = Objects.requireNonNull(metadata);
    }

    @Override
    public void upsert(VectorRecord record) {
        if (record.getEmbedding().length != metadata.getDimension()) {
            throw new VectorStoreException(VectorStoreException.ErrorCode.DIMENSION_MISMATCH,
                    "Expected dimension " + metadata.getDimension() + ", got " + record.getEmbedding().length);
        }
        store.put(record.getVectorId(), record);
        deletedIds.remove(record.getVectorId());
    }

    @Override
    public void upsertBatch(List<VectorRecord> records) {
        for (VectorRecord record : records) {
            upsert(record);
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
                    if (entry.getKey().equals("_source_type")) {
                        if (!Objects.equals(record.getSourceType(), entry.getValue())) {
                            match = false;
                            break;
                        }
                    } else {
                        if (!Objects.equals(record.getMetadata().get(entry.getKey()), entry.getValue())) {
                            match = false;
                            break;
                        }
                    }
                }
                if (match) {
                    double score = DistanceUtils.compute(query.getQueryVector(), record.getEmbedding(), metadata.getDistance());
                    
                    if (hits.size() < query.getTopK()) {
                        hits.add(new VectorHit(record.getVectorId(), score, record));
                    } else if (score > hits.peek().getScore()) {
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
        deletedIds.add(vectorId);
    }

    @Override
    public VectorStoreMetadata getMetadata() {
        return metadata;
    }

    @Override
    public void close() {
        
    }
}
