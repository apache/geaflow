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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class VectorStoreTest {

    private Path tempFile;
    private VectorStoreMetadata metadata;

    @BeforeEach
    public void setUp() throws IOException {
        tempFile = Files.createTempFile("vector_store_test", ".jsonl");
        metadata = new VectorStoreMetadata("test_model", 128, DistanceMetric.COSINE, "1.0", System.currentTimeMillis(), 1);
    }

    @AfterEach
    public void tearDown() throws IOException {
        Files.deleteIfExists(tempFile);
        Files.deleteIfExists(tempFile.resolveSibling(tempFile.getFileName() + ".quarantine"));
    }

    @Test
    public void testMetadataValidation() {
        assertThrows(IllegalArgumentException.class, () -> {
            new VectorStoreMetadata("", 128, DistanceMetric.COSINE, "1.0", 0, 1);
        });
        
        assertThrows(IllegalArgumentException.class, () -> {
            new VectorStoreMetadata("test", 0, DistanceMetric.COSINE, "1.0", 0, 1);
        });
        
        assertThrows(IllegalArgumentException.class, () -> {
            new VectorStoreMetadata("test", 128, null, "1.0", 0, 1);
        });
    }

    @Test
    public void testInMemoryVectorStore() {
        VectorStore store = new InMemoryVectorStore(metadata);
        testVectorStore(store);
    }

    @Test
    public void testLocalVectorStore() {
        VectorStore store = new LocalVectorStore(metadata, tempFile);
        testVectorStore(store);
    }

    private void testVectorStore(VectorStore store) {
        double[] embedding = new double[128];
        embedding[0] = 1.0;
        
        VectorRecord record = new VectorRecord("v1", embedding, "chunk", "c1", Collections.emptyMap());
        store.upsert(record);
        
        VectorQuery query = new VectorQuery(embedding, 10, Collections.emptyMap());
        List<VectorHit> hits = store.search(query);
        
        assertEquals(1, hits.size());
        assertEquals("v1", hits.get(0).getVectorId());
        
        // Dimension mismatch
        double[] badEmbedding = new double[64];
        VectorRecord badRecord = new VectorRecord("v2", badEmbedding, "chunk", "c2", Collections.emptyMap());
        
        assertThrows(VectorStoreException.class, () -> {
            store.upsert(badRecord);
        });
        
        // Model mismatch
        VectorQuery badQuery = new VectorQuery(embedding, 10, Collections.singletonMap("model_name", "wrong_model"));
        assertThrows(VectorStoreException.class, () -> {
            store.search(badQuery);
        });
        
        // Delete
        store.markDeleted("v1");
        hits = store.search(query);
        assertEquals(0, hits.size());
        
        store.close();
    }

    @Test
    public void testMetadataFiltering() {
        VectorStore store = new InMemoryVectorStore(metadata);
        double[] embedding = new double[128];
        
        VectorRecord record1 = new VectorRecord("v1", embedding, "chunk", "c1", Collections.singletonMap("author", "Alice"));
        VectorRecord record2 = new VectorRecord("v2", embedding, "entity", "e1", Collections.singletonMap("author", "Bob"));
        
        store.upsertBatch(java.util.Arrays.asList(record1, record2));
        
        // Filter by author=Alice
        VectorQuery query1 = new VectorQuery(embedding, 10, Collections.singletonMap("author", "Alice"));
        List<VectorHit> hits1 = store.search(query1);
        assertEquals(1, hits1.size());
        assertEquals("v1", hits1.get(0).getVectorId());
        
        // Filter by _source_type=entity
        VectorQuery query2 = new VectorQuery(embedding, 10, Collections.singletonMap("_source_type", "entity"));
        List<VectorHit> hits2 = store.search(query2);
        assertEquals(1, hits2.size());
        assertEquals("v2", hits2.get(0).getVectorId());
        
        store.close();
    }

    @Test
    public void testChecksumQuarantine() throws IOException {
        String validLine = "{\"vectorId\":\"v1\",\"embedding\":[1.0],\"sourceType\":\"chunk\",\"sourceId\":\"c1\",\"metadata\":{},\"__checksum\":\"valid_checksum\"}";
        String invalidLine = "{\"vectorId\":\"v2\",\"embedding\":[1.0],\"sourceType\":\"chunk\",\"sourceId\":\"c2\",\"metadata\":{},\"__checksum\":\"wrong_checksum\"}";
        String legacyLine = "{\"vectorId\":\"v3\",\"embedding\":[1.0],\"sourceType\":\"chunk\",\"sourceId\":\"c3\",\"metadata\":{}}";

        Files.write(tempFile, java.util.Arrays.asList(validLine, invalidLine, legacyLine));
        
        VectorStore store = new LocalVectorStore(metadata, tempFile);
        VectorQuery query = new VectorQuery(new double[128], 10, Collections.emptyMap());
        List<VectorHit> hits = store.search(query);
        
        Path quarantinePath = tempFile.resolveSibling(tempFile.getFileName() + ".quarantine");
        List<String> quarantineLines = Files.readAllLines(quarantinePath);
        
        assertEquals(3, quarantineLines.size());
        
        store.close();
    }

    @Test
    public void testDistanceMetricsAndGetters() {
        // Test DistanceMetrics
        double[] v1 = {1.0, 0.0};
        double[] v2 = {0.0, 1.0};
        
        double dot = DistanceUtils.compute(v1, v2, DistanceMetric.DOT_PRODUCT);
        assertEquals(0.0, dot, 0.001);

        double l2 = DistanceUtils.compute(v1, v2, DistanceMetric.L2);
        
        assertEquals(1.0 / (1.0 + Math.sqrt(2)), l2, 0.001);
        
        assertThrows(IllegalArgumentException.class, () -> {
            DistanceUtils.compute(v1, v2, null);
        });

        VectorRecord record = new VectorRecord("v1", v1, "chunk", "c1", Collections.emptyMap());
        VectorHit hit = new VectorHit("v1", 0.9, record);
        assertEquals("v1", hit.getVectorId());
        assertEquals(0.9, hit.getScore());
        assertEquals(record, hit.getRecord());
        
        VectorQuery query = new VectorQuery(v1, 5, Collections.emptyMap());
        assertEquals(5, query.getTopK());
        assertEquals(0, query.getFilterMetadata().size());
        
        assertEquals(128, metadata.getDimension());
        assertEquals(DistanceMetric.COSINE, metadata.getDistance());
        assertEquals(1, metadata.getFormatVersion());
        assertEquals("test_model", metadata.getModelName());
    }

    @Test
    public void testLocalVectorStoreBatchAndExceptions() throws IOException {
        VectorStore store = new LocalVectorStore(metadata, tempFile);
        
        // Test getMetadata
        assertEquals(metadata.getDimension(), store.getMetadata().getDimension());
        
        double[] embedding = new double[128];
        VectorRecord r1 = new VectorRecord("v1", embedding, "chunk", "c1", Collections.emptyMap());
        VectorRecord r2 = new VectorRecord("v2", embedding, "chunk", "c2", Collections.emptyMap());
        
        store.upsertBatch(java.util.Arrays.asList(r1, r2));
        
        List<VectorHit> hits = store.search(new VectorQuery(embedding, 10, Collections.emptyMap()));
        assertEquals(2, hits.size());
        
        double[] badEmbedding = new double[64];
        VectorRecord badR = new VectorRecord("v3", badEmbedding, "chunk", "c3", Collections.emptyMap());
        
        VectorStoreException ex = assertThrows(VectorStoreException.class, () -> {
            store.upsertBatch(java.util.Arrays.asList(badR));
        });
        assertEquals(VectorStoreException.ErrorCode.DIMENSION_MISMATCH, ex.getErrorCode());
        
        // Mark deleted unknown record
        VectorStoreException ex2 = assertThrows(VectorStoreException.class, () -> {
            store.markDeleted("unknown_id");
        });
        assertEquals(VectorStoreException.ErrorCode.RECORD_NOT_FOUND, ex2.getErrorCode());
        
        store.close();
    }
}
