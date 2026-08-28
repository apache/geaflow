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
}
