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

package org.apache.geaflow.ai.index.vectorstore;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class VectorStoreMetadataTest {

    private Path tempFile;

    @BeforeEach
    public void setUp() throws IOException {
        tempFile = Files.createTempFile("metadata_test", ".json");
    }

    @AfterEach
    public void tearDown() throws IOException {
        Files.deleteIfExists(tempFile);
    }

    @Test
    public void testPersistAndLoadMetadata() throws IOException {
        VectorStoreMetadata metadata = new VectorStoreMetadata(
                "test_model",
                128,
                "COSINE",
                "1.0",
                System.currentTimeMillis(),
                "v1"
        );
        metadata.save(tempFile);

        VectorStoreMetadata loaded = VectorStoreMetadata.load(tempFile);
        assertEquals(metadata, loaded);
        assertEquals("test_model", loaded.getModelName());
        assertEquals(128, loaded.getDimension());
    }

    @Test
    public void testMissingMetadataValidationFails() {
        VectorStoreMetadata metadata = new VectorStoreMetadata();
        assertThrows(IllegalArgumentException.class, metadata::validateComplete);

        metadata.setModelName("test_model");
        assertThrows(IllegalArgumentException.class, metadata::validateComplete);

        metadata.setDimension(128);
        assertThrows(IllegalArgumentException.class, metadata::validateComplete);

        metadata.setDistance("L2");
        assertThrows(IllegalArgumentException.class, metadata::validateComplete);

        metadata.setIndexVersion("1.0");
        assertThrows(IllegalArgumentException.class, metadata::validateComplete);

        metadata.setFormatVersion("v1");
        // Should not throw now
        metadata.validateComplete();
    }

    @Test
    public void testDimensionMismatchFails() {
        VectorStoreMetadata expected = new VectorStoreMetadata("test_model", 128, "L2", "1.0", 0, "v1");
        VectorStoreMetadata actual = new VectorStoreMetadata("test_model", 256, "L2", "1.0", 0, "v1");

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> {
            actual.validate(expected);
        });
        assertEquals("Dimension mismatch. Expected 128, but got 256", ex.getMessage());
    }

    @Test
    public void testModelMismatchFails() {
        VectorStoreMetadata expected = new VectorStoreMetadata("expected_model", 128, "L2", "1.0", 0, "v1");
        VectorStoreMetadata actual = new VectorStoreMetadata("actual_model", 128, "L2", "1.0", 0, "v1");

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> {
            actual.validate(expected);
        });
        assertEquals("Model mismatch. Expected expected_model, but got actual_model", ex.getMessage());
    }

    @Test
    public void testLoadInvalidJsonFails() throws IOException {
        Files.write(tempFile, "{ \"dimension\": 128, \"model_name\": \"test\" ".getBytes(StandardCharsets.UTF_8)); // Invalid JSON
        
        assertThrows(com.google.gson.JsonSyntaxException.class, () -> {
            VectorStoreMetadata.load(tempFile);
        });
    }

    @Test
    public void testLoadMissingFieldsFails() throws IOException {
        // Missing distance, index_version, format_version
        Files.write(tempFile, "{ \"dimension\": 128, \"model_name\": \"test\" }".getBytes(StandardCharsets.UTF_8)); 
        
        assertThrows(IllegalArgumentException.class, () -> {
            VectorStoreMetadata.load(tempFile);
        });
    }
}
