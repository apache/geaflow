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

package org.apache.geaflow.context.nlp;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Unit tests for NLP embedding generator.
 */
public class DefaultEmbeddingGeneratorTest {

    private DefaultEmbeddingGenerator generator;

    @Before
    public void setUp() throws Exception {
        generator = new DefaultEmbeddingGenerator(768);
        generator.initialize();
    }

    @After
    public void tearDown() throws Exception {
        if (generator != null) {
            generator.close();
        }
    }

    @Test
    public void testEmbeddingGeneration() throws Exception {
        String text = "This is a test sentence";
        float[] embedding = generator.generateEmbedding(text);

        assertNotNull(embedding);
        assertEquals(768, embedding.length);
    }

    @Test
    public void testEmbeddingDimension() {
        assertEquals(768, generator.getEmbeddingDimension());
    }

    @Test
    public void testDeterministicEmbedding() throws Exception {
        String text = "Test deterministic embedding";
        float[] embedding1 = generator.generateEmbedding(text);
        float[] embedding2 = generator.generateEmbedding(text);

        // Same text should produce same embedding
        assertArrayEquals(embedding1, embedding2, 0.0001f);
    }

    @Test
    public void testBatchEmbedding() throws Exception {
        String[] texts = {
            "First text",
            "Second text",
            "Third text"
        };

        float[][] embeddings = generator.generateEmbeddings(texts);

        assertNotNull(embeddings);
        assertEquals(3, embeddings.length);
        assertEquals(768, embeddings[0].length);
    }

    @Test
    public void testNullTextHandling() throws Exception {
        try {
            generator.generateEmbedding(null);
            fail("Should throw exception for null text");
        } catch (IllegalArgumentException e) {
            assertTrue(true);
        }
    }

    @Test
    public void testVectorNormalization() throws Exception {
        String text = "Test normalization";
        float[] embedding = generator.generateEmbedding(text);

        // Check if vector is normalized to unit length
        double norm = 0.0;
        for (float v : embedding) {
            norm += v * v;
        }
        norm = Math.sqrt(norm);

        assertEquals(1.0, norm, 0.0001);
    }
}
