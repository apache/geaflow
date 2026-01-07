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

package org.apache.geaflow.store.lucene;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class GraphVectorIndexTest {

    private GraphVectorIndex<String> stringIndex;
    private GraphVectorIndex<Long> longIndex;
    private GraphVectorIndex<Integer> intIndex;
    private GraphVectorIndex<Float> floatIndex;

    @BeforeMethod
    public void setUp() {
        stringIndex = new GraphVectorIndex<>(String.class);
        longIndex = new GraphVectorIndex<>(Long.class);
        intIndex = new GraphVectorIndex<>(Integer.class);
        floatIndex = new GraphVectorIndex<>(Float.class);
    }

    @AfterMethod
    public void tearDown() {
        stringIndex.close();
        longIndex.close();
        intIndex.close();
        floatIndex.close();
    }

    @Test
    public void testAddAndSearchStringKey() {
        String key = "vertex_1";
        String fieldName = "embedding";
        float[] vector = {0.1f, 0.2f, 0.3f, 0.4f};

        // Add vector index
        stringIndex.addVectorIndex(true, key, fieldName, vector);

        // Search vector index
        String result = stringIndex.searchVectorIndex(true, fieldName, vector, 1);

        assertEquals(result, key);
    }

    @Test
    public void testAddAndSearchLongKey() {
        Long key = 12345L;
        String fieldName = "embedding";
        float[] vector = {0.5f, 0.6f, 0.7f, 0.8f};

        // Add vector index
        longIndex.addVectorIndex(false, key, fieldName, vector);

        // Search vector index
        Long result = longIndex.searchVectorIndex(false, fieldName, vector, 1);

        assertEquals(result, key);
    }

    @Test
    public void testAddAndSearchIntegerKey() {
        Integer key = 999;
        String fieldName = "features";
        float[] vector = {0.9f, 0.8f, 0.7f, 0.6f};

        // Add vector index
        intIndex.addVectorIndex(true, key, fieldName, vector);

        // Search vector index
        Integer result = intIndex.searchVectorIndex(true, fieldName, vector, 1);

        assertEquals(result, key);
    }

    @Test
    public void testAddAndSearchFloatKey() {
        Float key = 3.14f;
        String fieldName = "weights";
        float[] vector = {0.2f, 0.4f, 0.6f, 0.8f};

        // Add vector index
        floatIndex.addVectorIndex(false, key, fieldName, vector);

        // Search vector index
        Float result = floatIndex.searchVectorIndex(false, fieldName, vector, 1);

        assertEquals(result, key);
    }

    @Test
    public void testVectorSimilaritySearch() {
        String fieldName = "similarity_test";

        // Add several vectors with different similarities
        stringIndex.addVectorIndex(true, "doc1", fieldName, new float[]{1.0f, 0.0f, 0.0f, 0.0f});
        stringIndex.addVectorIndex(true, "doc2", fieldName, new float[]{0.8f, 0.2f, 0.0f, 0.0f});
        stringIndex.addVectorIndex(true, "doc3", fieldName, new float[]{0.0f, 0.0f, 1.0f, 0.0f});

        // Query using a vector identical to doc1
        float[] queryVector = {1.0f, 0.0f, 0.0f, 0.0f};
        String result = stringIndex.searchVectorIndex(true, fieldName, queryVector, 1);

        // Should return the most similar document
        assertEquals(result, "doc1");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testUnsupportedKeyType() {
        // Use unsupported key type
        Double key = 1.23;
        GraphVectorIndex<Double> doubleIndex = new GraphVectorIndex<>(Double.class);

        try {
            doubleIndex.addVectorIndex(true, key, "test_field", new float[]{0.1f, 0.2f});
        } finally {
            doubleIndex.close();
        }
    }

    @Test
    public void testMultipleVectorsSameKeyDifferentFields() {
        String key = "multi_field_vertex";
        float[] vector1 = {0.1f, 0.2f, 0.3f, 0.4f};
        float[] vector2 = {0.5f, 0.6f, 0.7f, 0.8f};

        // Add vectors for the same key in different fields
        stringIndex.addVectorIndex(true, key, "field1", vector1);
        stringIndex.addVectorIndex(true, key, "field2", vector2);

        // Search different fields separately
        String result1 = stringIndex.searchVectorIndex(true, "field1", vector1, 1);
        String result2 = stringIndex.searchVectorIndex(true, "field2", vector2, 1);

        assertEquals(result1, key);
        assertEquals(result2, key);
    }
}
