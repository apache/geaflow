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

package org.apache.geaflow.context.vector.store;

import java.util.List;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MilvusVectorStoreTest {

  private MilvusVectorStore store;

  @Before
  public void setUp() throws Exception {
    store = new MilvusVectorStore("localhost", 19530, "test_collection", 768, 4);
    store.initialize();
  }

  @After
  public void tearDown() throws Exception {
    if (store != null) {
      store.close();
    }
  }

  @Test
  public void testInitialization() {
    Assert.assertTrue(store.isConnected());
  }

  @Test
  public void testAddVector() throws Exception {
    float[] vec = generateVector(768);
    store.addVector("entity-1", vec, System.currentTimeMillis());
    Assert.assertEquals(1, store.size());
  }

  @Test
  public void testAddMultipleVectors() throws Exception {
    for (int i = 0; i < 10; i++) {
      store.addVector("entity-" + i, generateVector(768), System.currentTimeMillis());
    }
    Assert.assertEquals(10, store.size());
  }

  @Test
  public void testSearch() throws Exception {
    float[] vec1 = generateVector(768);
    float[] vec2 = generateVector(768);
    store.addVector("entity-1", vec1, System.currentTimeMillis());
    store.addVector("entity-2", vec2, System.currentTimeMillis());

    List<VectorIndexStore.VectorSearchResult> results = store.search(vec1, 10, 0.5);
    Assert.assertNotNull(results);
    Assert.assertTrue(results.size() > 0);
  }

  @Test
  public void testSearchWithThreshold() throws Exception {
    float[] vec = generateVector(768);
    store.addVector("entity-1", vec, System.currentTimeMillis());

    List<VectorIndexStore.VectorSearchResult> results = store.search(vec, 10, 0.9);
    Assert.assertTrue(results.size() > 0);
    Assert.assertTrue(results.get(0).getSimilarity() >= 0.9);
  }

  @Test
  public void testDeleteVector() throws Exception {
    store.addVector("entity-1", generateVector(768), System.currentTimeMillis());
    Assert.assertEquals(1, store.size());
    store.deleteVector("entity-1");
    Assert.assertEquals(0, store.size());
  }

  @Test
  public void testShardDistribution() throws Exception {
    for (int i = 0; i < 100; i++) {
      store.addVector("entity-" + i, generateVector(768), System.currentTimeMillis());
    }
    Assert.assertEquals(100, store.size());
    String stats = store.getShardStats();
    Assert.assertTrue(stats.contains("shard-0"));
  }

  @Test
  public void testDimensionValidation() throws Exception {
    try {
      store.addVector("entity-1", new float[256], System.currentTimeMillis());
      Assert.fail("Should throw exception");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains("Dimension mismatch"));
    }
  }

  @Test
  public void testSearchEmpty() throws Exception {
    List<VectorIndexStore.VectorSearchResult> results = store.search(generateVector(768), 10, 0.5);
    Assert.assertNotNull(results);
    Assert.assertEquals(0, results.size());
  }

  @Test
  public void testTopKLimit() throws Exception {
    for (int i = 0; i < 50; i++) {
      store.addVector("entity-" + i, generateVector(768), System.currentTimeMillis());
    }
    List<VectorIndexStore.VectorSearchResult> results = store.search(generateVector(768), 10, 0.0);
    Assert.assertTrue(results.size() <= 10);
  }

  @Test
  public void testGetVector() throws Exception {
    float[] vec = generateVector(768);
    store.addVector("entity-1", vec, System.currentTimeMillis());
    float[] retrieved = store.getVector("entity-1");
    Assert.assertNotNull(retrieved);
    Assert.assertEquals(768, retrieved.length);
  }

  @Test
  public void testVectorCount() {
    Assert.assertEquals(0, store.getVectorCount());
  }

  private float[] generateVector(int dim) {
    float[] vec = new float[dim];
    for (int i = 0; i < dim; i++) {
      vec[i] = (float) Math.random();
    }
    float norm = 0;
    for (float v : vec) {
      norm += v * v;
    }
    norm = (float) Math.sqrt(norm);
    for (int i = 0; i < vec.length; i++) {
      vec[i] /= norm;
    }
    return vec;
  }
}
