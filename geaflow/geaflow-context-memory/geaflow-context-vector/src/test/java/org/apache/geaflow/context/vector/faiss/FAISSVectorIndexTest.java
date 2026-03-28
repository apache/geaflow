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

package org.apache.geaflow.context.vector.faiss;

import org.apache.geaflow.common.config.Configuration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class FAISSVectorIndexTest {

  private FAISSVectorIndex index;
  private Configuration config;

  @Before
  public void setUp() {
    config = new Configuration();
    config.put("infer.env.enable", "false");
    index = new FAISSVectorIndex(config, 384, "IVF_FLAT");
  }

  @After
  public void tearDown() throws Exception {
    if (index != null) {
      index.close();
    }
  }

  @Test
  public void testGetVectorDimension() {
    Assert.assertEquals(384, index.getVectorDimension());
  }

  @Test
  public void testGetIndexType() {
    Assert.assertEquals("IVF_FLAT", index.getIndexType());
  }

  @Test
  public void testDefaultIndexType() {
    FAISSVectorIndex defaultIndex = new FAISSVectorIndex(config, 768);
    Assert.assertEquals("IVF_FLAT", defaultIndex.getIndexType());
    Assert.assertEquals(768, defaultIndex.getVectorDimension());
  }

  @Test
  public void testNotInitializedState() {
    Assert.assertFalse(index.isInitialized());
  }

  @Test(expected = IllegalStateException.class)
  public void testAddWithoutInit() throws Exception {
    float[] vector = new float[384];
    index.addEmbedding("test", vector);
  }

  @Test(expected = IllegalStateException.class)
  public void testSearchWithoutInit() throws Exception {
    float[] vector = new float[384];
    index.search(vector, 10, 0.5);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testWrongDimension() throws Exception {
    try {
      index.initialize();
    } catch (Exception ignored) {
    }
    float[] vector = new float[256];
    index.addEmbedding("test", vector);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNullEmbedding() throws Exception {
    try {
      index.initialize();
    } catch (Exception ignored) {
    }
    index.addEmbedding("test", null);
  }

  @Test
  public void testMultipleIndexTypes() {
    String[] indexTypes = {"IVF_FLAT", "IVF_PQ", "HNSW", "FLAT"};
    
    for (String type : indexTypes) {
      FAISSVectorIndex testIndex = new FAISSVectorIndex(config, 384, type);
      Assert.assertEquals(type, testIndex.getIndexType());
    }
  }

  @Test
  public void testSizeWithoutInit() throws Exception {
    Assert.assertEquals(0, index.size());
  }

  @Test
  public void testGetEmbeddingWithoutInit() throws Exception {
    float[] result = index.getEmbedding("test");
    Assert.assertNull(result);
  }
}
