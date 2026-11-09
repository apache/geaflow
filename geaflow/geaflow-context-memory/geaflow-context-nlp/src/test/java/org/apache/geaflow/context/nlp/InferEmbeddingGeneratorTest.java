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

import org.apache.geaflow.common.config.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class InferEmbeddingGeneratorTest {

  private Configuration config;

  @Before
  public void setUp() {
    config = new Configuration();
    config.put("infer.env.enable", "false");
  }

  @Test
  public void testGetEmbeddingDimension() {
    InferEmbeddingGenerator generator = new InferEmbeddingGenerator(config, 384);
    Assert.assertEquals(384, generator.getEmbeddingDimension());
  }

  @Test
  public void testDefaultDimension() {
    InferEmbeddingGenerator generator = new InferEmbeddingGenerator(config);
    Assert.assertEquals(384, generator.getEmbeddingDimension());
  }

  @Test
  public void testNotInitializedState() {
    InferEmbeddingGenerator generator = new InferEmbeddingGenerator(config);
    Assert.assertFalse(generator.isInitialized());
  }

  @Test(expected = IllegalStateException.class)
  public void testGenerateWithoutInit() throws Exception {
    InferEmbeddingGenerator generator = new InferEmbeddingGenerator(config);
    generator.generateEmbedding("test");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNullText() throws Exception {
    InferEmbeddingGenerator generator = new InferEmbeddingGenerator(config, 384);
    generator.generateEmbedding(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyText() throws Exception {
    InferEmbeddingGenerator generator = new InferEmbeddingGenerator(config, 384);
    generator.generateEmbedding("");
  }
}
