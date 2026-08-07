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

package org.apache.geaflow.context.nlp.llm;

import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test cases for DefaultLLMProvider.
 */
public class DefaultLLMProviderTest {

  private LLMProvider provider;

  /**
   * Setup test fixtures.
   *

   * @throws Exception if setup fails
   */
  @Before
  public void setUp() throws Exception {
    provider = new DefaultLLMProvider();
    Map<String, String> config = new HashMap<>();
    config.put("api_key", "test-key");
    config.put("endpoint", "http://localhost:8000");
    provider.initialize(config);
  }

  /**
   * Test text generation.
   *

   * @throws Exception if test fails
   */
  @Test
  public void testGenerateText() throws Exception {
    String prompt = "Extract entities from this text: John works for Apple.";
    String response = provider.generateText(prompt);

    Assert.assertNotNull(response);
    Assert.assertFalse(response.isEmpty());
  }

  /**
   * Test text generation with history.
   *

   * @throws Exception if test fails
   */
  @Test
  public void testGenerateTextWithHistory() throws Exception {
    Map<String, String> msg1 = new HashMap<>();
    msg1.put("role", "user");
    msg1.put("content", "Extract entities");

    Map<String, String> msg2 = new HashMap<>();
    msg2.put("role", "assistant");
    msg2.put("content", "Ready to extract entities");

    java.util.List<Map<String, String>> messages = new java.util.ArrayList<>();
    messages.add(msg1);
    messages.add(msg2);

    String response = provider.generateTextWithHistory(messages);
    Assert.assertNotNull(response);
    Assert.assertFalse(response.isEmpty());
  }

  /**
   * Test stream generation.
   *

   * @throws Exception if test fails
   */
  @Test
  public void testStreamGeneration() throws Exception {
    StringBuilder result = new StringBuilder();
    LLMProvider.StreamCallback callback = new LLMProvider.StreamCallback() {
      @Override
      public void onChunk(String chunk) {
        result.append(chunk);
      }

      @Override
      public void onComplete() {
        // Stream complete
      }

      @Override
      public void onError(String error) {
        Assert.fail("Stream error: " + error);
      }
    };

    provider.streamGenerateText("Test prompt", callback);
    Assert.assertTrue(result.length() > 0);
  }

  /**
   * Test model name.
   *

   * @throws Exception if test fails
   */
  @Test
  public void testGetModelName() throws Exception {
    String modelName = provider.getModelName();
    Assert.assertNotNull(modelName);
    Assert.assertFalse(modelName.isEmpty());
  }

  /**
   * Test availability.
   *

   * @throws Exception if test fails
   */
  @Test
  public void testIsAvailable() throws Exception {
    Assert.assertTrue(provider.isAvailable());
  }

  /**
   * Cleanup test fixtures.
   *

   * @throws Exception if cleanup fails
   */
  @Test
  public void testClose() throws Exception {
    provider.close();
    Assert.assertFalse(provider.isAvailable());
  }
}
