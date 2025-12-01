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

import com.alibaba.fastjson.JSON;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default LLM provider implementation for demonstration and testing.
 * In production, replace with actual API integrations (OpenAI, Claude, etc.).
 */
public class DefaultLLMProvider implements LLMProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultLLMProvider.class);

  private static final String MODEL_NAME = "default-demo-llm";
  private String apiKey;
  private String endpoint;
  private boolean initialized = false;

  /**
   * Initialize the LLM provider.
   *
   * @param config Configuration parameters
   * @throws Exception if initialization fails
   */
  @Override
  public void initialize(Map<String, String> config) throws Exception {
    LOGGER.info("Initializing DefaultLLMProvider");
    this.apiKey = config.getOrDefault("api_key", "demo-key");
    this.endpoint = config.getOrDefault("endpoint", "http://localhost:8000");
    this.initialized = true;
    LOGGER.info("DefaultLLMProvider initialized with endpoint: {}", endpoint);
  }

  /**
   * Send a prompt to the LLM and get a response.
   *
   * @param prompt The input prompt
   * @return The LLM response
   * @throws Exception if the request fails
   */
  @Override
  public String generateText(String prompt) throws Exception {
    if (!initialized) {
      throw new IllegalStateException("LLM provider not initialized");
    }

    LOGGER.debug("Generating text for prompt: {}", prompt.substring(0, Math.min(50, prompt
        .length())));

    // In production, call actual LLM API
    // For now, return a simulated response
    return simulateLLMResponse(prompt);
  }

  /**
   * Send a prompt with conversation history.
   *
   * @param messages List of messages with roles (system, user, assistant)
   * @return The LLM response
   * @throws Exception if the request fails
   */
  @Override
  public String generateTextWithHistory(List<Map<String, String>> messages) throws Exception {
    if (!initialized) {
      throw new IllegalStateException("LLM provider not initialized");
    }

    LOGGER.debug("Generating text with {} messages in history", messages.size());

    // Get last user message as context
    String lastPrompt = "";
    for (int i = messages.size() - 1; i >= 0; i--) {
      Map<String, String> msg = messages.get(i);
      if ("user".equals(msg.get("role"))) {
        lastPrompt = msg.get("content");
        break;
      }
    }

    return simulateLLMResponse(lastPrompt);
  }

  /**
   * Stream text generation.
   *
   * @param prompt The input prompt
   * @param callback Callback to handle streamed chunks
   * @throws Exception if the request fails
   */
  @Override
  public void streamGenerateText(String prompt, StreamCallback callback) throws Exception {
    if (!initialized) {
      throw new IllegalStateException("LLM provider not initialized");
    }

    try {
      String fullResponse = simulateLLMResponse(prompt);
      // Simulate streaming by splitting response into words
      String[] words = fullResponse.split("\\s+");
      for (String word : words) {
        callback.onChunk(word + " ");
        Thread.sleep(10); // Simulate network latency
      }
      callback.onComplete();
    } catch (Exception e) {
      callback.onError(e.getMessage());
      throw e;
    }
  }

  /**
   * Get the model name.
   *
   * @return The model name
   */
  @Override
  public String getModelName() {
    return MODEL_NAME;
  }

  /**
   * Check if the provider is available.
   *
   * @return True if provider is available
   */
  @Override
  public boolean isAvailable() {
    return initialized;
  }

  /**
   * Close the provider.
   *
   * @throws Exception if closing fails
   */
  @Override
  public void close() throws Exception {
    LOGGER.info("Closing DefaultLLMProvider");
    initialized = false;
  }

  /**
   * Simulate an LLM response for demonstration purposes.
   *
   * @param prompt The input prompt
   * @return Simulated LLM response
   */
  private String simulateLLMResponse(String prompt) {
    // Simple rule-based responses for demo
    if (prompt.toLowerCase().contains("entity") || prompt.toLowerCase().contains("extract")) {
      return "The following entities were identified: Person, Organization, Location. "
          + "Each entity has been assigned a unique identifier and type label.";
    } else if (prompt.toLowerCase().contains("relation") || prompt.toLowerCase().contains(
        "relationship")) {
      return "The relationships found include: person works_for organization, "
          + "person located_in location, organization competes_with organization. "
          + "These relations form the basis of the knowledge graph structure.";
    } else if (prompt.toLowerCase().contains("summary") || prompt.toLowerCase().contains(
        "summarize")) {
      return "Summary: The input text describes entities and their relationships. "
          + "Key entities have been extracted and linked, with relations identified "
          + "to form a knowledge graph representation of the content.";
    } else {
      return "Response: The LLM has processed your request. "
          + "In production, this would be a response from OpenAI GPT-4, Claude, or another LLM. "
          + "The response would be context-aware and based on the actual model's inference.";
    }
  }
}
