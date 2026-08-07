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

import java.util.List;
import java.util.Map;

/**
 * Interface for LLM provider abstraction.
 * Supports multiple LLM backends (OpenAI, Claude, local LLaMA, etc.).
 */
public interface LLMProvider {

  /**
   * Initialize the LLM provider with configuration.
   *
   * @param config Configuration parameters
   * @throws Exception if initialization fails
   */
  void initialize(Map<String, String> config) throws Exception;

  /**
   * Send a prompt to the LLM and get a response.
   *
   * @param prompt The input prompt
   * @return The LLM response
   * @throws Exception if the request fails
   */
  String generateText(String prompt) throws Exception;

  /**
   * Send a prompt with multiple turns (conversation).
   *
   * @param messages List of messages with roles (system, user, assistant)
   * @return The LLM response
   * @throws Exception if the request fails
   */
  String generateTextWithHistory(List<Map<String, String>> messages) throws Exception;

  /**
   * Stream text generation (for long responses).
   *
   * @param prompt The input prompt
   * @param callback Callback to handle streamed chunks
   * @throws Exception if the request fails
   */
  void streamGenerateText(String prompt, StreamCallback callback) throws Exception;

  /**
   * Get the model name.
   *
   * @return The model name
   */
  String getModelName();

  /**
   * Check if the provider is available.
   *
   * @return True if provider is available, false otherwise
   */
  boolean isAvailable();

  /**
   * Close the provider and release resources.
   *
   * @throws Exception if closing fails
   */
  void close() throws Exception;

  /**
   * Callback interface for streaming responses.
   */
  interface StreamCallback {

    /**
     * Called when a chunk of text is received.
     *
     * @param chunk The text chunk
     */
    void onChunk(String chunk);

    /**
     * Called when streaming is complete.
     */
    void onComplete();

    /**
     * Called when an error occurs.
     *
     * @param error The error message
     */
    void onError(String error);
  }
}
