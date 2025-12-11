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

package org.apache.geaflow.ai.common.model;

import com.google.gson.annotations.SerializedName;

import java.util.List;


public class Response {
    public String id;
    @SerializedName("created")
    public long createdTimestamp;
    public String model;
    public String object;
    public List<Choice> choices;
    public Usage usage;
    @SerializedName("system_fingerprint")
    public String systemFingerprint;

    public static class Choice {
        public String finish_reason;
        public int index;
        public Message message;
        public Object logprobs;

        // Getters and Setters...
    }

    public static class Message {
        public String role;
        public String content;

        // Getters and Setters...
    }

    public static class Usage {
        @SerializedName("completion_tokens")
        public int completionTokens;
        @SerializedName("prompt_tokens")
        public int promptTokens;
        @SerializedName("total_tokens")
        public int totalTokens;

        // Getters and Setters...
    }

    // Getters and Setters for all fields...
}