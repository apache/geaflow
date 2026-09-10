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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.geaflow.ai.retrieval.config;

import org.apache.geaflow.ai.retrieval.api.model.ExecutionMode;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalException;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalMode;
import org.noear.solon.annotation.Component;
import org.noear.solon.annotation.Init;
import org.noear.solon.annotation.Inject;

/** Versioned defaults and hard limits for retrieval requests. */
@Component
public class RetrievalProperties {

    @Inject("${retrieval.config-version:v1}")
    private String configVersion = "v1";
    @Inject("${retrieval.ready-graph-name:Confucius}")
    private String readyGraphName = "Confucius";
    @Inject("${retrieval.default-mode:KEYWORD}")
    private String defaultMode = "KEYWORD";
    @Inject("${retrieval.default-execution-mode:SEQUENTIAL}")
    private String defaultExecutionMode = "SEQUENTIAL";
    @Inject("${retrieval.default-top-k:10}")
    private int defaultTopK = 10;
    @Inject("${retrieval.max-top-k:100}")
    private int maxTopK = 100;
    @Inject("${retrieval.default-timeout-ms:3000}")
    private int defaultTimeoutMs = 3000;
    @Inject("${retrieval.max-timeout-ms:10000}")
    private int maxTimeoutMs = 10000;
    @Inject("${retrieval.default-max-candidates:100}")
    private int defaultMaxCandidates = 100;
    @Inject("${retrieval.max-candidates:1000}")
    private int maxCandidates = 1000;
    @Inject("${retrieval.default-token-budget:4096}")
    private int defaultTokenBudget = 4096;
    @Inject("${retrieval.max-token-budget:16384}")
    private int maxTokenBudget = 16384;

    @Init
    public void validateConfiguration() {
        if (!"v1".equals(configVersion) || blank(readyGraphName)) {
            throw new RetrievalException(
                org.apache.geaflow.ai.retrieval.api.model.RetrievalErrorCode.INVALID_REQUEST,
                "retrieval config-version must be v1 and ready-graph-name is required");
        }
        if (!RetrievalMode.KEYWORD.name().equals(defaultMode)) {
            throw invalid("default-mode must be KEYWORD");
        }
        if (!ExecutionMode.SEQUENTIAL.name().equals(defaultExecutionMode)) {
            throw invalid("default-execution-mode must be SEQUENTIAL");
        }
        positiveLimit(defaultTopK, maxTopK, "topK");
        positiveLimit(defaultTimeoutMs, maxTimeoutMs, "timeoutMs");
        positiveLimit(defaultMaxCandidates, maxCandidates, "maxCandidates");
        positiveLimit(defaultTokenBudget, maxTokenBudget, "tokenBudget");
        if (defaultTopK > defaultMaxCandidates) {
            throw invalid("default-top-k must not exceed default-max-candidates");
        }
    }

    private static void positiveLimit(int defaultValue, int maxValue, String name) {
        if (defaultValue < 1 || maxValue < 1 || defaultValue > maxValue) {
            throw invalid("invalid " + name + " default or hard limit");
        }
    }

    private static boolean blank(String value) {
        return value == null || value.trim().isEmpty();
    }

    private static RetrievalException invalid(String message) {
        return new RetrievalException(
            org.apache.geaflow.ai.retrieval.api.model.RetrievalErrorCode.INVALID_REQUEST, message);
    }

    public String getConfigVersion() {
        return configVersion;
    }

    public void setConfigVersion(String configVersion) {
        this.configVersion = configVersion;
    }

    public String getReadyGraphName() {
        return readyGraphName;
    }

    public void setReadyGraphName(String readyGraphName) {
        this.readyGraphName = readyGraphName;
    }

    public String getDefaultMode() {
        return defaultMode;
    }

    public void setDefaultMode(String defaultMode) {
        this.defaultMode = defaultMode;
    }

    public String getDefaultExecutionMode() {
        return defaultExecutionMode;
    }

    public void setDefaultExecutionMode(String defaultExecutionMode) {
        this.defaultExecutionMode = defaultExecutionMode;
    }

    public int getDefaultTopK() {
        return defaultTopK;
    }

    public void setDefaultTopK(int defaultTopK) {
        this.defaultTopK = defaultTopK;
    }

    public int getMaxTopK() {
        return maxTopK;
    }

    public void setMaxTopK(int maxTopK) {
        this.maxTopK = maxTopK;
    }

    public int getDefaultTimeoutMs() {
        return defaultTimeoutMs;
    }

    public void setDefaultTimeoutMs(int defaultTimeoutMs) {
        this.defaultTimeoutMs = defaultTimeoutMs;
    }

    public int getMaxTimeoutMs() {
        return maxTimeoutMs;
    }

    public void setMaxTimeoutMs(int maxTimeoutMs) {
        this.maxTimeoutMs = maxTimeoutMs;
    }

    public int getDefaultMaxCandidates() {
        return defaultMaxCandidates;
    }

    public void setDefaultMaxCandidates(int defaultMaxCandidates) {
        this.defaultMaxCandidates = defaultMaxCandidates;
    }

    public int getMaxCandidates() {
        return maxCandidates;
    }

    public void setMaxCandidates(int maxCandidates) {
        this.maxCandidates = maxCandidates;
    }

    public int getDefaultTokenBudget() {
        return defaultTokenBudget;
    }

    public void setDefaultTokenBudget(int defaultTokenBudget) {
        this.defaultTokenBudget = defaultTokenBudget;
    }

    public int getMaxTokenBudget() {
        return maxTokenBudget;
    }

    public void setMaxTokenBudget(int maxTokenBudget) {
        this.maxTokenBudget = maxTokenBudget;
    }
}
