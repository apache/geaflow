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

import java.net.InetAddress;
import java.net.UnknownHostException;
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
    @Inject("${retrieval.default-max-edge-scans:100}")
    private int defaultMaxEdgeScans = 100;
    @Inject("${retrieval.max-edge-scans:5000}")
    private int maxEdgeScans = 5000;
    @Inject("${retrieval.default-token-budget:4096}")
    private int defaultTokenBudget = 4096;
    @Inject("${retrieval.max-token-budget:16384}")
    private int maxTokenBudget = 16384;
    @Inject("${server.host:127.0.0.1}")
    private String serverHost = "127.0.0.1";
    @Inject("${retrieval.remote-access-enabled:false}")
    private boolean remoteAccessEnabled;
    @Inject("${retrieval.api-token:}")
    private String apiToken;
    @Inject("${retrieval.artifact-bundle-root:}")
    private String artifactBundleRoot;
    @Inject("${retrieval.artifact-graph-name:}")
    private String artifactGraphName;
    @Inject("${retrieval.artifact-graph-version:}")
    private String artifactGraphVersion;
    @Inject("${retrieval.artifact-index-version:}")
    private String artifactIndexVersion;
    @Inject("${retrieval.artifact-graph-path:graph.bin}")
    private String artifactGraphPath = "graph.bin";
    @Inject("${retrieval.artifact-bm25-path:bm25}")
    private String artifactBm25Path = "bm25";
    @Inject("${retrieval.artifact-vector-path:vector.bin}")
    private String artifactVectorPath = "vector.bin";
    @Inject("${retrieval.artifact-vector-source:}")
    private String artifactVectorSource;
    @Inject("${retrieval.artifact-vector-version:}")
    private String artifactVectorVersion;
    @Inject("${retrieval.routing-policy-id:deterministic-rule}")
    private String routingPolicyId = "deterministic-rule";
    @Inject("${retrieval.max-replan-count:1}")
    private int maxReplanCount = 1;
    @Inject("${retrieval.strict-plan-validation:true}")
    private boolean strictPlanValidation = true;
    @Inject("${retrieval.reranker-id:deterministic-reranker-v1}")
    private String rerankerId = "deterministic-reranker-v1";
    @Inject("${retrieval.token-estimator-id:chars4-v1}")
    private String tokenEstimatorId = "chars4-v1";
    @Inject("${retrieval.deterministic-rule.high-average-idf:1.5}")
    private double highAverageIdf = 1.5D;
    @Inject("${retrieval.deterministic-rule.high-score-gap:0.20}")
    private double highScoreGap = 0.20D;
    @Inject("${retrieval.deterministic-rule.long-query-length:12}")
    private int longQueryLength = 12;
    @Inject("${retrieval.deterministic-rule.strong-anchor-confidence:0.75}")
    private double strongAnchorConfidence = 0.75D;
    @Inject("${retrieval.deterministic-rule.minimum-anchor-count:1}")
    private int minimumAnchorCount = 1;
    @Inject("${retrieval.deterministic-rule.high-candidate-degree:32}")
    private int highCandidateDegree = 32;
    @Inject("${retrieval.deterministic-rule.high-growth-rate:2.0}")
    private double highGrowthRate = 2.0D;
    @Inject("${retrieval.deterministic-rule.base-neighbor-cap:16}")
    private int baseNeighborCap = 16;

    @Init
    public void validateConfiguration() {
        if (!"v1".equals(configVersion) || blank(readyGraphName)) {
            throw new RetrievalException(
                org.apache.geaflow.ai.retrieval.api.model.RetrievalErrorCode.INVALID_REQUEST,
                "retrieval config-version must be v1 and ready-graph-name is required");
        }
        try {
            RetrievalMode.valueOf(defaultMode.trim().toUpperCase(java.util.Locale.ROOT));
        } catch (IllegalArgumentException | NullPointerException e) {
            throw invalid("default-mode is unsupported: " + defaultMode);
        }
        try {
            ExecutionMode.valueOf(defaultExecutionMode.trim().toUpperCase(java.util.Locale.ROOT));
        } catch (IllegalArgumentException | NullPointerException e) {
            throw invalid("default-execution-mode is unsupported: " + defaultExecutionMode);
        }
        positiveLimit(defaultTopK, maxTopK, "topK");
        positiveLimit(defaultTimeoutMs, maxTimeoutMs, "timeoutMs");
        positiveLimit(defaultMaxCandidates, maxCandidates, "maxCandidates");
        positiveLimit(defaultMaxEdgeScans, maxEdgeScans, "maxEdgeScans");
        positiveLimit(defaultTokenBudget, maxTokenBudget, "tokenBudget");
        if (defaultTopK > defaultMaxCandidates) {
            throw invalid("default-top-k must not exceed default-max-candidates");
        }
        boolean loopback = isLoopbackHost(serverHost);
        if (!loopback && !remoteAccessEnabled) {
            throw invalid("non-loopback server.host requires retrieval.remote-access-enabled=true");
        }
        if (remoteAccessEnabled && blank(apiToken)) {
            throw invalid("retrieval.api-token is required when remote access is enabled");
        }
        if (!blank(artifactBundleRoot)) {
            if (blank(artifactGraphName) || blank(artifactGraphVersion) || blank(artifactIndexVersion)
                || blank(artifactVectorSource) || blank(artifactVectorVersion)) {
                throw invalid("artifact bundle identity and vector metadata are required");
            }
            if (blank(artifactGraphPath) || blank(artifactBm25Path) || blank(artifactVectorPath)) {
                throw invalid("artifact bundle paths are required");
            }
        }
        if (routingPolicyId == null || !"deterministic-rule".equals(routingPolicyId.trim())) {
            throw invalid("routing-policy-id is unsupported: " + routingPolicyId);
        }
        if (maxReplanCount < 0 || maxReplanCount > 4) {
            throw invalid("max-replan-count must be between 0 and 4");
        }
        if (rerankerId == null || !"deterministic-reranker-v1".equals(rerankerId.trim())) {
            throw invalid("reranker-id is unsupported: " + rerankerId);
        }
        if (tokenEstimatorId == null || !"chars4-v1".equals(tokenEstimatorId.trim())) {
            throw invalid("token-estimator-id is unsupported: " + tokenEstimatorId);
        }
        if (!(highAverageIdf >= 0.0D) || !Double.isFinite(highAverageIdf)
            || !(highScoreGap >= 0.0D) || !Double.isFinite(highScoreGap)
            || longQueryLength < 1 || !(strongAnchorConfidence >= 0.0D) || !(strongAnchorConfidence <= 1.0D)
            || minimumAnchorCount < 1 || highCandidateDegree < 1
            || !(highGrowthRate >= 0.0D) || !Double.isFinite(highGrowthRate) || baseNeighborCap < 1) {
            throw invalid("deterministic-rule thresholds are invalid");
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

    private static boolean isLoopbackHost(String host) {
        if (blank(host)) {
            return false;
        }
        try {
            return InetAddress.getByName(host.trim()).isLoopbackAddress();
        } catch (UnknownHostException error) {
            throw invalid("server.host is not a resolvable address");
        }
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

    public int getDefaultMaxEdgeScans() {
        return defaultMaxEdgeScans;
    }

    public void setDefaultMaxEdgeScans(int defaultMaxEdgeScans) {
        this.defaultMaxEdgeScans = defaultMaxEdgeScans;
    }

    public int getMaxEdgeScans() {
        return maxEdgeScans;
    }

    public void setMaxEdgeScans(int maxEdgeScans) {
        this.maxEdgeScans = maxEdgeScans;
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

    public String getServerHost() {
        return serverHost;
    }

    public void setServerHost(String serverHost) {
        this.serverHost = serverHost;
    }

    public boolean isRemoteAccessEnabled() {
        return remoteAccessEnabled;
    }

    public void setRemoteAccessEnabled(boolean remoteAccessEnabled) {
        this.remoteAccessEnabled = remoteAccessEnabled;
    }

    public String getApiToken() {
        return apiToken;
    }

    public void setApiToken(String apiToken) {
        this.apiToken = apiToken;
    }

    public String getArtifactBundleRoot() {
        return artifactBundleRoot;
    }

    public void setArtifactBundleRoot(String artifactBundleRoot) {
        this.artifactBundleRoot = artifactBundleRoot;
    }

    public String getArtifactGraphName() {
        return artifactGraphName;
    }

    public void setArtifactGraphName(String artifactGraphName) {
        this.artifactGraphName = artifactGraphName;
    }

    public String getArtifactGraphVersion() {
        return artifactGraphVersion;
    }

    public void setArtifactGraphVersion(String artifactGraphVersion) {
        this.artifactGraphVersion = artifactGraphVersion;
    }

    public String getArtifactIndexVersion() {
        return artifactIndexVersion;
    }

    public void setArtifactIndexVersion(String artifactIndexVersion) {
        this.artifactIndexVersion = artifactIndexVersion;
    }

    public String getArtifactGraphPath() {
        return artifactGraphPath;
    }

    public void setArtifactGraphPath(String artifactGraphPath) {
        this.artifactGraphPath = artifactGraphPath;
    }

    public String getArtifactBm25Path() {
        return artifactBm25Path;
    }

    public void setArtifactBm25Path(String artifactBm25Path) {
        this.artifactBm25Path = artifactBm25Path;
    }

    public String getArtifactVectorPath() {
        return artifactVectorPath;
    }

    public void setArtifactVectorPath(String artifactVectorPath) {
        this.artifactVectorPath = artifactVectorPath;
    }

    public String getArtifactVectorSource() {
        return artifactVectorSource;
    }

    public void setArtifactVectorSource(String artifactVectorSource) {
        this.artifactVectorSource = artifactVectorSource;
    }

    public String getArtifactVectorVersion() {
        return artifactVectorVersion;
    }

    public void setArtifactVectorVersion(String artifactVectorVersion) {
        this.artifactVectorVersion = artifactVectorVersion;
    }

    public String getRoutingPolicyId() {
        return routingPolicyId;
    }

    public void setRoutingPolicyId(String routingPolicyId) {
        this.routingPolicyId = routingPolicyId;
    }

    public int getMaxReplanCount() {
        return maxReplanCount;
    }

    public void setMaxReplanCount(int maxReplanCount) {
        this.maxReplanCount = maxReplanCount;
    }

    public boolean isStrictPlanValidation() {
        return strictPlanValidation;
    }

    public void setStrictPlanValidation(boolean strictPlanValidation) {
        this.strictPlanValidation = strictPlanValidation;
    }

    public String getRerankerId() {
        return rerankerId;
    }

    public void setRerankerId(String rerankerId) {
        this.rerankerId = rerankerId;
    }

    public String getTokenEstimatorId() {
        return tokenEstimatorId;
    }

    public void setTokenEstimatorId(String tokenEstimatorId) {
        this.tokenEstimatorId = tokenEstimatorId;
    }

    public double getHighAverageIdf() {
        return highAverageIdf;
    }

    public void setHighAverageIdf(double highAverageIdf) {
        this.highAverageIdf = highAverageIdf;
    }

    public double getHighScoreGap() {
        return highScoreGap;
    }

    public void setHighScoreGap(double highScoreGap) {
        this.highScoreGap = highScoreGap;
    }

    public int getLongQueryLength() {
        return longQueryLength;
    }

    public void setLongQueryLength(int longQueryLength) {
        this.longQueryLength = longQueryLength;
    }

    public double getStrongAnchorConfidence() {
        return strongAnchorConfidence;
    }

    public void setStrongAnchorConfidence(double strongAnchorConfidence) {
        this.strongAnchorConfidence = strongAnchorConfidence;
    }

    public int getMinimumAnchorCount() {
        return minimumAnchorCount;
    }

    public void setMinimumAnchorCount(int minimumAnchorCount) {
        this.minimumAnchorCount = minimumAnchorCount;
    }

    public int getHighCandidateDegree() {
        return highCandidateDegree;
    }

    public void setHighCandidateDegree(int highCandidateDegree) {
        this.highCandidateDegree = highCandidateDegree;
    }

    public double getHighGrowthRate() {
        return highGrowthRate;
    }

    public void setHighGrowthRate(double highGrowthRate) {
        this.highGrowthRate = highGrowthRate;
    }

    public int getBaseNeighborCap() {
        return baseNeighborCap;
    }

    public void setBaseNeighborCap(int baseNeighborCap) {
        this.baseNeighborCap = baseNeighborCap;
    }
}
