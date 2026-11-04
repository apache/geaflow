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
import java.util.LinkedHashMap;
import java.util.Map;
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
    /**
     * Logical graph name that {@code /ready} reports on.
     *
     * <p>Defaults to the sample graph so a freshly started server has a concrete readiness target;
     * a deployment must override it through {@code retrieval.ready-graph-name} or the
     * {@code GEAFLOW_RETRIEVAL_READY_GRAPH_NAME} variable. Validation rejects a blank value.
     */
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
    /**
     * Threshold on the first-stage score gap, in the units of the fusion score it is compared with.
     *
     * <p>The score gap is a difference of weighted reciprocal-rank terms, {@code w / (k + rank)},
     * so it is bounded well below 1: the measured 2Wiki run reports a median of {@code 0.00055} and
     * a maximum of {@code 0.003}. The previous default of {@code 0.20} was therefore unreachable and
     * silently reduced every gap-based decision to "not confident". The default is set inside the
     * measured band so the comparison can actually separate queries.
     */
    @Inject("${retrieval.deterministic-rule.high-score-gap:0.0005}")
    private double highScoreGap = 0.0005D;
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
    /**
     * Fusion weight given to the graph channel when a confident anchor makes it the lead channel.
     *
     * <p>The measured end-to-end runs put BM25 and vector recall at {@code 0.002} while graph
     * expansion supplies the evidence, so the graph channel must be able to outweigh the text
     * channels in the fused score. Raising the graph weight from {@code 1.0} to {@code 2.0} while
     * demoting text to {@code 0.25} moved 2Wiki MRR from {@code 0.1019} to {@code 0.4747}. Keeping
     * this configurable is the point: it is a quality-sensitive parameter, not a constant.
     */
    @Inject("${retrieval.graph-fusion.graph-weight:2.0}")
    private double graphFusionGraphWeight = 2.0D;
    /** Fusion weight given to each text channel when an anchor makes the graph channel lead. */
    @Inject("${retrieval.graph-fusion.text-weight:0.25}")
    private double graphFusionTextWeight = 0.25D;
    /**
     * Largest share of the candidate budget the text probe may take before graph expansion.
     *
     * <p>Everything else goes to the graph channel, which must never receive less than a text
     * channel. At {@code 0.4} a 50-candidate request leaves the graph channel 30 candidates.
     */
    @Inject("${retrieval.graph-fusion.probe-share:0.4}")
    private double graphProbeShare = 0.4D;
    /** BM25 fusion weight when the query shows lexical confidence. */
    @Inject("${retrieval.graph-fusion.lexical-bm25-weight:1.25}")
    private double lexicalBm25Weight = 1.25D;
    /** Vector fusion weight when the query shows lexical confidence. */
    @Inject("${retrieval.graph-fusion.lexical-vector-weight:0.85}")
    private double lexicalVectorWeight = 0.85D;
    /** BM25 fusion weight when the query reads as a long semantic question. */
    @Inject("${retrieval.graph-fusion.semantic-bm25-weight:0.85}")
    private double semanticBm25Weight = 0.85D;
    /** Vector fusion weight when the query reads as a long semantic question. */
    @Inject("${retrieval.graph-fusion.semantic-vector-weight:1.25}")
    private double semanticVectorWeight = 1.25D;
    /**
     * Rank constant of the weighted reciprocal-rank fusion.
     *
     * <p>Sets the units of every fused score, {@code weight / (rankConstant + rank)}, and therefore
     * the reachable range of the first-stage score gap that the routing thresholds compare against.
     * Configurable so a threshold and the fusion it measures cannot drift apart again.
     */
    @Inject("${retrieval.rrf-rank-constant:60}")
    private int rrfRankConstant = 60;
    /**
     * Smallest probe score gap that still lets the cascaded graph stage run.
     *
     * <p>Expressed in fusion units, {@code w / (rankConstant + rank)}. The measured 2Wiki run
     * reports a median cascade probe gap of {@code 0.00055}, so this threshold sits inside the
     * reachable band rather than above it.
     */
    @Inject("${retrieval.cascade.min-probe-score-gap:0.0001}")
    private double cascadeMinProbeScoreGap = 0.0001D;
    /** Minimum number of probe evidence candidates required before the graph stage runs. */
    @Inject("${retrieval.cascade.min-probe-evidence:2}")
    private int cascadeMinProbeEvidence = 2;
    /** Candidate budget below which cascaded plans fall back to an even probe split. */
    @Inject("${retrieval.cascade.min-budget:5}")
    private int cascadeMinBudget = 5;
    /** Upper bound on the probe budget per text channel. */
    @Inject("${retrieval.cascade.probe-max:10}")
    private int cascadeProbeMax = 10;
    /** Lower bound on the probe budget per text channel. */
    @Inject("${retrieval.cascade.probe-min:2}")
    private int cascadeProbeMin = 2;
    /** Divisor applied to the candidate budget to size the text probe. */
    @Inject("${retrieval.cascade.probe-divisor:8}")
    private int cascadeProbeDivisor = 8;
    /**
     * Weight of the graph path-quality feature in the deterministic reranker.
     *
     * <p>Stays at {@code 0.0} so the reranker reproduces the ordering previously produced by the
     * fusion alone. A nonzero value promotes shallower graph paths; it is configurable because the
     * design lists path quality as an explicit secondary feature, and a feature that cannot be
     * weighted cannot be evaluated.
     */
    @Inject("${retrieval.reranker.graph-path-weight:0.0}")
    private double rerankerGraphPathWeight = 0.0D;
    /**
     * Neighbour sampling rate.
     *
     * <p>Stays at {@code 1.0} (draw every neighbour) on purpose. Lowering it is a recall/cost
     * trade-off that must be measured per dataset: with any value below {@code 1.0} a single-edge
     * fixture can lose its only edge, and the measured end-to-end runs reach the graph channel
     * through very short frontiers where a 0.5 draw already changes which evidence is returned.
     * Operators that need bounded expansion on high-degree graphs set
     * {@code GEAFLOW_RETRIEVAL_GRAPH_SAMPLE_RATE} explicitly instead of inheriting a default that
     * silently trades recall for cost. The control is wired end to end, so setting it takes effect.
     */
    @Inject("${retrieval.graph.sample-rate:1.0}")
    private double graphSampleRate = 1.0D;
    @Inject("${retrieval.graph.min-marginal-yield:0.05}")
    private double graphMinMarginalYield = 0.05D;
    @Inject("${retrieval.graph.min-association-score:0.01}")
    private double graphMinAssociationScore = 0.01D;
    @Inject("${retrieval.graph.min-association-survival-rate:0.1}")
    private double graphMinAssociationSurvivalRate = 0.1D;
    /**
     * Relation-type priorities as {@code label:weight} pairs, highest first for neighbour capping.
     *
     * <p>A non-empty map is what makes relation-type priority a real control instead of dead
     * configuration: an empty map sends every edge through the {@code 1.0} fallback in
     * {@code GraphRetrievalConfig#relationPriority}, collapsing the ordering to a hash tie-break.
     * The shipped default therefore names the two relation types the ingestion pipeline emits.
     */
    @Inject("${retrieval.graph.relation-priorities:mentions:1.0,cooccurs:1.0}")
    private String graphRelationPrioritiesSpec = "mentions:1.0,cooccurs:1.0";
    private Map<String, Double> graphRelationPriorities = new LinkedHashMap<>();

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
        if (!(graphFusionGraphWeight >= 0.0D) || !Double.isFinite(graphFusionGraphWeight)
            || !(graphFusionTextWeight >= 0.0D) || !Double.isFinite(graphFusionTextWeight)
            || (graphFusionGraphWeight == 0.0D && graphFusionTextWeight == 0.0D)
            || !(lexicalBm25Weight >= 0.0D) || !Double.isFinite(lexicalBm25Weight)
            || !(lexicalVectorWeight >= 0.0D) || !Double.isFinite(lexicalVectorWeight)
            || !(semanticBm25Weight >= 0.0D) || !Double.isFinite(semanticBm25Weight)
            || !(semanticVectorWeight >= 0.0D) || !Double.isFinite(semanticVectorWeight)
            || !(graphProbeShare >= 0.0D) || !(graphProbeShare < 1.0D) || !Double.isFinite(graphProbeShare)) {
            throw invalid("graph-fusion weights and probe share are invalid");
        }
        if (rrfRankConstant < 1) {
            throw invalid("rrf-rank-constant must be at least 1");
        }
        if (!(cascadeMinProbeScoreGap >= 0.0D) || !Double.isFinite(cascadeMinProbeScoreGap)
            || cascadeMinProbeEvidence < 1 || cascadeMinBudget < 1
            || cascadeProbeMax < 1 || cascadeProbeMin < 1 || cascadeProbeDivisor < 1
            || cascadeProbeMin > cascadeProbeMax) {
            throw invalid("cascade thresholds are invalid");
        }
        if (!(rerankerGraphPathWeight >= 0.0D) || !Double.isFinite(rerankerGraphPathWeight)) {
            throw invalid("reranker graph-path-weight must be finite and non-negative");
        }
        validateGraphExpansion();
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

    /**
     * Rejects graph expansion controls outside their physical range.
     *
     * <p>These values are operator-tunable, so a typo such as a sample rate of {@code 1.5} or a
     * negative floor must fail at startup instead of silently producing a skewed retrieval.
     */
    private void validateGraphExpansion() {
        graphRelationPriorities = parseRelationPriorities(graphRelationPrioritiesSpec);
        if (!(graphSampleRate >= 0.0D) || !(graphSampleRate <= 1.0D) || !Double.isFinite(graphSampleRate)) {
            throw invalid("graph.sample-rate must be between 0.0 and 1.0");
        }
        if (!(graphMinMarginalYield >= 0.0D) || !Double.isFinite(graphMinMarginalYield)) {
            throw invalid("graph.min-marginal-yield must be finite and non-negative");
        }
        if (!(graphMinAssociationScore >= 0.0D) || !(graphMinAssociationScore <= 1.0D)
            || !Double.isFinite(graphMinAssociationScore)) {
            throw invalid("graph.min-association-score must be between 0.0 and 1.0");
        }
        if (!(graphMinAssociationSurvivalRate >= 0.0D) || !(graphMinAssociationSurvivalRate <= 1.0D)
            || !Double.isFinite(graphMinAssociationSurvivalRate)) {
            throw invalid("graph.min-association-survival-rate must be between 0.0 and 1.0");
        }
        for (Map.Entry<String, Double> entry : graphRelationPriorities.entrySet()) {
            if (entry.getKey() == null || entry.getKey().trim().isEmpty()) {
                throw invalid("graph.relation-priorities keys must be non-empty");
            }
            Double value = entry.getValue();
            if (value == null || !(value >= 0.0D) || !Double.isFinite(value)) {
                throw invalid("graph.relation-priorities values must be finite and non-negative");
            }
        }
    }

    public double getGraphSampleRate() {
        return graphSampleRate;
    }

    public void setGraphSampleRate(double graphSampleRate) {
        this.graphSampleRate = graphSampleRate;
    }

    public double getGraphMinMarginalYield() {
        return graphMinMarginalYield;
    }

    public void setGraphMinMarginalYield(double graphMinMarginalYield) {
        this.graphMinMarginalYield = graphMinMarginalYield;
    }

    public double getGraphMinAssociationScore() {
        return graphMinAssociationScore;
    }

    public void setGraphMinAssociationScore(double graphMinAssociationScore) {
        this.graphMinAssociationScore = graphMinAssociationScore;
    }

    public double getGraphMinAssociationSurvivalRate() {
        return graphMinAssociationSurvivalRate;
    }

    public void setGraphMinAssociationSurvivalRate(double graphMinAssociationSurvivalRate) {
        this.graphMinAssociationSurvivalRate = graphMinAssociationSurvivalRate;
    }

    public Map<String, Double> getGraphRelationPriorities() {
        return new LinkedHashMap<>(graphRelationPriorities);
    }

    public void setGraphRelationPrioritiesSpec(String graphRelationPrioritiesSpec) {
        this.graphRelationPrioritiesSpec = graphRelationPrioritiesSpec;
    }

    public String getGraphRelationPrioritiesSpec() {
        return graphRelationPrioritiesSpec;
    }

    public void setGraphRelationPriorities(Map<String, Double> graphRelationPriorities) {
        this.graphRelationPriorities = graphRelationPriorities == null
            ? new LinkedHashMap<>() : new LinkedHashMap<>(graphRelationPriorities);
        this.graphRelationPrioritiesSpec = formatRelationPriorities(this.graphRelationPriorities);
    }

    /**
     * Parses {@code label:weight,label:weight} into an ordered priority map.
     *
     * <p>Declaration order is preserved because it doubles as the deterministic tie-break when two
     * relation types share a weight: operators get the priority they declared instead of a value
     * that depends on hash ordering.
     */
    private static Map<String, Double> parseRelationPriorities(String spec) {
        Map<String, Double> parsed = new LinkedHashMap<>();
        if (blank(spec)) {
            return parsed;
        }
        for (String rawEntry : spec.split(",")) {
            String entry = rawEntry.trim();
            if (entry.isEmpty()) {
                continue;
            }
            int separator = entry.indexOf(':');
            if (separator <= 0 || separator == entry.length() - 1) {
                throw invalid("graph.relation-priorities entry must be label:weight, got '"
                    + entry + "'");
            }
            String label = entry.substring(0, separator).trim();
            String weight = entry.substring(separator + 1).trim();
            if (label.isEmpty()) {
                throw invalid("graph.relation-priorities keys must be non-empty");
            }
            try {
                parsed.put(label, Double.parseDouble(weight));
            } catch (NumberFormatException e) {
                throw invalid("graph.relation-priorities weight must be numeric, got '"
                    + weight + "'");
            }
        }
        return parsed;
    }

    private static String formatRelationPriorities(Map<String, Double> priorities) {
        StringBuilder spec = new StringBuilder();
        for (Map.Entry<String, Double> entry : priorities.entrySet()) {
            if (spec.length() > 0) {
                spec.append(',');
            }
            spec.append(entry.getKey()).append(':').append(entry.getValue());
        }
        return spec.toString();
    }

    public int getBaseNeighborCap() {
        return baseNeighborCap;
    }

    public void setBaseNeighborCap(int baseNeighborCap) {
        this.baseNeighborCap = baseNeighborCap;
    }

    public double getGraphFusionGraphWeight() {
        return graphFusionGraphWeight;
    }

    public void setGraphFusionGraphWeight(double graphFusionGraphWeight) {
        this.graphFusionGraphWeight = graphFusionGraphWeight;
    }

    public double getGraphFusionTextWeight() {
        return graphFusionTextWeight;
    }

    public void setGraphFusionTextWeight(double graphFusionTextWeight) {
        this.graphFusionTextWeight = graphFusionTextWeight;
    }

    public double getGraphProbeShare() {
        return graphProbeShare;
    }

    public void setGraphProbeShare(double graphProbeShare) {
        this.graphProbeShare = graphProbeShare;
    }

    public double getLexicalBm25Weight() {
        return lexicalBm25Weight;
    }

    public void setLexicalBm25Weight(double lexicalBm25Weight) {
        this.lexicalBm25Weight = lexicalBm25Weight;
    }

    public double getLexicalVectorWeight() {
        return lexicalVectorWeight;
    }

    public void setLexicalVectorWeight(double lexicalVectorWeight) {
        this.lexicalVectorWeight = lexicalVectorWeight;
    }

    public double getSemanticBm25Weight() {
        return semanticBm25Weight;
    }

    public void setSemanticBm25Weight(double semanticBm25Weight) {
        this.semanticBm25Weight = semanticBm25Weight;
    }

    public double getSemanticVectorWeight() {
        return semanticVectorWeight;
    }

    public void setSemanticVectorWeight(double semanticVectorWeight) {
        this.semanticVectorWeight = semanticVectorWeight;
    }

    public int getRrfRankConstant() {
        return rrfRankConstant;
    }

    public void setRrfRankConstant(int rrfRankConstant) {
        this.rrfRankConstant = rrfRankConstant;
    }

    public double getCascadeMinProbeScoreGap() {
        return cascadeMinProbeScoreGap;
    }

    public void setCascadeMinProbeScoreGap(double cascadeMinProbeScoreGap) {
        this.cascadeMinProbeScoreGap = cascadeMinProbeScoreGap;
    }

    public int getCascadeMinProbeEvidence() {
        return cascadeMinProbeEvidence;
    }

    public void setCascadeMinProbeEvidence(int cascadeMinProbeEvidence) {
        this.cascadeMinProbeEvidence = cascadeMinProbeEvidence;
    }

    public int getCascadeMinBudget() {
        return cascadeMinBudget;
    }

    public void setCascadeMinBudget(int cascadeMinBudget) {
        this.cascadeMinBudget = cascadeMinBudget;
    }

    public int getCascadeProbeMax() {
        return cascadeProbeMax;
    }

    public void setCascadeProbeMax(int cascadeProbeMax) {
        this.cascadeProbeMax = cascadeProbeMax;
    }

    public int getCascadeProbeMin() {
        return cascadeProbeMin;
    }

    public void setCascadeProbeMin(int cascadeProbeMin) {
        this.cascadeProbeMin = cascadeProbeMin;
    }

    public int getCascadeProbeDivisor() {
        return cascadeProbeDivisor;
    }

    public void setCascadeProbeDivisor(int cascadeProbeDivisor) {
        this.cascadeProbeDivisor = cascadeProbeDivisor;
    }

    public double getRerankerGraphPathWeight() {
        return rerankerGraphPathWeight;
    }

    public void setRerankerGraphPathWeight(double rerankerGraphPathWeight) {
        this.rerankerGraphPathWeight = rerankerGraphPathWeight;
    }
}
