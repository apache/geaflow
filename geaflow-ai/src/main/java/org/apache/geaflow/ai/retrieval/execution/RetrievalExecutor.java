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
package org.apache.geaflow.ai.retrieval.execution;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;
import org.apache.geaflow.ai.retrieval.answer.AnswerRequest;
import org.apache.geaflow.ai.retrieval.answer.AnswerResponse;
import org.apache.geaflow.ai.retrieval.answer.AnswerSynthesizer;
import org.apache.geaflow.ai.retrieval.api.model.ExecutionMode;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalBudget;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalCommand;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalErrorCode;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalException;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalMode;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalResponse;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalTrace;
import org.apache.geaflow.ai.retrieval.api.model.TraceStage;
import org.apache.geaflow.ai.retrieval.channel.graph.EntityAnchorResolver;
import org.apache.geaflow.ai.retrieval.channel.graph.GraphNeighborProvider;
import org.apache.geaflow.ai.retrieval.channel.graph.GraphQueryLease;
import org.apache.geaflow.ai.retrieval.channel.graph.GraphRetrievalConfig;
import org.apache.geaflow.ai.retrieval.channel.graph.GraphRetriever;
import org.apache.geaflow.ai.retrieval.config.RetrievalProperties;
import org.apache.geaflow.ai.retrieval.execution.BudgetConstraint;
import org.apache.geaflow.ai.retrieval.execution.BudgetLedger;
import org.apache.geaflow.ai.retrieval.execution.DeterministicSeed;
import org.apache.geaflow.ai.retrieval.execution.PlanAdjustment;
import org.apache.geaflow.ai.retrieval.execution.RecallPlan;
import org.apache.geaflow.ai.retrieval.execution.RecallStageStatus;
import org.apache.geaflow.ai.retrieval.execution.RecallStopReason;
import org.apache.geaflow.ai.retrieval.execution.Reranker;
import org.apache.geaflow.ai.retrieval.execution.RetrievalPlan;
import org.apache.geaflow.ai.retrieval.execution.RetrievalPlanValidator;
import org.apache.geaflow.ai.retrieval.execution.RetrievalProgress;
import org.apache.geaflow.ai.retrieval.execution.RetrievalReplanEvent;
import org.apache.geaflow.ai.retrieval.execution.RetrievalStrategyFactory;
import org.apache.geaflow.ai.retrieval.execution.RoutingContext;
import org.apache.geaflow.ai.retrieval.execution.RoutingFeatures;
import org.apache.geaflow.ai.retrieval.execution.StageExecutor;
import org.apache.geaflow.ai.retrieval.execution.StageExecutor.Stage;
import org.apache.geaflow.ai.retrieval.execution.StageExecutor.StageResult;
import org.apache.geaflow.ai.retrieval.execution.TokenContextTruncator;
import org.apache.geaflow.ai.retrieval.execution.TokenEstimator;
import org.apache.geaflow.ai.retrieval.fusion.CandidateFusion;
import org.apache.geaflow.ai.retrieval.fusion.WeightedRrfConfig;
import org.apache.geaflow.ai.retrieval.fusion.WeightedRrfFusion;
import org.apache.geaflow.ai.retrieval.index.ChannelSearchResult;
import org.apache.geaflow.ai.retrieval.index.bm25.Bm25Hit;
import org.apache.geaflow.ai.retrieval.index.bm25.Bm25IndexReader;
import org.apache.geaflow.ai.retrieval.index.vector.VectorHit;
import org.apache.geaflow.ai.retrieval.index.vector.VectorIndexReader;
import org.apache.geaflow.ai.retrieval.model.document.RetrievalIds;
import org.apache.geaflow.ai.retrieval.model.document.SourceRef;
import org.apache.geaflow.ai.retrieval.model.document.TextChunk;
import org.apache.geaflow.ai.retrieval.model.evidence.ChannelScore;
import org.apache.geaflow.ai.retrieval.model.evidence.Evidence;
import org.apache.geaflow.ai.retrieval.model.evidence.EvidenceKind;
import org.apache.geaflow.ai.retrieval.model.evidence.GraphEvidenceTrace;
import org.apache.geaflow.ai.retrieval.model.graph.EntityRef;
import org.apache.geaflow.ai.retrieval.model.graph.GraphEdgeRef;

/** Executes one pinned retrieval request across routing, channel, fusion, and trace stages. */
public final class RetrievalExecutor {
    /**
     * Documented default probe gap; the effective value comes from {@link RetrievalProperties}.
     */
    private static final double CASCADE_MIN_PROBE_SCORE_GAP = 0.0001D;
    private static final String UNMEASURED_SCORE_GAP_REASON = "FIRST_STAGE_SCORE_GAP_UNMEASURED";
    private static final String UNKNOWN_GROWTH_REASON = "GRAPH_GROWTH_UNKNOWN_INITIAL";
    private static final long DEFAULT_CONFIG_SEED = 0L;
    private static final int DEFAULT_MAX_EDGE_SCANS = 100;
    /** Token ceiling for the extractive answer when the request does not set a token budget. */
    private static final int DEFAULT_ANSWER_TOKEN_BUDGET = 4096;
    /**
     * Association-score floor applied when no property overrides it.
     *
     * <p>The design lists "all remaining paths are below the minimum association score" as a
     * diffusion stop. Leaving the floor at zero made that stop unreachable in production, so a
     * weakly-related neighbourhood could consume the whole edge-scan budget. The score is
     * multiplicative and hop-decayed, so the floor is expressed relative to the strongest path in
     * the hop rather than as an absolute constant.
     */
    private static final double DEFAULT_MIN_ASSOCIATION_SCORE = 0.01D;
    /**
     * Fraction of scored edges that must survive the association floor for a hop to keep running.
     *
     * <p>Zero disabled the early stop entirely: every rejected edge still consumed scan budget.
     */
    private static final double DEFAULT_MIN_ASSOCIATION_SURVIVAL_RATE = 0.05D;
    private final Bm25IndexReader bm25;
    private final VectorIndexReader vector;
    private final List<TextChunk> chunks;
    private final Map<String, TextChunk> chunksById;
    private final List<EntityRef> entities;
    private final EntityAnchorResolver anchorResolver;
    private final Map<String, Double> idfByToken;
    private final List<GraphEdgeRef> edges;
    private final GraphNeighborProvider graphProvider;
    private final CandidateFusion fusion;
    private final LongSupplier clock;
    private final TokenEstimator tokenEstimator;
    private final Reranker reranker;
    private final AdaptivePolicy routingPolicy;
    /** Stage boundary reached between two graph hops, where only contraction can still act. */
    static final String BOUNDARY_AFTER_GRAPH_HOP = "AFTER_GRAPH_HOP";

    private final int maxReplanCount;
    private final boolean strictPlanValidation;
    private final RetrievalProperties properties;

    private final AnswerSynthesizer answerSynthesizer;

    public RetrievalExecutor(Bm25IndexReader bm25, VectorIndexReader vector, List<TextChunk> chunks,
                             Map<String, TextChunk> chunksById, List<EntityRef> entities,
                             EntityAnchorResolver anchorResolver, Map<String, Double> idfByToken,
                             List<GraphEdgeRef> edges, GraphNeighborProvider graphProvider, CandidateFusion fusion,
                             LongSupplier clock, TokenEstimator tokenEstimator, Reranker reranker,
                             AdaptivePolicy routingPolicy, int maxReplanCount,
                             boolean strictPlanValidation, RetrievalProperties properties) {
        if (maxReplanCount < 0) {
            throw new IllegalArgumentException("maxReplanCount must be non-negative");
        }
        this.strictPlanValidation = strictPlanValidation;
        this.properties = properties == null ? new RetrievalProperties() : properties;
        this.bm25 = bm25;
        this.vector = vector;
        this.chunks = chunks == null ? Collections.emptyList() : Collections.unmodifiableList(new ArrayList<>(chunks));
        this.chunksById = chunksById == null ? Collections.emptyMap()
            : Collections.unmodifiableMap(new LinkedHashMap<>(chunksById));
        this.entities = entities == null ? Collections.emptyList() : Collections.unmodifiableList(new ArrayList<>(entities));
        this.anchorResolver = Objects.requireNonNull(anchorResolver, "anchorResolver");
        this.idfByToken = idfByToken == null ? Collections.emptyMap()
            : Collections.unmodifiableMap(new LinkedHashMap<>(idfByToken));
        this.edges = edges == null ? Collections.emptyList() : Collections.unmodifiableList(new ArrayList<>(edges));
        this.graphProvider = graphProvider;
        this.fusion = Objects.requireNonNull(fusion, "fusion");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.tokenEstimator = Objects.requireNonNull(tokenEstimator, "tokenEstimator");
        this.reranker = Objects.requireNonNull(reranker, "reranker");
        this.routingPolicy = Objects.requireNonNull(routingPolicy, "routingPolicy");
        this.maxReplanCount = maxReplanCount;
        this.answerSynthesizer = RetrievalStrategyFactory.answerSynthesizer(tokenEstimator);
    }

    public RetrievalResponse execute(RetrievalCommand command, float[] queryVector, long deadline,
                                     boolean partial, long started, RequestResources resources,
                                     String requestId) {
        GraphNeighborProvider pinnedGraphProvider = resources.getGraphProvider() == null
            ? graphProvider : resources.getGraphProvider();
        return executePinned(command, queryVector, deadline, partial, started, resources, requestId,
            pinnedGraphProvider);
    }

    /** Immutable resources pinned for one registry-backed request. */
    public static final class RequestResources {
        private final String graphVersion;
        private final String indexVersion;
        private final Map<String, RetrievalException> failures;
        private final GraphNeighborProvider graphProvider;
        private final RequestLeaseFactory leaseFactory;

        public RequestResources(String graphVersion, String indexVersion,
                                Map<String, RetrievalException> failures,
                                GraphNeighborProvider graphProvider, RequestLeaseFactory leaseFactory) {
            this.graphVersion = graphVersion;
            this.indexVersion = indexVersion;
            this.failures = failures == null ? Collections.emptyMap()
                : Collections.unmodifiableMap(new LinkedHashMap<>(failures));
            this.graphProvider = graphProvider;
            this.leaseFactory = leaseFactory;
        }

        public static RequestResources empty() {
            return new RequestResources(null, null, Collections.emptyMap(), null, null);
        }

        private String getGraphVersion() {
            return graphVersion;
        }

        private String getIndexVersion() {
            return indexVersion;
        }

        private Map<String, RetrievalException> getFailures() {
            return failures;
        }

        private GraphNeighborProvider getGraphProvider() {
            return graphProvider;
        }

        private AutoCloseable acquireLease() {
            return leaseFactory == null ? null : leaseFactory.acquire();
        }
    }

    @FunctionalInterface
    public interface RequestLeaseFactory {
        AutoCloseable acquire();
    }

    private InitialRouting routingContext(RetrievalCommand command, GraphNeighborProvider provider,
                                          RetrievalBudget budget, int candidateLimit) {
        int queryLength = LexicalStatistics.queryLength(command.getQuery());
        double averageIdf = LexicalStatistics.averageIdf(command.getQuery(), idfByToken);
        List<EntityAnchorResolver.Anchor> anchors = anchorResolver.resolve(command.getQuery(), 0L, clock);
        int maxDegree = 0;
        double confidence = 0.0D;
        for (EntityAnchorResolver.Anchor anchor : anchors) {
            confidence = Math.max(confidence, anchor.getConfidence());
            if (provider != null && provider.getStatistics() != null) {
                maxDegree = Math.max(maxDegree,
                    provider.getStatistics().getDegree(anchor.getEntity().getEntityId()));
            }
        }
        final RoutingFeatures features = RoutingFeatures.builder().queryLength(queryLength)
            .averageIdf(averageIdf).anchorCount(anchors.size()).anchorConfidence(confidence)
            .candidateDegree(maxDegree).build();
        List<String> notMeasured = new ArrayList<>();
        if (anchors.isEmpty()) {
            notMeasured.add(UNMEASURED_SCORE_GAP_REASON);
        }
        if (provider == null || provider.getStatistics() == null) {
            notMeasured.add(UNKNOWN_GROWTH_REASON);
        }
        List<String> available = new ArrayList<>();
        if (bm25 != null) {
            available.add("BM25");
        }
        if (vector != null) {
            available.add("VECTOR");
        }
        if (provider != null) {
            available.add("GRAPH");
        }
        int maxCandidates = candidateLimit;
        RetrievalBudget boundedBudget = new RetrievalBudget(budget.getTopK(), budget.getTimeoutMs(),
            maxCandidates, budget.getTokenBudget(), budget.getPrimaryConstraint());
        RoutingContext context = RoutingContext.builder().query(command.getQuery()).features(features)
            .availableIndexes(available).budget(boundedBudget).requestedMode(command.getMode())
            .executionMode(command.getExecutionMode()).maxHop(properties.getMaxHop())
            .maxReplanCount(maxReplanCount)
            .configVersion("v1").seed(DeterministicSeed.routingSeed(command.getQuery(), configSeed())).build();
        return new InitialRouting(context, notMeasured);
    }

    /** Initial routing context plus the features that could not be measured before the first stage. */
    private static final class InitialRouting {
        private final RoutingContext context;
        private final List<String> notMeasured;

        private InitialRouting(RoutingContext context, List<String> notMeasured) {
            this.context = context;
            this.notMeasured = java.util.Collections.unmodifiableList(new ArrayList<>(notMeasured));
        }
    }

    private boolean isDefaultFusionConfig() {
        return fusion.getConfig().getBm25Weight() == 1.0D
            && fusion.getConfig().getVectorWeight() == 1.0D
            && fusion.getConfig().getGraphWeight() == 1.0D
            && fusion.getConfig().getRankConstant() == 60;
    }

    private RetrievalProgress routingProgress(RetrievalTrace trace, RoutingContext context, long deadline,
                                               RecallPlan plan, BudgetLedger ledger,
                                               Map<String, EvidenceBuilder> merged, int replanCount,
                                               String boundary) {
        int candidatesUsed = candidatesUsed(trace);
        int tokenUsed = tokensUsed(trace, ledger);
        long remainingLatency = deadline == 0L ? 0L : Math.max(0L, deadline - clock.getAsLong());
        int anchorCount = trace.getAnchorsConsidered() == 0
            ? context.getFeatures().getAnchorCount() : trace.getAnchorsConsidered();
        double confidence = 0.0D;
        if (anchorCount > 0) {
            for (EntityAnchorResolver.Anchor anchor : anchorResolver.resolve(context.getQuery(), 0L, clock)) {
                confidence = Math.max(confidence, anchor.getConfidence());
            }
        }
        return RetrievalProgress.builder().boundary(boundary)
            .completedChannels(trace.getChannelStatuses().keySet())
            .candidateCounts(trace.getCandidateCounts())
            .remainingCandidates(Math.max(0, context.getBudget().getMaxCandidates() - candidatesUsed))
            .remainingTokens(Math.max(0, context.getBudget().getTokenBudget() - tokenUsed))
            .remainingLatencyNanos(remainingLatency).firstStageScoreGap(cascadeProbeScoreGap(merged))
            .anchorCount(anchorCount).anchorConfidence(confidence)
            .candidateDegree(context.getFeatures().getCandidateDegree())
            .graphGrowthRate(trace.getGraphGrowthRate()).graphMarginalYield(trace.getGraphMarginalYield())
            .replanCount(replanCount).build();
    }

    private static int candidatesUsed(RetrievalTrace trace) {
        // Budget consumption is the candidate output, not the scoring work behind it. A channel
        // that ranked its whole index still contributes at most its candidate share downstream,
        // so counting scored documents here would let one channel spend the request's budget and
        // leave later stages with a zero allocation.
        int candidatesUsed = 0;
        for (Integer count : trace.getCandidateCounts().values()) {
            candidatesUsed += count;
        }
        return candidatesUsed;
    }

    private int tokensUsed(RetrievalTrace trace, BudgetLedger ledger) {
        return ledger == null ? (int) Math.min(Integer.MAX_VALUE, trace.getTokensRetained())
            : (int) Math.min(Integer.MAX_VALUE, ledger.getTokensRetained());
    }

    /** Candidates the previous plan allocated but the completed stages never evaluated. */
    private static int unusedCandidates(RetrievalPlan previous, RetrievalTrace trace) {
        int allocated = 0;
        for (Integer budget : previous.getChannelBudgets().values()) {
            allocated += budget;
        }
        return Math.max(0, Math.max(allocated, previous.getCandidateBudget()) - candidatesUsed(trace));
    }

    /** Tokens the previous plan reserved but the completed stages never retained. */
    private int unusedTokens(RetrievalPlan previous, RetrievalTrace trace, BudgetLedger ledger) {
        return Math.max(0, previous.getTokenBudget() - tokensUsed(trace, ledger));
    }

    /**
     * True when two plans take the same routing decision, so replanning would not change execution.
     * The replan loop stops on this to avoid spending {@code maxReplanCount} on no-op rounds.
     */
    static boolean sameRoutingDecision(RetrievalPlan left, RetrievalPlan right) {
        if (left == right) {
            return true;
        }
        if (left == null || right == null || left.getSelectedMode() != right.getSelectedMode()
            || left.getMaxHop() != right.getMaxHop() || left.getNeighborCap() != right.getNeighborCap()
            || left.getEdgeScanCap() != right.getEdgeScanCap()
            || Double.compare(left.getMinMarginalYield(), right.getMinMarginalYield()) != 0
            || Double.compare(left.getMinAssociationScore(), right.getMinAssociationScore()) != 0
            || !left.getChannelBudgets().equals(right.getChannelBudgets())
            || !left.getChannelOrder().equals(right.getChannelOrder())) {
            return false;
        }
        return Double.compare(left.getBm25Weight(), right.getBm25Weight()) == 0
            && Double.compare(left.getVectorWeight(), right.getVectorWeight()) == 0
            && Double.compare(left.getGraphWeight(), right.getGraphWeight()) == 0;
    }

    private static Map<String, Integer> orderedBudgets(RetrievalPlan plan) {
        Map<String, Integer> ordered = new LinkedHashMap<>();
        for (String channel : plan.getChannelOrder()) {
            Integer value = plan.getChannelBudgets().get(channel);
            if (value != null) {
                ordered.put(channel, value);
            }
        }
        return ordered;
    }

    /**
     * Merges the features measured after the first stages into the routing feature snapshot so the
     * trace records what actually drove the final plan instead of the pre-measurement estimate.
     */
    private static Map<String, Double> measuredFeatures(RoutingFeatures initial, RetrievalProgress progress) {
        RoutingFeatures measured = RoutingFeatures.builder().queryLength(initial.getQueryLength())
            .averageIdf(initial.getAverageIdf()).anchorCount(progress.getAnchorCount())
            .anchorConfidence(progress.getAnchorConfidence())
            .candidateDegree(progress.getCandidateDegree())
            .firstStageScoreGap(progress.getFirstStageScoreGap())
            .graphGrowthRate(progress.getGraphGrowthRate()).build();
        Map<String, Double> values = new LinkedHashMap<>();
        for (Map.Entry<String, Number> feature : measured.asMap().entrySet()) {
            values.put(feature.getKey(), feature.getValue().doubleValue());
        }
        return values;
    }

    private static List<String> candidateChunkIds(Map<String, EvidenceBuilder> candidates) {
        List<String> result = new ArrayList<>();
        for (EvidenceBuilder candidate : candidates.values()) {
            result.add(candidate.chunk.getChunkId());
        }
        return result;
    }

    private RetrievalResponse executePinned(RetrievalCommand command, float[] queryVector, long deadline,
                                             boolean partial, long started,
                                             RequestResources resources, String requestId,
                                             GraphNeighborProvider pinnedGraphProvider) {
        RetrievalBudget budget = command.getBudget();
        int executionCandidateLimit = budget.getPrimaryConstraint() == BudgetConstraint.CANDIDATES
            ? budget.getMaxCandidates() : command.getExecutionCandidateLimit();
        budget.setMaxEdgeScans(resolveEdgeScanBudget(budget));
        RetrievalPlan routingPlan = null;
        // Adaptive routing is selected by the requested mode. The design says adaptive hybrid should
        // default to cascaded execution, so cascaded must not be the one mode that skips routing.
        boolean adaptive = command.getMode().isAdaptive() && bm25 != null && vector != null
            && routingPolicy.supports(command.getMode());
        RoutingContext routingContext = null;
        List<String> initialRoutingReasons = Collections.emptyList();
        if (adaptive) {
            AdaptivePolicy policy = routingPolicy;
            InitialRouting initial = routingContext(command, pinnedGraphProvider, budget, executionCandidateLimit);
            routingContext = initial.context;
            initialRoutingReasons = initial.notMeasured;
            routingPlan = strictPlanValidation
                ? RetrievalPlanValidator.validate(policy.plan(routingContext), routingContext)
                : RetrievalPlanValidator.validateAndAdjust(policy.plan(routingContext), routingContext).getPlan();
            if (!isDefaultFusionConfig()) {
                routingPlan = RetrievalPlan.builder(routingPlan)
                    .bm25Weight(fusion.getConfig().getBm25Weight())
                    .vectorWeight(fusion.getConfig().getVectorWeight())
                    .graphWeight(fusion.getConfig().getGraphWeight()).build();
            }
        }
        RetrievalPlan effectiveRoutingPlan = routingPlan;
        RecallPlan plan = routingPlan == null
            ? new RecallPlan(command.getMode(), budget.getTopK(), executionCandidateLimit, deadline,
                resources.getGraphVersion(), resources.getIndexVersion(),
                vector == null ? null : vector.getVectorVersion(), budget.getPrimaryConstraint(),
                command.getExecutionMode(), cascadeMinBudget(), cascadeProbeMax(), cascadeProbeMin(),
                cascadeProbeDivisor())
            : new RecallPlan(routingPlan.getSelectedMode(), routingPlan.getTopK(), deadline,
                resources.getGraphVersion(), resources.getIndexVersion(),
                vector == null ? null : vector.getVectorVersion(), budget.getPrimaryConstraint(),
                command.getExecutionMode(), orderedBudgets(routingPlan), routingPlan.getMaxHop(),
                routingPlan.getNeighborCap(), routingPlan.getMaxDiffusionGrowth(),
                routingPlan.getEdgeScanCap(), routingPlan.getMinMarginalYield(),
                routingPlan.getMinAssociationScore());
        Map<String, RetrievalException> failures = resources.getFailures();
        BudgetLedger tokenLedger = budget.getPrimaryConstraint() == BudgetConstraint.TOKENS
            ? new BudgetLedger(executionCandidateLimit, budget.getTokenBudget(), deadline, clock) : null;
        if (plan.getChannelBudgets().containsKey("VECTOR")) {
            VectorIndexReader.validateValues(queryVector);
            if (vector != null) {
                vector.validateQuery(queryVector);
            }
        }
        Map<String, TextChunk> byId = chunksById;
        Map<String, EvidenceBuilder> merged = new LinkedHashMap<>();
        RetrievalTrace trace = new RetrievalTrace();
        trace.setTraceVersion("v1");
        trace.setGraphVersion(plan.getGraphVersion());
        if (pinnedGraphProvider != null) {
            if (pinnedGraphProvider.getGraphVersion() != null) {
                trace.setVisibleGraphVersion(pinnedGraphProvider.getGraphVersion().getVersion());
                trace.setActualVisibleGraphVersion(pinnedGraphProvider.getGraphVersion().getVersion());
            }
            trace.setDeltaDigest(pinnedGraphProvider.getDeltaDigest());
            trace.setStatisticsVersion(pinnedGraphProvider.getStatisticsVersion());
        }
        trace.setIndexVersion(plan.getIndexVersion());
        trace.setVectorVersion(plan.getVectorVersion());
        trace.setOriginalQuery(command.getQuery());
        trace.setSelectedMode(plan.getMode());
        trace.setRequestedMode(command.getRequestedMode());
        trace.setExecutionMode(command.getExecutionMode());
        trace.setExactRanking(command.isExactRanking());
        trace.setTokenEstimatorVersion(tokenEstimator.getVersion());
        trace.setRerankerVersion(reranker.getVersion());
        trace.setFusionVersion(fusion.getVersion());
        trace.setPrimaryConstraint(budget.getPrimaryConstraint().name());
        if (routingPlan != null) {
            RoutingFeatures features = routingContext.getFeatures();
            trace.setPolicyId(routingPlan.getPolicyId());
            trace.setPolicyVersion(routingPlan.getPolicyVersion());
            trace.setFeatureSchemaVersion(features.getSchemaVersion());
            Map<String, Double> featureValues = new LinkedHashMap<>();
            for (Map.Entry<String, Number> feature : features.asMap().entrySet()) {
                featureValues.put(feature.getKey(), feature.getValue().doubleValue());
            }
            trace.setRoutingFeatures(featureValues);
            List<String> reasons = new ArrayList<>(routingPlan.getReasons());
            reasons.addAll(initialRoutingReasons);
            trace.setRoutingReasons(reasons);
            trace.setPolicyParameters(new LinkedHashMap<>(routingPolicy.getParameters()));
        }
        trace.setSelectedChannels(new ArrayList<>(plan.getChannelBudgets().keySet()));
        trace.setChannelBudgets(plan.getChannelBudgets());
        int effectiveCandidateBudget = 0;
        for (Integer channelBudget : plan.getChannelBudgets().values()) {
            effectiveCandidateBudget += channelBudget;
        }
        trace.setEffectiveCandidateBudget(effectiveCandidateBudget);
        trace.setEffectiveTopK(plan.getTopK());
        trace.setBm25Weight(routingPlan == null ? fusion.getConfig().getBm25Weight()
            : routingPlan.getBm25Weight());
        trace.setVectorWeight(routingPlan == null ? fusion.getConfig().getVectorWeight()
            : routingPlan.getVectorWeight());
        trace.setGraphWeight(routingPlan == null ? fusion.getConfig().getGraphWeight()
            : routingPlan.getGraphWeight());
        trace.setRrfRankConstant(fusion.getConfig().getRankConstant());
        Map<String, Long> channelElapsedNanos = new LinkedHashMap<>();
        for (String name : plan.getChannelBudgets().keySet()) {
            trace.getEvaluatedCounts().put(name, 0);
            trace.getCandidateCounts().put(name, 0);
            trace.getRawCandidatesScored().put(name, 0);
        }
        List<Map.Entry<String, Integer>> channels = new ArrayList<>(plan.getChannelBudgets().entrySet());
        boolean cascaded = command.getExecutionMode() == ExecutionMode.CASCADED
            && plan.getMode() == RetrievalMode.HYBRID;
        if (cascaded) {
            List<Map.Entry<String, Integer>> probes = new ArrayList<>();
            for (Map.Entry<String, Integer> channel : channels) {
                if (!"GRAPH".equals(channel.getKey())) {
                    probes.add(channel);
                }
            }
            runChannels(probes, ExecutionMode.SEQUENTIAL, command, queryVector, deadline, partial,
                requestId, failures, byId, plan, merged, trace, channelElapsedNanos, resources,
                pinnedGraphProvider, tokenLedger);
            boolean probeDeadline = expired(deadline)
                || trace.getChannelStopReasons().containsValue(RecallStopReason.DEADLINE);
            double minProbeScoreGap = cascadeMinProbeScoreGap();
            int minProbeEvidence = cascadeMinProbeEvidence();
            boolean hasTextEvidence = !merged.isEmpty();
            boolean hasProbeEvidence = merged.size() >= minProbeEvidence;
            boolean hasAnchor = hasTextEvidence && !probeDeadline
                && !anchorResolver.resolve(command.getQuery(), deadline, clock).isEmpty();
            double scoreGap = cascadeProbeScoreGap(merged);
            trace.setCascadeProbeScoreGap(scoreGap);
            // The evaluated threshold is recorded alongside the measured gap so a skipped graph
            // stage can be explained from the trace alone.
            trace.setCascadeMinProbeScoreGap(minProbeScoreGap);
            trace.setCascadeMinProbeEvidence(minProbeEvidence);
            boolean noAnchorFallback = hasTextEvidence && hasProbeEvidence && !hasAnchor
                && scoreGap >= minProbeScoreGap;
            if (probeDeadline || !hasTextEvidence || !hasProbeEvidence
                || (scoreGap < minProbeScoreGap && !noAnchorFallback)) {
                RecallStopReason reason = probeDeadline ? RecallStopReason.DEADLINE
                    : !hasTextEvidence ? RecallStopReason.NO_RESULT
                    : !hasProbeEvidence ? RecallStopReason.NO_RESULT
                    : RecallStopReason.LOW_SCORE_GAP;
                String decision = probeDeadline ? "SKIP_DEADLINE"
                    : !hasTextEvidence ? "SKIP_NO_EVIDENCE"
                    : !hasProbeEvidence ? "SKIP_INSUFFICIENT_EVIDENCE"
                    : "SKIP_LOW_SCORE_GAP";
                String message = probeDeadline ? "cascaded probe exhausted the latency budget"
                    : !hasTextEvidence ? "cascaded probe produced no text evidence"
                    : !hasProbeEvidence ? "cascaded probe produced fewer than two evidence candidates"
                    : "cascaded probe score gap is below the graph-stage threshold";
                trace.setCascadeDecision(decision);
                if (probeDeadline) {
                    timedOut(partial);
                }
                record(trace, "GRAPH", RecallStageStatus.NOT_RUN, reason, message);
                channelElapsedNanos.put("GRAPH", 0L);
                trace.getStageElapsedNanos().put("GRAPH", 0L);
            } else {
                trace.setCascadeDecision(hasAnchor ? "RUN_GRAPH" : "RUN_GRAPH_FALLBACK_NO_ANCHOR");
                // Adaptive cascaded runs still replan at this boundary: the probe produced real
                // score-gap, yield and degree statistics, which is exactly what the design says
                // the policy should consume before the graph stage is sized.
                RecallPlan graphPlan = plan;
                if (routingPlan != null) {
                    StageReplan probeReplan = replanAtStageBoundary(routingPlan, routingContext, trace,
                        deadline, plan, tokenLedger, merged, "AFTER_TEXT_PROBE");
                    effectiveRoutingPlan = probeReplan.plan;
                    graphPlan = withPlan(plan, probeReplan.plan);
                }
                runGraphInHops(graphPlan, routingPlan == null ? null : effectiveRoutingPlan, routingContext,
                    command, queryVector, deadline, partial, requestId, failures, byId, merged, trace,
                    channelElapsedNanos, resources, pinnedGraphProvider, tokenLedger);
            }
        } else if (routingPlan != null && plan.getChannelBudgets().containsKey("GRAPH")) {
            List<Map.Entry<String, Integer>> textChannels = new ArrayList<>();
            for (Map.Entry<String, Integer> channel : channels) {
                if (!"GRAPH".equals(channel.getKey())) {
                    textChannels.add(channel);
                }
            }
            runChannels(textChannels, command.getExecutionMode(), command, queryVector, deadline, partial,
                requestId, failures, byId, plan, merged, trace, channelElapsedNanos, resources,
                pinnedGraphProvider, tokenLedger);
            StageReplan replan = replanAtStageBoundary(routingPlan, routingContext, trace, deadline,
                plan, tokenLedger, merged, "AFTER_TEXT");
            RetrievalPlan replanned = replan.plan;
            RetrievalProgress progress = replan.progress;
            effectiveRoutingPlan = replanned;
            if (replan.count > 0) {
                trace.setRoutingFeatures(measuredFeatures(routingContext.getFeatures(), progress));
            }
            int textBudget = 0;
            for (String textChannel : Arrays.asList("BM25", "VECTOR")) {
                if (plan.getChannelBudgets().containsKey(textChannel)) {
                    // Charge the candidates a text channel retained, not the documents it scored.
                    // Ranking a full index is bounded work that still returns at most its share;
                    // charging the scan made text channels exhaust the request budget on their own
                    // and left the graph stage with nothing, even though the graph allocation was
                    // untouched.
                    textBudget += Math.max(1, trace.getCandidateCounts().get(textChannel));
                }
            }
            int graphBudget = 0;
            if (replanned.getChannelBudgets().containsKey("GRAPH")) {
                int replannedBudget = replanned.getChannelBudgets().get("GRAPH");
                int remainingAllocation = command.isExactRanking()
                    ? plan.getChannelBudgets().get("GRAPH")
                    : trace.getEffectiveCandidateBudget() - textBudget;
                graphBudget = Math.min(replannedBudget,
                    Math.min(remainingAllocation, progress.getRemainingCandidates()));
            }
            if (graphBudget > 0) {
                if (!command.isExactRanking()) {
                    for (String textChannel : Arrays.asList("BM25", "VECTOR")) {
                        if (plan.getChannelBudgets().containsKey(textChannel)) {
                            int retained = trace.getCandidateCounts().get(textChannel);
                            int allocated = Math.max(1, retained);
                            trace.getChannelBudgets().put(textChannel, allocated);
                        }
                    }
                }
                Map<String, Integer> remainingGraphBudget = Collections.singletonMap("GRAPH", graphBudget);
                if (!command.isExactRanking()) {
                    trace.getChannelBudgets().put("GRAPH", graphBudget);
                }
                RecallPlan graphPlan = new RecallPlan(plan.getMode(),
                    Math.min(plan.getTopK(), remainingGraphBudget.get("GRAPH")), plan.getDeadlineNanos(),
                    plan.getGraphVersion(), plan.getIndexVersion(), plan.getVectorVersion(),
                    budget.getPrimaryConstraint(), command.getExecutionMode(), remainingGraphBudget,
                    replanned.getMaxHop(), replanned.getNeighborCap(), replanned.getMaxDiffusionGrowth(),
                    replanned.getEdgeScanCap(), replanned.getMinMarginalYield(),
                    replanned.getMinAssociationScore());
                // The non-cascaded adaptive path gets the same hop boundary: whichever execution mode
                // runs, a measured hop must be able to stop the next one.
                runGraphInHops(graphPlan, replanned, routingContext, command, queryVector, deadline,
                    partial, requestId, failures, byId, merged, trace, channelElapsedNanos, resources,
                    pinnedGraphProvider, tokenLedger);
            } else {
                RecallStopReason skipReason = progress.getRemainingCandidates() == 0 || graphBudget == 0
                    ? RecallStopReason.CANDIDATE_BUDGET
                    : progress.getGraphGrowthRate() > replanned.getMaxDiffusionGrowth()
                        ? RecallStopReason.DIFFUSION_GROWTH_LIMIT : RecallStopReason.NO_RELIABLE_ANCHOR;
                record(trace, "GRAPH", RecallStageStatus.NOT_RUN, skipReason,
                    "adaptive policy did not schedule the graph stage at the boundary");
                channelElapsedNanos.put("GRAPH", 0L);
                trace.getStageElapsedNanos().put("GRAPH", 0L);
            }
        } else {
            runChannels(channels, command.getExecutionMode(), command, queryVector, deadline, partial,
                requestId, failures, byId, plan, merged, trace, channelElapsedNanos, resources,
                pinnedGraphProvider, tokenLedger);
        }
        List<Evidence> evidence = new ArrayList<>();
        for (EvidenceBuilder value : merged.values()) {
            evidence.add(value.build());
        }
        CandidateFusion activeFusion = fusion;
        // Only the weighted RRF fusion derives its channel weights from the routing plan. A custom
        // strategy keeps full control of its own weighting, so it must not be replaced here.
        if (effectiveRoutingPlan != null && fusion instanceof WeightedRrfFusion
            && isDefaultFusionConfig()) {
            activeFusion = new WeightedRrfFusion(new WeightedRrfConfig(effectiveRoutingPlan.getBm25Weight(),
                effectiveRoutingPlan.getVectorWeight(), effectiveRoutingPlan.getGraphWeight(),
                fusion.getConfig().getRankConstant()));
        }
        if (effectiveRoutingPlan != null) {
            trace.setBm25Weight(activeFusion.getConfig().getBm25Weight());
            trace.setVectorWeight(activeFusion.getConfig().getVectorWeight());
            trace.setGraphWeight(activeFusion.getConfig().getGraphWeight());
            trace.setRoutingReasons(effectiveRoutingPlan.getReasons());
        }
        if (plan.getMode() == RetrievalMode.HYBRID) {
            evidence = activeFusion.sort(evidence);
        } else {
            evidence.sort(Comparator.comparingDouble(Evidence::getFusedScore).reversed().thenComparing(Evidence::getEvidenceId));
        }
        evidence = reranker.rerank(evidence);
        trace.setUniqueEvidenceCandidates(evidence.size());
        if (plan.getMode() == RetrievalMode.HYBRID) {
            // The quota keeps graph evidence in the window; it must stay below the window or it
            // becomes a floor that outranks the fused order.
            evidence = activeFusion.preserveTopKChannel(evidence, plan.getTopK(), "GRAPH",
                graphQuota(plan.getTopK()));
        }
        boolean enforceTokenBudget = budget.getPrimaryConstraint() == BudgetConstraint.TOKENS
            && tokenLedger == null;
        TokenContextTruncator.Result context = new TokenContextTruncator(tokenEstimator)
            .truncate(evidence, plan.getTopK(), budget.getTokenBudget(), enforceTokenBudget);
        trace.setTokensRetained(context.getTokensRetained());
        boolean truncated = evidence.size() > plan.getTopK();
        boolean tokenTruncated = context.getStopReason() == RecallStopReason.TOKEN_BUDGET;
        List<Evidence> ranked = new ArrayList<>();
        for (Evidence value : context.getEvidence()) {
            ranked.add(new Evidence(value.getEvidenceId(), value.getKind(), value.getText(), value.getChunks(),
                value.getEntities(), value.getSources(), value.getStageScores(), value.getFusedScore(),
                value.getFinalScore(), ranked.size() + 1, value.getGraphTraces(),
                value.getSchemaVersion(), value.getDataset(), value.getDatasetVersion(),
                value.getSplit(), value.getGraphVersion(), value.getIndexVersion()));
        }
        boolean deadlineReached = expired(deadline) || trace.getChannelStopReasons().containsValue(RecallStopReason.DEADLINE);
        if (deadlineReached) {
            timedOut(partial);
        }
        RecallStopReason stop = deadlineReached ? RecallStopReason.DEADLINE
            : trace.getChannelStopReasons().containsValue(RecallStopReason.TRACE_VALIDATION_ERROR)
                ? RecallStopReason.TRACE_VALIDATION_ERROR
            : trace.getChannelStopReasons().containsValue(RecallStopReason.EDGE_SCAN_LIMIT)
                ? RecallStopReason.EDGE_SCAN_LIMIT
            : trace.getChannelStopReasons().containsValue(RecallStopReason.CANDIDATE_LIMIT)
                ? RecallStopReason.CANDIDATE_LIMIT
            : tokenTruncated ? RecallStopReason.TOKEN_BUDGET
            : trace.getChannelStopReasons().containsValue(RecallStopReason.CANDIDATE_BUDGET)
                ? RecallStopReason.CANDIDATE_BUDGET
            : truncated ? RecallStopReason.TOP_K : ranked.isEmpty() ? RecallStopReason.NO_RESULT : RecallStopReason.COMPLETED;
        trace.setStopReason(stop.name());
        List<TraceStage> stages = new ArrayList<>();
        int evaluated = 0;
        for (String name : trace.getSelectedChannels()) {
            evaluated += trace.getEvaluatedCounts().get(name);
            stages.add(new TraceStage(name, trace.getChannelStatuses().get(name).name(),
                "reason=" + trace.getChannelStopReasons().get(name) + ";evaluated=" + trace.getEvaluatedCounts().get(name)
                    + ";candidates=" + trace.getCandidateCounts().get(name)
                    + ";budget=" + trace.getChannelBudgets().get(name)
                    + ";elapsedNanos=" + channelElapsedNanos.get(name)));
        }
        stages.add(new TraceStage("FUSION", deadlineReached ? "DEGRADED" : "SUCCESS",
            "weighted_rrf_k=" + trace.getRrfRankConstant() + ";weights=bm25:" + trace.getBm25Weight()
                + ",vector:" + trace.getVectorWeight() + ",graph:" + trace.getGraphWeight()));
        trace.setStages(stages);
        trace.setTotalCandidatesEvaluated(evaluated);
        RetrievalResponse response = new RetrievalResponse();
        response.setEvidence(ranked);
        List<SourceRef> sources = new ArrayList<>();
        for (Evidence value : ranked) {
            for (SourceRef source : value.getSources()) {
                if (!sources.contains(source)) {
                    sources.add(source);
                }
            }
        }
        response.setSources(sources);
        response.setRequestId(requestId);
        trace.setRequestId(requestId);
        trace.setElapsedNanos(clock.getAsLong() - started);
        response.setTrace(trace);
        response.setEffectiveBudget(budget);
        response.setDegradedChannels(trace.getDegradedChannels());
        attachExtractiveAnswer(response, command);
        return response;
    }

    /**
     * Attaches the offline extractive answer to the response.
     *
     * <p>The design requires an extractive answer evaluation, which is only measurable if the
     * retrieval response carries a predicted answer. The answer is a verbatim span of the returned
     * evidence chosen by the deterministic consumer, never generated text, so retrieval stays
     * reproducible and free of model APIs. A result that cannot produce an answer records why.
     */
    private void attachExtractiveAnswer(RetrievalResponse response, RetrievalCommand command) {
        java.util.List<Evidence> ranked = response.getEvidence();
        if (ranked == null || ranked.isEmpty()) {
            response.setAnswer(null);
            response.setAnswerAbstentionReason("NO_EVIDENCE");
            return;
        }
        int answerTokenBudget = command.getBudget().getTokenBudget() == null
            ? DEFAULT_ANSWER_TOKEN_BUDGET : command.getBudget().getTokenBudget();
        AnswerRequest answerRequest = new AnswerRequest(command.getQuery(), answerTokenBudget,
            response.getRequestId(), response.getGraphVersion(), response.getTrace() == null
                ? null : response.getTrace().getIndexVersion(), command.getGraphName(),
            command.getMode().name());
        AnswerResponse answer = answerSynthesizer.synthesize(answerRequest, ranked);
        response.setAnswer(answer.getAnswer());
        response.setAnswerAbstentionReason(answer.getAbstentionReason());
    }

    /**
     * Builds the graph retrieval configuration for one plan.
     *
     * <p>Plan-level stop rules win over global configuration so a policy that lowered the marginal
     * yield threshold for a hard query is not silently overridden by the default.
     */
    private GraphRetrievalConfig graphConfig(int candidateBudget, RetrievalCommand command,
                                             RecallPlan plan) {
        int edgeScanCap = plan.getEdgeScanCap() > 0 ? plan.getEdgeScanCap()
            : resolveEdgeScanBudget(command.getBudget());
        double minMarginalYield = plan.getMinMarginalYield() >= 0.0D ? plan.getMinMarginalYield()
            : graphMinMarginalYield();
        double minAssociationScore = plan.getMinAssociationScore() >= 0.0D
            ? plan.getMinAssociationScore() : graphMinAssociationScore();
        return GraphRetrievalConfig.builder()
            .maxHop(plan.getMaxHop())
            .candidateBudget(candidateBudget)
            .edgeScanBudget(edgeScanCap)
            .neighborCap(Math.min(candidateBudget, plan.getNeighborCap()))
            .maxDiffusionGrowth(plan.getMaxDiffusionGrowth())
            .sampleRate(graphSampleRate())
            .relationPriorities(graphRelationPriorities())
            .minMarginalYield(minMarginalYield)
            .minAssociationScore(minAssociationScore)
            .minAssociationSurvivalRate(graphMinAssociationSurvivalRate())
            .tokenBudget(command.getBudget().getPrimaryConstraint() == BudgetConstraint.TOKENS
                ? command.getBudget().getTokenBudget() : Integer.MAX_VALUE)
            .samplingSeed(DeterministicSeed.graphSeed(command.getQuery(), plan.getGraphVersion(),
                configSeed()))
            .build();
    }

    /** Re-bases an execution plan onto a routing plan the policy returned at a boundary. */
    private static RecallPlan withPlan(RecallPlan base, RetrievalPlan routing) {
        if (routing == null) {
            return base;
        }
        return new RecallPlan(routing.getSelectedMode(), Math.min(base.getTopK(), routing.getTopK()),
            base.getDeadlineNanos(), base.getGraphVersion(), base.getIndexVersion(), base.getVectorVersion(),
            base.getPrimaryConstraint(), base.getExecutionMode(), orderedBudgets(routing),
            routing.getMaxHop(), routing.getNeighborCap(), routing.getMaxDiffusionGrowth(),
            routing.getEdgeScanCap(), routing.getMinMarginalYield(), routing.getMinAssociationScore());
    }

    /**
     * Runs the graph channel one hop at a time and replans between hops.
     *
     * <p>This is the adaptive closed loop the design asks for. Expansion is split because
     * {@code graphGrowthRate} and {@code graphMarginalYield} only exist after a hop has run:
     *
     * <ol>
     *   <li>expand hop 1 with the plan the probe produced,</li>
     *   <li>replan at {@code AFTER_GRAPH_HOP} on the now-measured statistics,</li>
     *   <li>fund hop 2 only if the revised plan still allows it.</li>
     * </ol>
     *
     * <p>When the policy does not replan (or there is no routing plan) this collapses into the
     * original single call, so fixed-hybrid behaviour is unchanged.
     */
    private void runGraphInHops(RecallPlan plan, RetrievalPlan routingPlan, RoutingContext routingContext,
                                RetrievalCommand command, float[] queryVector, long deadline, boolean partial,
                                String requestId, Map<String, RetrievalException> failures,
                                Map<String, TextChunk> byId, Map<String, EvidenceBuilder> merged,
                                RetrievalTrace trace, Map<String, Long> channelElapsedNanos,
                                RequestResources resources, GraphNeighborProvider pinnedGraphProvider,
                                BudgetLedger tokenLedger) {
        Integer graphBudget = plan.getChannelBudgets().get("GRAPH");
        if (graphBudget == null) {
            return;
        }
        if (!adaptiveHopsApplicable(routingPlan, plan)) {
            runChannels(Collections.singletonList(new java.util.AbstractMap.SimpleImmutableEntry<>("GRAPH",
                graphBudget)), ExecutionMode.SEQUENTIAL, command, queryVector, deadline, partial, requestId,
                failures, byId, plan, merged, trace, channelElapsedNanos, resources, pinnedGraphProvider,
                tokenLedger);
            return;
        }
        long hopStarted = clock.getAsLong();
        GraphRetriever retriever = new GraphRetriever(anchorResolver, tokenEstimator);
        try (AutoCloseable stageLease = resources.acquireLease();
             GraphQueryLease graphLease = pinnedGraphProvider == null ? null
                 : pinnedGraphProvider.openLease()) {
            GraphNeighborProvider provider = graphLease == null ? pinnedGraphProvider : graphLease;
            GraphRetrievalConfig hopConfig = graphConfig(graphBudget, command,
                withPlan(plan, routingPlan));
            GraphRetriever.ExpansionSession session = retriever.beginRetrieval(command.getQuery(),
                entities, provider, byId, hopConfig, deadline, clock, tokenLedger,
                candidateChunkIds(merged));
            trace.setGraphGrowthRate(session.getStats().getGrowthRate());
            trace.setGraphMarginalYield(session.getStats().getMarginalYield());
            while (!session.isExhausted() && !expired(deadline)) {
                int hop = session.getCurrentHop();
                if (hop >= routingPlan.getMaxHop()) {
                    break;
                }
                retriever.expandOneHop(session, hopConfig, deadline, clock);
                if (session.isStopped()) {
                    break;
                }
                retrieveGraphStats(trace, session.getStats());
                if (hop + 1 >= routingPlan.getMaxHop()) {
                    break;
                }
                // The hop that just completed is the first real evidence about diffusion cost.
                StageReplan hopReplan = replanAtStageBoundary(routingPlan, routingContext, trace, deadline,
                    plan, tokenLedger, merged, BOUNDARY_AFTER_GRAPH_HOP);
                if (hopReplan.count > 0) {
                    trace.setRoutingFeatures(
                        measuredFeatures(routingContext.getFeatures(), hopReplan.progress));
                }
                RecallPlan replanned = withPlan(plan, hopReplan.plan);
                if (!replanned.getChannelBudgets().containsKey("GRAPH")
                    || replanned.getMaxHop() <= hop + 1) {
                    break;
                }
                hopConfig = graphConfig(graphBudget, command, replanned);
            }
            List<GraphRetriever.GraphHit> hits = session.finish();
            ChannelOutput output = new ChannelOutput();
            output.graphHits = hits;
            output.graphStats = session.getStats();
            output.evaluated = session.getStats().getCandidatesProduced();
            output.rawCandidatesScored = session.getStats().getCandidatesProduced();
            output.candidates = session.getStats().getCandidatesProduced();
            output.reason = session.getStats().getStopReason();
            output.elapsedNanos = Math.max(0L, clock.getAsLong() - hopStarted);
            channelElapsedNanos.put("GRAPH", output.elapsedNanos);
            trace.getStageElapsedNanos().put("GRAPH", output.elapsedNanos);
            retrieveGraphStats(trace, session.getStats());
            if (tokenLedger != null) {
                chargeEvaluatedCandidates(tokenLedger, output.candidates);
            }
            trace.getEvaluatedCounts().put("GRAPH", output.evaluated);
            trace.getRawCandidatesScored().put("GRAPH", output.rawCandidatesScored);
            trace.getCandidateCounts().put("GRAPH", output.candidates);
            mergeChannel("GRAPH", output, requestId, command.getQuery(), byId, plan, merged, tokenLedger);
            if (output.reason == RecallStopReason.DEADLINE) {
                timedOut(partial);
            }
            record(trace, "GRAPH", output.reason == RecallStopReason.TRACE_VALIDATION_ERROR
                ? RecallStageStatus.DEGRADED : RecallStageStatus.SUCCESS, output.reason, null);
        } catch (RetrievalException error) {
            throw error;
        } catch (Exception error) {
            throw notReady("retrieval stage failed: " + error.getMessage());
        }
    }

    /**
     * True when the graph stage can be split into individually funded hops.
     *
     * <p>Splitting only pays off when a policy can react to the first hop. Without a routing plan the
     * single call is the correct and simpler path, and a plan limited to one hop has nothing to
     * decide after hop one.
     */
    private static boolean adaptiveHopsApplicable(RetrievalPlan routingPlan, RecallPlan plan) {
        return routingPlan != null && plan.getMaxHop() > 1 && routingPlan.getMaxHop() > 1;
    }

    /** Copies the graph work counters into the trace. */
    private static void retrieveGraphStats(RetrievalTrace trace, GraphRetriever.GraphStats stats) {
        trace.setAnchorsConsidered(stats.getAnchorsConsidered());
        trace.setEdgesExamined(stats.getEdgesExamined());
        trace.setNeighborsSampled(stats.getNeighborsSampled());
        trace.setVerticesReached(stats.getVerticesReached());
        trace.setUniqueVertices(stats.getUniqueVertices());
        trace.setGraphCandidatesProduced(stats.getCandidatesProduced());
        trace.setGraphTokensEstimated(stats.getTokensEstimated());
        trace.setGraphEvidenceYield(stats.getEvidenceYield());
        trace.setGraphGrowthRate(stats.getGrowthRate());
        trace.setGraphMarginalYield(stats.getMarginalYield());
    }

    private void runChannels(List<Map.Entry<String, Integer>> channels, ExecutionMode executionMode,
                             RetrievalCommand command, float[] queryVector, long deadline, boolean partial,
                             String requestId, Map<String, RetrievalException> failures,
                             Map<String, TextChunk> byId, RecallPlan plan,
                             Map<String, EvidenceBuilder> merged, RetrievalTrace trace,
                             Map<String, Long> channelElapsedNanos,
                             RequestResources resources,
                             GraphNeighborProvider pinnedGraphProvider, BudgetLedger tokenLedger) {
        AtomicBoolean anyStageStarted = new AtomicBoolean();
        boolean deadlineAlreadyRecorded = false;
        List<Stage<ChannelOutput>> stages = new ArrayList<>();
        for (Map.Entry<String, Integer> channel : channels) {
            String name = channel.getKey();
            stages.add(new Stage<>(name, () -> {
                try (AutoCloseable stageLease = resources.acquireLease();
                     GraphQueryLease graphLease = "GRAPH".equals(name) && pinnedGraphProvider != null
                         ? pinnedGraphProvider.openLease() : null) {
                    if (expired(deadline)) {
                        ChannelOutput stopped = new ChannelOutput();
                        stopped.reason = RecallStopReason.DEADLINE;
                        stopped.notRun = !anyStageStarted.compareAndSet(false, true);
                        return stopped;
                    }
                    anyStageStarted.set(true);
                    long started = clock.getAsLong();
                    GraphNeighborProvider stageGraphProvider = graphLease == null
                        ? pinnedGraphProvider : graphLease;
                    ChannelOutput output = searchChannel(name, channel.getValue(), command,
                        queryVector, deadline, requestId, failures, byId, plan, stageGraphProvider,
                        tokenLedger, candidateChunkIds(merged));
                    output.elapsedNanos = Math.max(0L, clock.getAsLong() - started);
                    return output;
                }
            }));
        }
        ExecutorService executor = executionMode == ExecutionMode.PARALLEL
            ? Executors.newFixedThreadPool(Math.max(1, stages.size())) : null;
        Map<String, StageResult<ChannelOutput>> results;
        try {
            StageExecutor stageExecutor = new StageExecutor();
            if (executionMode == ExecutionMode.PARALLEL) {
                results = stageExecutor.execute(stages, executionMode, executor, deadline, clock);
            } else {
                results = new LinkedHashMap<>();
                long stageDeadline = executionMode == ExecutionMode.SEQUENTIAL ? 0L : deadline;
                for (Stage<ChannelOutput> stage : stages) {
                    results.putAll(stageExecutor.execute(Collections.singletonList(stage),
                        ExecutionMode.SEQUENTIAL, null, stageDeadline, clock));
                }
            }
        } finally {
            if (executor != null) {
                executor.shutdownNow();
            }
        }
        for (Map.Entry<String, Integer> channel : channels) {
            String name = channel.getKey();
            StageResult<ChannelOutput> stage = results.get(name);
            long elapsed = stage.isSuccess() ? stage.getValue().elapsedNanos : stage.getElapsedNanos();
            channelElapsedNanos.put(name, elapsed);
            trace.getStageElapsedNanos().put(name, elapsed);
            if (!stage.isSuccess()) {
                Throwable cause = stage.getError();
                if (cause instanceof TimeoutException) {
                    timedOut(partial);
                    RecallStageStatus status = deadlineAlreadyRecorded
                        ? RecallStageStatus.NOT_RUN : RecallStageStatus.DEGRADED;
                    record(trace, name, status, RecallStopReason.DEADLINE, cause.getMessage());
                    deadlineAlreadyRecorded = true;
                    continue;
                }
                RetrievalException error = cause instanceof RetrievalException
                    ? (RetrievalException) cause : notReady("retrieval stage failed: " + cause.getMessage());
                if (!partial || error.getCode() != RetrievalErrorCode.INDEX_NOT_READY) {
                    throw error;
                }
                record(trace, name, RecallStageStatus.DEGRADED, RecallStopReason.INDEX_NOT_READY,
                    error.getMessage());
                continue;
            }
            ChannelOutput output = stage.getValue();
            if (tokenLedger != null) {
                chargeEvaluatedCandidates(tokenLedger, output.candidates);
            }
            trace.getEvaluatedCounts().put(name, output.evaluated);
            trace.getRawCandidatesScored().put(name, output.rawCandidatesScored);
            trace.getCandidateCounts().put(name, output.candidates);
            if (output.graphStats != null) {
                GraphRetriever.GraphStats stats = output.graphStats;
                trace.setAnchorsConsidered(stats.getAnchorsConsidered());
                trace.setEdgesExamined(stats.getEdgesExamined());
                trace.setNeighborsSampled(stats.getNeighborsSampled());
                trace.setVerticesReached(stats.getVerticesReached());
                trace.setUniqueVertices(stats.getUniqueVertices());
                trace.setGraphCandidatesProduced(stats.getCandidatesProduced());
                trace.setGraphTokensEstimated(stats.getTokensEstimated());
                trace.setGraphEvidenceYield(stats.getEvidenceYield());
                trace.setGraphGrowthRate(stats.getGrowthRate());
                trace.setGraphMarginalYield(stats.getMarginalYield());
                trace.getValidationErrors().addAll(stats.getValidationErrors());
            }
            mergeChannel(name, output, requestId, command.getQuery(), byId, plan, merged, tokenLedger);
            if (tokenLedger != null && tokenLedger.getStopReason() == RecallStopReason.TOKEN_BUDGET
                && output.reason == RecallStopReason.COMPLETED) {
                output.reason = RecallStopReason.TOKEN_BUDGET;
            }
            if (output.reason == RecallStopReason.DEADLINE) {
                timedOut(partial);
            }
            RecallStageStatus status = output.notRun ? RecallStageStatus.NOT_RUN
                : output.reason == RecallStopReason.DEADLINE
                    ? deadlineAlreadyRecorded ? RecallStageStatus.NOT_RUN : RecallStageStatus.DEGRADED
                    : output.reason == RecallStopReason.TRACE_VALIDATION_ERROR
                        ? RecallStageStatus.DEGRADED : RecallStageStatus.SUCCESS;
            if (output.reason == RecallStopReason.DEADLINE) {
                deadlineAlreadyRecorded = true;
            }
            record(trace, name, status, output.reason, null);
        }
    }

    /**
     * Charges a channel's candidate output against the ledger.
     *
     * <p>The budget bounds how many candidates a request may retain, not how many documents a
     * channel had to score to find them. Charging the scoring count conflated the two: a text
     * channel that ranked its full index still returns at most its candidate share, so charging
     * the scan made one channel consume the whole request's budget and starve the graph stage.
     * Scoring work stays visible in {@code rawCandidatesScored}.
     */
    private static void chargeEvaluatedCandidates(BudgetLedger ledger, int evaluated) {
        ledger.tryEvaluateCandidates(evaluated);
    }

    private ChannelOutput searchChannel(String name, int candidateBudget, RetrievalCommand command,
                                        float[] queryVector, long deadline, String requestId,
                                        Map<String, RetrievalException> failures,
                                        Map<String, TextChunk> byId, RecallPlan plan,
                                        GraphNeighborProvider pinnedGraphProvider,
                                        BudgetLedger tokenLedger, List<String> candidateChunkIds) {
        RetrievalException loadFailure = failures.get(name);
        if (loadFailure != null) {
            throw loadFailure;
        }
        ChannelOutput output = new ChannelOutput();
        if ("BM25".equals(name)) {
            if (bm25 == null) {
                throw notReady("artifact unavailable");
            }
            ChannelSearchResult<Bm25Hit> search = command.isExactRanking()
                ? bm25.searchExactWithStats(command.getQuery(), candidateBudget, plan.getTopK(), deadline, clock)
                : bm25.searchWithStats(command.getQuery(), candidateBudget, plan.getTopK(), deadline, clock);
            output.bm25Hits = search.getHits();
            output.evaluated = search.getCandidatesEvaluated();
            output.rawCandidatesScored = search.getCandidatesEvaluated();
            output.candidates = search.getHits().size();
            output.reason = search.getStopReason();
        } else if ("VECTOR".equals(name)) {
            if (vector == null) {
                throw notReady("artifact unavailable");
            }
            ChannelSearchResult<VectorHit> search = command.isExactRanking()
                ? vector.searchExactWithStats(queryVector, candidateBudget, plan.getTopK(), deadline, clock)
                : vector.searchWithStats(queryVector, candidateBudget, plan.getTopK(), deadline, clock);
            output.vectorHits = search.getHits();
            output.evaluated = search.getCandidatesEvaluated();
            output.rawCandidatesScored = search.getCandidatesEvaluated();
            output.candidates = search.getHits().size();
            output.reason = search.getStopReason();
        } else {
            GraphRetriever retriever = new GraphRetriever(anchorResolver, tokenEstimator);
            GraphRetrievalConfig graphConfig = graphConfig(candidateBudget, command, plan);
            output.graphHits = retriever.retrieve(command.getQuery(), entities, pinnedGraphProvider,
                byId, graphConfig, deadline, clock, tokenLedger, candidateChunkIds);
            output.graphStats = retriever.getLastStats();
            output.evaluated = output.graphStats.getCandidatesProduced();
            output.rawCandidatesScored = output.graphStats.getCandidatesProduced();
            output.candidates = output.graphStats.getCandidatesProduced();
            output.reason = output.graphStats.getStopReason();
        }
        return output;
    }

    private void mergeChannel(String name, ChannelOutput output, String requestId, String query,
                              Map<String, TextChunk> byId, RecallPlan plan,
                              Map<String, EvidenceBuilder> merged, BudgetLedger tokenLedger) {
        if (output.bm25Hits != null) {
            for (Bm25Hit hit : output.bm25Hits) {
                TextChunk chunk = byId.get(hit.getChunkId());
                if (!retainTokenEvidence(tokenLedger, hit.getChunkId(), chunk)) {
                    continue;
                }
                EvidenceBuilder value = builder(merged, byId, hit.getChunkId(), hit.getDocumentId(),
                    EvidenceKind.CHUNK, plan);
                if (!value.chunk.getText().equals(hit.getText())) {
                    throw notReady("BM25 text does not match fixture");
                }
                value.scores.put(name, new ChannelScore(name, hit.getScore(), null, hit.getRank()));
            }
        }
        if (output.vectorHits != null) {
            for (VectorHit hit : output.vectorHits) {
                TextChunk chunk = byId.get(hit.getChunkId());
                if (!retainTokenEvidence(tokenLedger, hit.getChunkId(), chunk)) {
                    continue;
                }
                EvidenceBuilder value = builder(merged, byId, hit.getChunkId(), hit.getDocumentId(),
                    EvidenceKind.CHUNK, plan);
                value.scores.put(name, new ChannelScore(name, hit.getSimilarity(), null, hit.getRank()));
            }
        }
        if (output.graphHits != null) {
            int rank = 0;
            for (GraphRetriever.GraphHit hit : output.graphHits) {
                for (String id : hit.getSourceChunkIds()) {
                    if (!retainTokenEvidence(tokenLedger, id, byId.get(id))) {
                        continue;
                    }
                    EvidenceBuilder value = builder(merged, byId, id, byId.get(id).getDocumentId(),
                        EvidenceKind.GRAPH_PATH, plan);
                    if (!value.entities.contains(hit.getAnchor().getEntity())) {
                        value.entities.add(hit.getAnchor().getEntity());
                    }
                    value.graphTraces.add(new GraphEvidenceTrace(
                        RetrievalIds.queryId(query, plan.getGraphVersion()),
                        plan.getMode(), hit.getAnchor().getMatchType(), hit.getAnchor().getConfidence(),
                        plan.getGraphVersion() == null ? "fixture" : plan.getGraphVersion(), hit.getPath(),
                        Collections.singletonList(id)));
                    if (!value.scores.containsKey(name)) {
                        value.scores.put(name, new ChannelScore(name, hit.getScore(), null, ++rank));
                    }
                }
            }
        }
    }

    private boolean retainTokenEvidence(BudgetLedger tokenLedger, String chunkId, TextChunk chunk) {
        if (tokenLedger == null) {
            return true;
        }
        if (chunk == null) {
            throw notReady("channel chunk does not match fixture");
        }
        return tokenLedger.tryRetain(chunkId, chunk, tokenEstimator);
    }

    private int cascadeMinBudget() {
        return properties == null ? 5 : properties.getCascadeMinBudget();
    }

    /**
     * Resolves how many returned positions graph evidence may be given by quota.
     *
     * <p>At least one position so the channel is never dropped entirely, and at most
     * {@code topK - 1} so the quota can never occupy the entire window.
     */
    private int graphQuota(int topK) {
        double share = properties == null ? 0.34D : properties.getGraphQuotaShare();
        int quota = (int) Math.round(topK * share);
        return Math.max(1, Math.min(topK - 1, quota));
    }

    private int cascadeProbeMax() {
        return properties == null ? 10 : properties.getCascadeProbeMax();
    }

    private int cascadeProbeMin() {
        return properties == null ? 2 : properties.getCascadeProbeMin();
    }

    private int cascadeProbeDivisor() {
        return properties == null ? 8 : properties.getCascadeProbeDivisor();
    }

    private double cascadeMinProbeScoreGap() {
        return properties == null ? CASCADE_MIN_PROBE_SCORE_GAP : properties.getCascadeMinProbeScoreGap();
    }

    private int cascadeMinProbeEvidence() {
        return properties == null ? 2 : properties.getCascadeMinProbeEvidence();
    }

    private double cascadeProbeScoreGap(Map<String, EvidenceBuilder> candidates) {
        if (candidates.size() < 2) {
            return 0.0D;
        }
        List<Double> scores = new ArrayList<>();
        for (EvidenceBuilder candidate : candidates.values()) {
            scores.add(fusion.score(candidate.build()));
        }
        scores.sort(Comparator.reverseOrder());
        return scores.get(0) - scores.get(1);
    }

    private static final class ChannelOutput {
        private List<Bm25Hit> bm25Hits;
        private List<VectorHit> vectorHits;
        private List<GraphRetriever.GraphHit> graphHits;
        private GraphRetriever.GraphStats graphStats;
        private GraphRetriever.ExpansionSession graphSession;
        private int evaluated;
        private int rawCandidatesScored;
        private int candidates;
        private long elapsedNanos;
        private RecallStopReason reason;
        private boolean notRun;
    }

    private boolean expired(long deadline) {
        return deadline != 0L && clock.getAsLong() - deadline >= 0L;
    }

    /**
     * Applies bounded replanning at a stage boundary.
     *
     * <p>Both execution paths share this so a cascaded adaptive run replans on the statistics its
     * probe just measured, instead of the policy being consulted only once before execution.
     *
     * @return the final plan, the last observed progress, and how many replans were applied
     */
    private StageReplan replanAtStageBoundary(RetrievalPlan initialPlan, RoutingContext routingContext,
                                              RetrievalTrace trace, long deadline, RecallPlan plan,
                                              BudgetLedger tokenLedger,
                                              Map<String, EvidenceBuilder> merged, String boundary) {
        RetrievalProgress progress = routingProgress(trace, routingContext, deadline, plan, tokenLedger,
            merged, trace.getReplanEvents().size(), boundary);
        RetrievalPlan replanned = initialPlan;
        int replanCount = 0;
        while (replanCount < maxReplanCount && !expired(deadline)) {
            RetrievalPlan previous = replanned;
            RetrievalPlanValidator.Result candidate = RetrievalPlanValidator.validateAndAdjust(
                routingPolicy.replan(routingContext, progress), routingContext);
            RetrievalPlan next = candidate.getPlan();
            if (BOUNDARY_AFTER_GRAPH_HOP.equals(boundary)) {
                next = constrainPostHopPlan(previous, next);
            }
            if (!isDefaultFusionConfig()) {
                next = RetrievalPlan.builder(next)
                    .bm25Weight(fusion.getConfig().getBm25Weight())
                    .vectorWeight(fusion.getConfig().getVectorWeight())
                    .graphWeight(fusion.getConfig().getGraphWeight()).build();
            }
            if (sameRoutingDecision(previous, next)) {
                break;
            }
            List<String> adjustments = new ArrayList<>();
            for (PlanAdjustment adjustment : candidate.getAdjustments()) {
                adjustments.add(adjustment.toString());
            }
            trace.getReplanEvents().add(new RetrievalReplanEvent(boundary, "GRAPH_STAGE_BOUNDARY",
                previous, next, progress, unusedCandidates(previous, trace),
                unusedTokens(previous, trace, tokenLedger), adjustments, null));
            replanned = next;
            replanCount++;
            progress = routingProgress(trace, routingContext, deadline, plan, tokenLedger, merged,
                replanCount, boundary);
        }
        return new StageReplan(replanned, progress, replanCount);
    }

    /**
     * Restricts a post-hop replan to decisions that can still change execution.
     *
     * <p>By the time a hop has run the text channels are complete, so re-slicing their candidate
     * budget cannot produce any work - it only rewrites the trace and hands the graph stage an
     * allocation that nothing consumes. With the default single replan the policy used to spend
     * the hop boundary restoring the pre-probe split, which is why replanning fired on almost every
     * request while leaving the result bit-identical. Keeping the previous allocation turns that
     * round trip into a no-op the loop can stop on.
     *
     * <p>The two decisions that remain meaningful are kept: dropping the graph channel once growth
     * or yield says expansion is not paying, and lowering the hop ceiling. Raising the ceiling is
     * rejected because hops already executed cannot be unspent, so the ceiling is monotone.
     */
    static RetrievalPlan constrainPostHopPlan(RetrievalPlan previous, RetrievalPlan next) {
        if (previous == null || next == null) {
            return next;
        }
        RetrievalPlan.Builder builder = null;
        if (next.getMaxHop() > previous.getMaxHop()) {
            builder = RetrievalPlan.builder(next).maxHop(previous.getMaxHop());
        }
        // A plan that drops the graph channel is the stop decision itself and is left untouched.
        if (previous.getChannelBudgets().containsKey("GRAPH")
            && next.getChannelBudgets().containsKey("GRAPH")) {
            if (!previous.getChannelBudgets().equals(next.getChannelBudgets())) {
                builder = (builder == null ? RetrievalPlan.builder(next) : builder)
                    .channelBudgets(previous.getChannelBudgets());
            }
            if (!previous.getChannelOrder().equals(next.getChannelOrder())) {
                builder = (builder == null ? RetrievalPlan.builder(next) : builder)
                    .channelOrder(previous.getChannelOrder());
            }
        }
        return builder == null ? next : builder.build();
    }

    /** Result of one stage-boundary replanning pass. */
    private static final class StageReplan {
        private final RetrievalPlan plan;
        private final RetrievalProgress progress;
        private final int count;

        private StageReplan(RetrievalPlan plan, RetrievalProgress progress, int count) {
            this.plan = plan;
            this.progress = progress;
            this.count = count;
        }
    }

    /**
     * Base seed for reproducible sampling and routing draws.
     *
     * <p>Defaults to {@code 0} so that output is stable across processes. Operators may override
     * it through configuration to draw a different but still reproducible sample.
     */
    private static long configSeed() {
        return DEFAULT_CONFIG_SEED;
    }

    /**
     * Resolves the graph edge-scan bound independently from the candidate bound.
     *
     * <p>Edge scanning is the dominant implicit cost of diffusion, so it keeps its own bound. A
     * request that only sets {@code maxCandidates} still gets the configured default scan bound
     * rather than silently inheriting the candidate budget.
     */
    private double graphSampleRate() {
        return properties == null ? 1.0D : properties.getGraphSampleRate();
    }

    private Map<String, Double> graphRelationPriorities() {
        return properties == null ? Collections.emptyMap() : properties.getGraphRelationPriorities();
    }

    private double graphMinMarginalYield() {
        return properties == null ? 0.0D : properties.getGraphMinMarginalYield();
    }

    private double graphMinAssociationScore() {
        return properties == null ? DEFAULT_MIN_ASSOCIATION_SCORE
            : properties.getGraphMinAssociationScore();
    }

    private double graphMinAssociationSurvivalRate() {
        return properties == null ? DEFAULT_MIN_ASSOCIATION_SURVIVAL_RATE
            : properties.getGraphMinAssociationSurvivalRate();
    }

    private int resolveEdgeScanBudget(RetrievalBudget budget) {
        if (budget.getMaxEdgeScans() != null && budget.getMaxEdgeScans() > 0) {
            return budget.getMaxEdgeScans();
        }
        return properties == null ? DEFAULT_MAX_EDGE_SCANS : properties.getDefaultMaxEdgeScans();
    }

    private static void timedOut(boolean partial) {
        if (!partial) {
            throw new RetrievalException(RetrievalErrorCode.RETRIEVAL_TIMEOUT, "retrieval deadline exceeded");
        }
    }

    private static void record(RetrievalTrace trace, String name, RecallStageStatus status, RecallStopReason reason, String message) {
        trace.getChannelStatuses().put(name, status);
        trace.getChannelStopReasons().put(name, reason);
        if (status != RecallStageStatus.SUCCESS) {
            trace.getDegradedChannels().add(name);
            trace.getDegradationReasons().put(name, message == null ? reason.name() : message);
        }
    }

    private static RetrievalException notReady(String message) {
        return new RetrievalException(RetrievalErrorCode.INDEX_NOT_READY, message);
    }


    private static EvidenceBuilder builder(Map<String, EvidenceBuilder> merged,
                                            Map<String, TextChunk> chunks, String chunkId,
                                            String documentId, EvidenceKind kind, RecallPlan plan) {
        TextChunk chunk = chunks.get(chunkId);
        if (chunk == null || !chunk.getDocumentId().equals(documentId)) {
            throw notReady("channel chunk does not match fixture");
        }
        String id = RetrievalIds.chunkEvidenceId(documentId, chunkId);
        return merged.computeIfAbsent(id, key -> new EvidenceBuilder(id, chunk, kind,
            plan.getGraphVersion(), plan.getIndexVersion()));
    }

    private static final class EvidenceBuilder {
        private final String id;
        private final TextChunk chunk;
        private final EvidenceKind kind;
        private final String graphVersion;
        private final String indexVersion;
        private final Map<String, ChannelScore> scores = new LinkedHashMap<>();
        private final List<EntityRef> entities = new ArrayList<>();
        private final List<GraphEvidenceTrace> graphTraces = new ArrayList<>();

        private EvidenceBuilder(String id, TextChunk chunk, EvidenceKind kind, String graphVersion,
                                String indexVersion) {
            this.id = id;
            this.chunk = chunk;
            this.kind = kind;
            this.graphVersion = graphVersion;
            this.indexVersion = indexVersion;
        }

        private Evidence build() {
            double fused = 0.0;
            for (ChannelScore score : scores.values()) {
                fused += 1.0 / (60.0 + score.getRank());
            }
            if (chunk.getSourceUri() == null) {
                throw notReady("chunk source URI is missing");
            }
            SourceRef source = chunk.getDataset() == null
                ? new SourceRef(chunk.getDocumentId(), chunk.getSourceUri(), chunk.getStartOffset(),
                    chunk.getEndOffset())
                : new SourceRef(chunk.getDocumentId(), chunk.getSourceUri(), chunk.getStartOffset(),
                    chunk.getEndOffset(), chunk.getDataset(), chunk.getDatasetVersion(),
                    chunk.getSplit(), chunk.getSourceHash());
            String evidenceGraphVersion = graphTraces.isEmpty() ? graphVersion
                : graphTraces.get(0).getGraphVersion();
            return new Evidence(id, kind, chunk.getText(), Collections.singletonList(chunk), entities,
                Collections.singletonList(source), scores, fused, null, 1, graphTraces,
                Evidence.CURRENT_SCHEMA_VERSION,
                chunk.getDataset(), chunk.getDatasetVersion(),
                chunk.getSplit(), evidenceGraphVersion, indexVersion);
        }
    }
}
