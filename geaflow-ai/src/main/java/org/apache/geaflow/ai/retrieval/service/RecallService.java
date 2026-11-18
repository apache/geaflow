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
package org.apache.geaflow.ai.retrieval.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.LongSupplier;
import java.util.regex.Pattern;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalBudget;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalCommand;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalErrorCode;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalException;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalRequest;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalResponse;
import org.apache.geaflow.ai.retrieval.channel.graph.EntityAnchorResolver;
import org.apache.geaflow.ai.retrieval.channel.graph.GraphNeighborProvider;
import org.apache.geaflow.ai.retrieval.channel.graph.InMemoryGraphNeighborProvider;
import org.apache.geaflow.ai.retrieval.config.RetrievalProperties;
import org.apache.geaflow.ai.retrieval.execution.AdaptivePolicy;
import org.apache.geaflow.ai.retrieval.execution.BudgetConstraint;
import org.apache.geaflow.ai.retrieval.execution.DeterministicReranker;
import org.apache.geaflow.ai.retrieval.execution.DeterministicRulePolicy;
import org.apache.geaflow.ai.retrieval.execution.DeterministicTokenEstimator;
import org.apache.geaflow.ai.retrieval.execution.LexicalStatistics;
import org.apache.geaflow.ai.retrieval.execution.Reranker;
import org.apache.geaflow.ai.retrieval.execution.RetrievalExecutor;
import org.apache.geaflow.ai.retrieval.execution.RetrievalStrategyFactory;
import org.apache.geaflow.ai.retrieval.execution.TokenEstimator;
import org.apache.geaflow.ai.retrieval.fusion.CandidateFusion;
import org.apache.geaflow.ai.retrieval.fusion.WeightedRrfConfig;
import org.apache.geaflow.ai.retrieval.fusion.WeightedRrfFusion;
import org.apache.geaflow.ai.retrieval.index.bm25.Bm25IndexReader;
import org.apache.geaflow.ai.retrieval.index.vector.VectorIndexReader;
import org.apache.geaflow.ai.retrieval.model.document.TextChunk;
import org.apache.geaflow.ai.retrieval.model.evidence.Evidence;
import org.apache.geaflow.ai.retrieval.model.graph.EntityRef;
import org.apache.geaflow.ai.retrieval.model.graph.GraphEdgeRef;

/** Session-free sequential recall; all execution state belongs to one request. */
public final class RecallService {
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
    private static final Pattern QUERY_TOKEN = Pattern.compile("[\\p{L}\\p{N}]+", Pattern.UNICODE_CHARACTER_CLASS);

    private final Bm25IndexReader bm25;
    private final VectorIndexReader vector;
    private final List<TextChunk> chunks;
    private final Map<String, TextChunk> chunksById;
    private final List<EntityRef> entities;
    private final EntityAnchorResolver anchorResolver;
    private final Map<String, Double> idfByToken;
    private final List<GraphEdgeRef> edges;
    private final GraphNeighborProvider graphProvider;
    private final RetrievalChannelRegistry registry;
    private final CandidateFusion fusion;
    private final LongSupplier clock;
    private final TokenEstimator tokenEstimator;
    private final Reranker reranker;
    private final AdaptivePolicy routingPolicy;
    private final int maxReplanCount;
    private final boolean strictPlanValidation;
    private final RetrievalProperties properties;
    private final RetrievalExecutor executor;

    /**
     * Builds an in-memory facade over channel readers and graph records without a registry.
     *
     * <p>Intended for tests and embedding, not for serving: it bypasses the registry's version
     * checks, readiness reporting and lease tracking. Production code always goes through
     * {@link #fromProperties}, which builds on a {@link RetrievalChannelRegistry}.
     *
     * @param bm25 BM25 reader, or null to omit the channel
     * @param vector vector reader, or null to omit the channel
     * @param chunks chunks the callers may query
     * @param entities entities backing anchor resolution
     * @param edges edges backing bounded expansion
     */
    public RecallService(Bm25IndexReader bm25, VectorIndexReader vector, List<TextChunk> chunks,
                         List<EntityRef> entities, List<GraphEdgeRef> edges) {
        this(bm25, vector, chunks, entities, edges, null, new InMemoryGraphNeighborProvider(edges),
            WeightedRrfConfig.DEFAULT, System::nanoTime, new DeterministicTokenEstimator(),
            new DeterministicReranker(), new DeterministicRulePolicy(), 1, true, null, null);
    }

    public RecallService(RetrievalChannelRegistry registry) {
        this(registry, WeightedRrfConfig.DEFAULT);
    }

    public RecallService(RetrievalChannelRegistry registry, WeightedRrfConfig config) {
        this(registry, config, System::nanoTime);
    }

    /** Accepts a monotonic clock for reproducible deadline verification. */
    public RecallService(RetrievalChannelRegistry registry, WeightedRrfConfig config, LongSupplier clock) {
        this(registry, config, clock, new DeterministicTokenEstimator(), new DeterministicReranker());
    }

    public RecallService(RetrievalChannelRegistry registry, WeightedRrfConfig config, LongSupplier clock,
                         TokenEstimator tokenEstimator, Reranker reranker) {
        this(registry, config, clock, tokenEstimator, reranker, new DeterministicRulePolicy(), 1);
    }

    /** Allows a routing policy and replan bound to be injected for replay and policy evaluation. */
    public RecallService(RetrievalChannelRegistry registry, WeightedRrfConfig config, LongSupplier clock,
                         TokenEstimator tokenEstimator, Reranker reranker,
                         AdaptivePolicy routingPolicy, int maxReplanCount) {
        this(registry, config, clock, tokenEstimator, reranker, routingPolicy, maxReplanCount, true);
    }

    /**
     * Builds a service from configuration defaults, keeping request validation on the same values.
     *
     * <p>The supplied fusion config is re-based onto the configured rank constant so the score gap
     * the routing thresholds compare against and the fusion that produces it stay in the same
     * units. The rank constant sets the scale of every fused score, {@code w / (k + rank)}, and was
     * a constant before; a threshold tuned against one scale silently misfires against another.
     */
    public static RecallService fromProperties(RetrievalChannelRegistry registry, WeightedRrfConfig config,
                                               LongSupplier clock, RetrievalProperties properties) {
        Objects.requireNonNull(properties, "properties");
        properties.validateConfiguration();
        WeightedRrfConfig effectiveConfig = config.getRankConstant() == properties.getRrfRankConstant()
            ? config
            : new WeightedRrfConfig(config.getBm25Weight(), config.getVectorWeight(),
                config.getGraphWeight(), properties.getRrfRankConstant());
        AdaptivePolicy policy = RetrievalStrategyFactory.adaptivePolicy(
            properties.getRoutingPolicyId(), properties);
        RecallService service = new RecallService(registry, effectiveConfig, clock,
            RetrievalStrategyFactory.tokenEstimator(properties.getTokenEstimatorId()),
            RetrievalStrategyFactory.reranker(properties.getRerankerId(),
                properties.getRerankerGraphPathWeight()), policy,
            properties.getMaxReplanCount(), properties.isStrictPlanValidation(), properties);
        return service;
    }

    public RecallService(RetrievalChannelRegistry registry, WeightedRrfConfig config, LongSupplier clock,
                         TokenEstimator tokenEstimator, Reranker reranker,
                         AdaptivePolicy routingPolicy, int maxReplanCount, boolean strictPlanValidation) {
        this(registry, config, clock, tokenEstimator, reranker, routingPolicy, maxReplanCount,
            strictPlanValidation, null);
    }

    public RecallService(RetrievalChannelRegistry registry, WeightedRrfConfig config, LongSupplier clock,
                         TokenEstimator tokenEstimator, Reranker reranker,
                         AdaptivePolicy routingPolicy, int maxReplanCount, boolean strictPlanValidation,
                         RetrievalProperties properties) {
        this(null, null, Collections.emptyList(), Collections.emptyList(), Collections.emptyList(),
            Objects.requireNonNull(registry, "registry"), null, config, clock, tokenEstimator, reranker,
            routingPolicy, maxReplanCount, strictPlanValidation, properties, null);
    }

    /**
     * Builds a service around a pluggable {@link CandidateFusion} strategy. The weighted RRF fusion stays
     * the default; this overload lets a caller supply another implementation without changing the
     * execution modes, budgets or trace shape.
     */
    public RecallService(RetrievalChannelRegistry registry, CandidateFusion fusion, LongSupplier clock,
                         TokenEstimator tokenEstimator, Reranker reranker,
                         AdaptivePolicy routingPolicy, int maxReplanCount,
                         RetrievalProperties properties) {
        this(null, null, Collections.emptyList(), Collections.emptyList(), Collections.emptyList(),
            Objects.requireNonNull(registry, "registry"), null,
            Objects.requireNonNull(fusion, "fusion").getConfig(), clock, tokenEstimator, reranker,
            routingPolicy, maxReplanCount, true, properties, Objects.requireNonNull(fusion, "fusion"));
    }

    private RecallService(Bm25IndexReader bm25, VectorIndexReader vector, List<TextChunk> chunks,
                          List<EntityRef> entities, List<GraphEdgeRef> edges, RetrievalChannelRegistry registry,
                          GraphNeighborProvider graphProvider, WeightedRrfConfig config, LongSupplier clock,
                          TokenEstimator tokenEstimator, Reranker reranker, AdaptivePolicy routingPolicy,
                          int maxReplanCount, boolean strictPlanValidation, RetrievalProperties properties,
                          CandidateFusion fusionOverride) {
        if (maxReplanCount < 0) {
            throw new IllegalArgumentException("maxReplanCount must be non-negative");
        }
        this.strictPlanValidation = strictPlanValidation;
        this.properties = properties == null ? new RetrievalProperties() : properties;
        this.bm25 = bm25;
        this.vector = vector;
        this.chunks = immutable(chunks);
        this.chunksById = indexChunks(this.chunks);
        this.entities = immutable(entities);
        this.anchorResolver = new EntityAnchorResolver(this.entities);
        this.idfByToken = LexicalStatistics.buildIdfIndex(this.chunks);
        this.edges = immutable(edges);
        this.graphProvider = graphProvider;
        this.registry = registry;
        this.fusion = fusionOverride == null ? new WeightedRrfFusion(config) : fusionOverride;
        this.clock = Objects.requireNonNull(clock, "clock");
        this.tokenEstimator = Objects.requireNonNull(tokenEstimator, "tokenEstimator");
        this.reranker = Objects.requireNonNull(reranker, "reranker");
        this.routingPolicy = Objects.requireNonNull(routingPolicy, "routingPolicy");
        this.maxReplanCount = maxReplanCount;
        this.executor = new RetrievalExecutor(this.bm25, this.vector, this.chunks, this.chunksById,
            this.entities, this.anchorResolver, this.idfByToken, this.edges, this.graphProvider,
            this.fusion, this.clock, this.tokenEstimator, this.reranker, this.routingPolicy,
            this.maxReplanCount, this.strictPlanValidation, this.properties);
    }

    private static <T> List<T> immutable(List<T> values) {
        return values == null ? Collections.emptyList() : Collections.unmodifiableList(new ArrayList<>(values));
    }

    private static Map<String, TextChunk> indexChunks(List<TextChunk> chunks) {
        Map<String, TextChunk> indexed = new LinkedHashMap<>();
        for (TextChunk chunk : chunks) {
            indexed.put(chunk.getChunkId(), chunk);
        }
        return Collections.unmodifiableMap(indexed);
    }

    private RecallService(RetrievalChannelRegistry.Fixture fixture, WeightedRrfConfig config,
                          LongSupplier clock) {
        this(fixture, config, clock, new DeterministicTokenEstimator(), new DeterministicReranker(),
            new DeterministicRulePolicy(), 1, true, null);
    }

    private RecallService(RetrievalChannelRegistry.Fixture fixture, CandidateFusion fusion,
                          LongSupplier clock, TokenEstimator tokenEstimator, Reranker reranker,
                          AdaptivePolicy routingPolicy, int maxReplanCount, boolean strictPlanValidation,
                          RetrievalProperties properties) {
        this(fixture, fusion.getConfig(), clock, tokenEstimator, reranker, routingPolicy,
            maxReplanCount, strictPlanValidation, properties, fusion);
    }

    private RecallService(RetrievalChannelRegistry.Fixture fixture, WeightedRrfConfig config,
                          LongSupplier clock, TokenEstimator tokenEstimator, Reranker reranker,
                          AdaptivePolicy routingPolicy, int maxReplanCount, boolean strictPlanValidation,
                          RetrievalProperties properties) {
        this(fixture, config, clock, tokenEstimator, reranker, routingPolicy, maxReplanCount,
            strictPlanValidation, properties, null);
    }

    private RecallService(RetrievalChannelRegistry.Fixture fixture, WeightedRrfConfig config,
                          LongSupplier clock, TokenEstimator tokenEstimator, Reranker reranker,
                          AdaptivePolicy routingPolicy, int maxReplanCount, boolean strictPlanValidation,
                          RetrievalProperties properties, CandidateFusion fusionOverride) {
        this.strictPlanValidation = strictPlanValidation;
        this.properties = properties == null ? new RetrievalProperties() : properties;
        this.bm25 = fixture.getBm25();
        this.vector = fixture.getVector();
        this.chunks = fixture.getChunks();
        this.chunksById = fixture.getChunksById();
        this.entities = fixture.getEntities();
        this.anchorResolver = fixture.getAnchorResolver();
        this.idfByToken = fixture.getIdfByToken();
        this.edges = fixture.getEdges();
        this.graphProvider = fixture.getGraphProvider();
        this.registry = null;
        this.fusion = fusionOverride == null ? new WeightedRrfFusion(config) : fusionOverride;
        this.clock = Objects.requireNonNull(clock, "clock");
        this.tokenEstimator = Objects.requireNonNull(tokenEstimator, "tokenEstimator");
        this.reranker = Objects.requireNonNull(reranker, "reranker");
        this.routingPolicy = Objects.requireNonNull(routingPolicy, "routingPolicy");
        this.maxReplanCount = maxReplanCount;
        this.executor = new RetrievalExecutor(this.bm25, this.vector, this.chunks, this.chunksById,
            this.entities, this.anchorResolver, this.idfByToken, this.edges, this.graphProvider,
            this.fusion, this.clock, this.tokenEstimator, this.reranker, this.routingPolicy,
            this.maxReplanCount, this.strictPlanValidation, this.properties);
    }

    public RetrievalResponse retrieve(RetrievalRequest request) {
        return retrieve(request, null);
    }

    public RetrievalResponse retrieve(RetrievalRequest request, String requestIdOverride) {
        final long started = clock.getAsLong();
        RetrievalCommand command = new RetrievalRequestValidator(properties).validate(request);
        requireStructuredVersions(request);
        if (registry == null) {
            throw notReady("fixture registry is required for structured retrieval");
        }
        RetrievalBudget budget = command.getBudget();
        long deadline = budget.getPrimaryConstraint() == BudgetConstraint.LATENCY
            ? started + budget.getTimeoutMs() * 1_000_000L : 0L;
        try (RetrievalChannelRegistry.Fixture.Lease lease = registry.acquire(command.getGraphName(),
            request.getGraphVersion(), request.getIndexVersion())) {
            RetrievalChannelRegistry.Fixture fixture = lease.getFixture();
            if (fixture.getVector() != null
                && ((request.getVectorSource() != null
                    && !request.getVectorSource().equals(fixture.getVector().getVectorSource()))
                    || (request.getVectorVersion() != null
                    && !request.getVectorVersion().equals(fixture.getVector().getVectorVersion())))) {
                throw notReady("vector source/version mismatch");
            }
            // Carry the configured fusion strategy across the registry delegation; otherwise a
            // caller-supplied strategy would silently fall back to the default weighted RRF.
            RecallService delegate = new RecallService(fixture, fusion, clock, tokenEstimator,
                reranker, routingPolicy, maxReplanCount, strictPlanValidation, properties);
            String requestId = requestIdOverride == null || requestIdOverride.trim().isEmpty()
                ? request.getRequestId() == null || request.getRequestId().trim().isEmpty()
                    ? UUID.randomUUID().toString() : request.getRequestId()
                : requestIdOverride;
            RetrievalResponse response = delegate.execute(command, toFloat(command.getQueryVector()), deadline,
                request.isAllowPartialResults(), started, fixture, requestId);
            response.setGraphName(fixture.getGraphName());
            response.setGraphVersion(fixture.getGraphVersion());
            response.getTrace().setGraphVersion(fixture.getGraphVersion());
            response.getTrace().setIndexVersion(fixture.getIndexVersion());
            if (fixture.getVector() != null) {
                response.getTrace().setVectorVersion(fixture.getVector().getVectorVersion());
            }
            response.getTrace().setElapsedNanos(clock.getAsLong() - started);
            return response;
        }
    }

    private static float[] toFloat(List<Double> values) {
        float[] result = new float[values.size()];
        for (int index = 0; index < values.size(); index++) {
            result[index] = values.get(index).floatValue();
        }
        return result;
    }

    public List<Evidence> retrieve(String query, float[] queryVector, String mode, int topK,
                                   int maxCandidates, long deadlineNanos) {
        return retrieveWithTrace(query, queryVector, mode, topK, maxCandidates, deadlineNanos).getEvidence();
    }

    public RetrievalResponse retrieveWithTrace(String query, float[] queryVector, String mode,
                                               int topK, int maxCandidates, long deadlineNanos) {
        return retrieveWithTrace(query, queryVector, mode, topK, maxCandidates, deadlineNanos, false);
    }

    public RetrievalResponse retrieveWithTrace(String query, float[] queryVector, String mode,
                                               int topK, int maxCandidates, long deadlineNanos,
                                               boolean allowPartialResults) {
        final long started = clock.getAsLong();
        RetrievalRequest request = new RetrievalRequest();
        request.setGraphName("fixture");
        request.setQuery(query);
        request.setMode(mode == null ? "BM25_ONLY" : mode.trim().toUpperCase(Locale.ROOT));
        request.setBudget(new RetrievalBudget(topK, 3000, maxCandidates, null,
            BudgetConstraint.LATENCY));
        List<Double> values = new ArrayList<>();
        if (queryVector != null) {
            for (float value : queryVector) {
                values.add((double) value);
            }
        }
        request.setQueryVector(values);
        RetrievalCommand command = new RetrievalRequestValidator(properties).validate(request);
        if (registry != null) {
            throw notReady("registry retrieval requires a structured request with graph/index versions");
        }
        return execute(command, queryVector, deadlineNanos, allowPartialResults, started, null,
            UUID.randomUUID().toString());
    }

    private RetrievalResponse execute(RetrievalCommand command, float[] queryVector, long deadline,
                                       boolean partial, long started, RetrievalChannelRegistry.Fixture fixture,
                                       String requestId) {
        RetrievalExecutor.RequestResources resources = fixture == null
            ? RetrievalExecutor.RequestResources.empty()
            : new RetrievalExecutor.RequestResources(fixture.getGraphVersion(), fixture.getIndexVersion(),
                fixture.getFailures(), fixture.getGraphProvider(), fixture::acquireLease);
        return executor.execute(command, queryVector, deadline, partial, started, resources, requestId);
    }

    private static void requireStructuredVersions(RetrievalRequest request) {
        if (request.getGraphVersion() == null || request.getGraphVersion().trim().isEmpty()
            || request.getIndexVersion() == null || request.getIndexVersion().trim().isEmpty()) {
            throw new RetrievalException(RetrievalErrorCode.INVALID_REQUEST,
                "graphVersion and indexVersion are required for structured retrieval");
        }
    }

    private static RetrievalException notReady(String message) {
        return new RetrievalException(RetrievalErrorCode.INDEX_NOT_READY, message);
    }


}
