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
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;
import org.apache.geaflow.ai.retrieval.api.model.ExecutionMode;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalBudget;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalCommand;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalErrorCode;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalException;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalMode;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalRequest;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalResponse;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalTrace;
import org.apache.geaflow.ai.retrieval.api.model.TraceStage;
import org.apache.geaflow.ai.retrieval.channel.graph.EntityAnchorResolver;
import org.apache.geaflow.ai.retrieval.channel.graph.GraphNeighborProvider;
import org.apache.geaflow.ai.retrieval.channel.graph.GraphQueryLease;
import org.apache.geaflow.ai.retrieval.channel.graph.GraphRetrievalConfig;
import org.apache.geaflow.ai.retrieval.channel.graph.GraphRetriever;
import org.apache.geaflow.ai.retrieval.channel.graph.InMemoryGraphNeighborProvider;
import org.apache.geaflow.ai.retrieval.config.RetrievalProperties;
import org.apache.geaflow.ai.retrieval.execution.BudgetConstraint;
import org.apache.geaflow.ai.retrieval.execution.BudgetLedger;
import org.apache.geaflow.ai.retrieval.execution.DeterministicReranker;
import org.apache.geaflow.ai.retrieval.execution.DeterministicTokenEstimator;
import org.apache.geaflow.ai.retrieval.execution.RecallPlan;
import org.apache.geaflow.ai.retrieval.execution.RecallStageStatus;
import org.apache.geaflow.ai.retrieval.execution.RecallStopReason;
import org.apache.geaflow.ai.retrieval.execution.Reranker;
import org.apache.geaflow.ai.retrieval.execution.StageExecutor;
import org.apache.geaflow.ai.retrieval.execution.StageExecutor.Stage;
import org.apache.geaflow.ai.retrieval.execution.StageExecutor.StageResult;
import org.apache.geaflow.ai.retrieval.execution.TokenContextTruncator;
import org.apache.geaflow.ai.retrieval.execution.TokenEstimator;
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
import org.apache.geaflow.ai.retrieval.model.graph.GraphPathRef;

/** Session-free sequential recall; all execution state belongs to one request. */
public final class RecallService {
    private static final double CASCADE_MIN_PROBE_SCORE_GAP = 0.0001D;

    private final Bm25IndexReader bm25;
    private final VectorIndexReader vector;
    private final List<TextChunk> chunks;
    private final Map<String, TextChunk> chunksById;
    private final List<EntityRef> entities;
    private final List<GraphEdgeRef> edges;
    private final GraphNeighborProvider graphProvider;
    private final RetrievalFixtureRegistry registry;
    private final WeightedRrfFusion fusion;
    private final LongSupplier clock;
    private final TokenEstimator tokenEstimator;
    private final Reranker reranker;

    public RecallService(Bm25IndexReader bm25, VectorIndexReader vector, List<TextChunk> chunks,
                         List<EntityRef> entities, List<GraphEdgeRef> edges) {
        this(bm25, vector, chunks, entities, edges, WeightedRrfConfig.DEFAULT);
    }

    public RecallService(Bm25IndexReader bm25, VectorIndexReader vector, List<TextChunk> chunks,
                         List<EntityRef> entities, List<GraphEdgeRef> edges, WeightedRrfConfig config) {
        this(bm25, vector, chunks, entities, edges, null, new InMemoryGraphNeighborProvider(edges), config,
            System::nanoTime, new DeterministicTokenEstimator(), new DeterministicReranker());
    }

    public RecallService(RetrievalFixtureRegistry registry) {
        this(registry, WeightedRrfConfig.DEFAULT);
    }

    public RecallService(RetrievalFixtureRegistry registry, WeightedRrfConfig config) {
        this(registry, config, System::nanoTime);
    }

    /** Accepts a monotonic clock for reproducible deadline verification. */
    public RecallService(RetrievalFixtureRegistry registry, WeightedRrfConfig config, LongSupplier clock) {
        this(registry, config, clock, new DeterministicTokenEstimator(), new DeterministicReranker());
    }

    public RecallService(RetrievalFixtureRegistry registry, WeightedRrfConfig config, LongSupplier clock,
                         TokenEstimator tokenEstimator, Reranker reranker) {
        this(null, null, Collections.emptyList(), Collections.emptyList(), Collections.emptyList(),
            Objects.requireNonNull(registry, "registry"), null, config, clock, tokenEstimator, reranker);
    }

    private RecallService(Bm25IndexReader bm25, VectorIndexReader vector, List<TextChunk> chunks,
                          List<EntityRef> entities, List<GraphEdgeRef> edges, RetrievalFixtureRegistry registry,
                          GraphNeighborProvider graphProvider, WeightedRrfConfig config, LongSupplier clock,
                          TokenEstimator tokenEstimator, Reranker reranker) {
        this.bm25 = bm25;
        this.vector = vector;
        this.chunks = immutable(chunks);
        this.chunksById = indexChunks(this.chunks);
        this.entities = immutable(entities);
        this.edges = immutable(edges);
        this.graphProvider = graphProvider;
        this.registry = registry;
        this.fusion = new WeightedRrfFusion(config);
        this.clock = Objects.requireNonNull(clock, "clock");
        this.tokenEstimator = Objects.requireNonNull(tokenEstimator, "tokenEstimator");
        this.reranker = Objects.requireNonNull(reranker, "reranker");
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

    private RecallService(RetrievalFixtureRegistry.Fixture fixture, WeightedRrfConfig config,
                          LongSupplier clock) {
        this(fixture, config, clock, new DeterministicTokenEstimator(), new DeterministicReranker());
    }

    private RecallService(RetrievalFixtureRegistry.Fixture fixture, WeightedRrfConfig config,
                          LongSupplier clock, TokenEstimator tokenEstimator, Reranker reranker) {
        this.bm25 = fixture.getBm25();
        this.vector = fixture.getVector();
        this.chunks = fixture.getChunks();
        this.chunksById = fixture.getChunksById();
        this.entities = fixture.getEntities();
        this.edges = fixture.getEdges();
        this.graphProvider = fixture.getGraphProvider();
        this.registry = null;
        this.fusion = new WeightedRrfFusion(config);
        this.clock = Objects.requireNonNull(clock, "clock");
        this.tokenEstimator = Objects.requireNonNull(tokenEstimator, "tokenEstimator");
        this.reranker = Objects.requireNonNull(reranker, "reranker");
    }

    public RetrievalResponse retrieve(RetrievalRequest request) {
        final long started = clock.getAsLong();
        RetrievalCommand command = new RetrievalRequestValidator(new RetrievalProperties()).validate(request);
        requireStructuredVersions(request);
        if (registry == null) {
            throw notReady("fixture registry is required for structured retrieval");
        }
        RetrievalBudget budget = command.getBudget();
        long deadline = budget.getPrimaryConstraint() == BudgetConstraint.LATENCY
            ? started + budget.getTimeoutMs() * 1_000_000L : 0L;
        try (RetrievalFixtureRegistry.Fixture.Lease lease = registry.acquire(command.getGraphName(),
            request.getGraphVersion(), request.getIndexVersion())) {
            RetrievalFixtureRegistry.Fixture fixture = lease.getFixture();
            if (fixture.getVector() != null
                && ((request.getVectorSource() != null
                    && !request.getVectorSource().equals(fixture.getVector().getVectorSource()))
                    || (request.getVectorVersion() != null
                    && !request.getVectorVersion().equals(fixture.getVector().getVectorVersion())))) {
                throw notReady("vector source/version mismatch");
            }
            RecallService delegate = new RecallService(fixture, fusion.getConfig(), clock,
                tokenEstimator, reranker);
            String requestId = request.getRequestId() == null || request.getRequestId().trim().isEmpty()
                ? UUID.randomUUID().toString() : request.getRequestId();
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
        RetrievalCommand command = new RetrievalRequestValidator(new RetrievalProperties()).validate(request);
        if (registry != null) {
            throw notReady("registry retrieval requires a structured request with graph/index versions");
        }
        return execute(command, queryVector, deadlineNanos, allowPartialResults, started, null,
            UUID.randomUUID().toString());
    }

    private RetrievalResponse execute(RetrievalCommand command, float[] queryVector, long deadline,
                                       boolean partial, long started, RetrievalFixtureRegistry.Fixture fixture,
                                       String requestId) {
        return executePinned(command, queryVector, deadline, partial, started, fixture, requestId,
            graphProvider);
    }

    private RetrievalResponse executePinned(RetrievalCommand command, float[] queryVector, long deadline,
                                             boolean partial, long started,
                                             RetrievalFixtureRegistry.Fixture fixture, String requestId,
                                             GraphNeighborProvider pinnedGraphProvider) {
        RetrievalBudget budget = command.getBudget();
        int executionCandidateLimit = budget.getPrimaryConstraint() == BudgetConstraint.CANDIDATES
            ? budget.getMaxCandidates() : command.getExecutionCandidateLimit();
        RecallPlan plan = new RecallPlan(command.getMode(), budget.getTopK(), executionCandidateLimit, deadline,
            fixture == null ? null : fixture.getGraphVersion(), fixture == null ? null : fixture.getIndexVersion(),
            vector == null ? null : vector.getVectorVersion(), budget.getPrimaryConstraint(),
            command.getExecutionMode());
        Map<String, RetrievalException> failures = fixture == null ? Collections.emptyMap() : fixture.getFailures();
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
        trace.setExecutionMode(command.getExecutionMode());
        trace.setTokenEstimatorVersion(tokenEstimator.getVersion());
        trace.setRerankerVersion(reranker.getVersion());
        trace.setPrimaryConstraint(budget.getPrimaryConstraint().name());
        trace.setSelectedChannels(new ArrayList<>(plan.getChannelBudgets().keySet()));
        trace.setChannelBudgets(plan.getChannelBudgets());
        int effectiveCandidateBudget = 0;
        for (Integer channelBudget : plan.getChannelBudgets().values()) {
            effectiveCandidateBudget += channelBudget;
        }
        trace.setEffectiveCandidateBudget(effectiveCandidateBudget);
        trace.setEffectiveTopK(plan.getTopK());
        trace.setBm25Weight(fusion.getConfig().getBm25Weight());
        trace.setVectorWeight(fusion.getConfig().getVectorWeight());
        trace.setGraphWeight(fusion.getConfig().getGraphWeight());
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
                requestId, failures, byId, plan, merged, trace, channelElapsedNanos, fixture,
                pinnedGraphProvider, tokenLedger);
            boolean probeDeadline = expired(deadline)
                || trace.getChannelStopReasons().containsValue(RecallStopReason.DEADLINE);
            boolean hasTextEvidence = !merged.isEmpty();
            boolean hasProbeEvidence = merged.size() >= 2;
            boolean hasAnchor = hasTextEvidence && !probeDeadline
                && !new EntityAnchorResolver().resolve(command.getQuery(), entities, deadline, clock).isEmpty();
            double scoreGap = cascadeProbeScoreGap(merged);
            trace.setCascadeProbeScoreGap(scoreGap);
            if (probeDeadline || !hasTextEvidence || !hasProbeEvidence || !hasAnchor
                || scoreGap < CASCADE_MIN_PROBE_SCORE_GAP) {
                RecallStopReason reason = probeDeadline ? RecallStopReason.DEADLINE
                    : !hasTextEvidence ? RecallStopReason.NO_RESULT
                    : !hasProbeEvidence ? RecallStopReason.NO_RESULT
                    : !hasAnchor ? RecallStopReason.NO_RELIABLE_ANCHOR : RecallStopReason.LOW_SCORE_GAP;
                String decision = probeDeadline ? "SKIP_DEADLINE"
                    : !hasTextEvidence ? "SKIP_NO_EVIDENCE"
                    : !hasProbeEvidence ? "SKIP_INSUFFICIENT_EVIDENCE"
                    : !hasAnchor ? "SKIP_NO_ANCHOR" : "SKIP_LOW_SCORE_GAP";
                String message = probeDeadline ? "cascaded probe exhausted the latency budget"
                    : !hasTextEvidence ? "cascaded probe produced no text evidence"
                    : !hasProbeEvidence ? "cascaded probe produced fewer than two evidence candidates"
                    : !hasAnchor ? "cascaded probe produced no reliable entity anchor"
                    : "cascaded probe score gap is below the graph-stage threshold";
                trace.setCascadeDecision(decision);
                if (probeDeadline) {
                    timedOut(partial);
                }
                record(trace, "GRAPH", RecallStageStatus.NOT_RUN, reason, message);
                channelElapsedNanos.put("GRAPH", 0L);
                trace.getStageElapsedNanos().put("GRAPH", 0L);
            } else {
                trace.setCascadeDecision("RUN_GRAPH");
                runChannels(Collections.singletonList(channels.stream()
                    .filter(value -> "GRAPH".equals(value.getKey())).findFirst().orElseThrow()),
                    ExecutionMode.SEQUENTIAL, command, queryVector, deadline, partial, requestId,
                    failures, byId, plan, merged, trace, channelElapsedNanos, fixture, pinnedGraphProvider,
                    tokenLedger);
            }
        } else {
            runChannels(channels, command.getExecutionMode(), command, queryVector, deadline, partial,
                requestId, failures, byId, plan, merged, trace, channelElapsedNanos, fixture,
                pinnedGraphProvider, tokenLedger);
        }
        List<Evidence> evidence = new ArrayList<>();
        for (EvidenceBuilder value : merged.values()) {
            evidence.add(value.build());
        }
        if (plan.getMode() == RetrievalMode.HYBRID) {
            evidence = fusion.sort(evidence);
        } else {
            evidence.sort(Comparator.comparingDouble(Evidence::getFusedScore).reversed().thenComparing(Evidence::getEvidenceId));
        }
        evidence = reranker.rerank(evidence);
        trace.setUniqueEvidenceCandidates(evidence.size());
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
                value.getEntities(), value.getPaths(), value.getSources(), value.getStageScores(), value.getFusedScore(),
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
        List<GraphPathRef> paths = new ArrayList<>();
        for (Evidence value : ranked) {
            for (SourceRef source : value.getSources()) {
                if (!sources.contains(source)) {
                    sources.add(source);
                }
            }
            for (GraphPathRef path : value.getPaths()) {
                if (!paths.contains(path)) {
                    paths.add(path);
                }
            }
        }
        response.setSources(sources);
        response.setPaths(paths);
        response.setRequestId(requestId);
        trace.setRequestId(requestId);
        trace.setElapsedNanos(clock.getAsLong() - started);
        response.setTrace(trace);
        response.setEffectiveBudget(budget);
        response.setDegradedChannels(trace.getDegradedChannels());
        return response;
    }

    private void runChannels(List<Map.Entry<String, Integer>> channels, ExecutionMode executionMode,
                             RetrievalCommand command, float[] queryVector, long deadline, boolean partial,
                             String requestId, Map<String, RetrievalException> failures,
                             Map<String, TextChunk> byId, RecallPlan plan,
                             Map<String, EvidenceBuilder> merged, RetrievalTrace trace,
                             Map<String, Long> channelElapsedNanos,
                             RetrievalFixtureRegistry.Fixture fixture,
                             GraphNeighborProvider pinnedGraphProvider, BudgetLedger tokenLedger) {
        AtomicBoolean anyStageStarted = new AtomicBoolean();
        boolean deadlineAlreadyRecorded = false;
        List<Stage<ChannelOutput>> stages = new ArrayList<>();
        for (Map.Entry<String, Integer> channel : channels) {
            String name = channel.getKey();
            stages.add(new Stage<>(name, () -> {
                try (RetrievalFixtureRegistry.Fixture.Lease stageLease = fixture == null
                    ? null : fixture.acquireLease();
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
                        queryVector, deadline, requestId, failures, byId, plan, stageGraphProvider, tokenLedger);
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
            mergeChannel(name, output, requestId, byId, plan, merged, tokenLedger);
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

    private ChannelOutput searchChannel(String name, int candidateBudget, RetrievalCommand command,
                                        float[] queryVector, long deadline, String requestId,
                                        Map<String, RetrievalException> failures,
                                        Map<String, TextChunk> byId, RecallPlan plan,
                                        GraphNeighborProvider pinnedGraphProvider,
                                        BudgetLedger tokenLedger) {
        RetrievalException loadFailure = failures.get(name);
        if (loadFailure != null) {
            throw loadFailure;
        }
        ChannelOutput output = new ChannelOutput();
        if ("BM25".equals(name)) {
            if (bm25 == null) {
                throw notReady("artifact unavailable");
            }
            ChannelSearchResult<Bm25Hit> search = bm25.searchWithStats(command.getQuery(), candidateBudget,
                plan.getTopK(), deadline, clock);
            output.bm25Hits = search.getHits();
            output.evaluated = search.getCandidatesEvaluated();
            output.rawCandidatesScored = search.getCandidatesEvaluated();
            output.candidates = search.getHits().size();
            output.reason = search.getStopReason();
        } else if ("VECTOR".equals(name)) {
            if (vector == null) {
                throw notReady("artifact unavailable");
            }
            ChannelSearchResult<VectorHit> search = vector.searchWithStats(queryVector, candidateBudget,
                plan.getTopK(), deadline, clock);
            output.vectorHits = search.getHits();
            output.evaluated = search.getCandidatesEvaluated();
            output.rawCandidatesScored = search.getCandidatesEvaluated();
            output.candidates = search.getHits().size();
            output.reason = search.getStopReason();
        } else {
            GraphRetriever retriever = new GraphRetriever(new EntityAnchorResolver(), tokenEstimator);
            GraphRetrievalConfig graphConfig = GraphRetrievalConfig.builder()
                .maxHop(1)
                .candidateBudget(candidateBudget)
                .edgeScanBudget(candidateBudget)
                .neighborCap(candidateBudget)
                .tokenBudget(command.getBudget().getPrimaryConstraint() == BudgetConstraint.TOKENS
                    ? command.getBudget().getTokenBudget() : Integer.MAX_VALUE)
                .samplingSeed(stableGraphSeed(requestId, command.getQuery()))
                .build();
            output.graphHits = retriever.retrieve(command.getQuery(), entities, pinnedGraphProvider,
                byId, graphConfig, deadline, clock, tokenLedger);
            output.graphStats = retriever.getLastStats();
            output.evaluated = output.graphStats.getEdgesExamined();
            output.rawCandidatesScored = output.graphStats.getCandidatesProduced();
            output.candidates = output.graphStats.getCandidatesProduced();
            output.reason = output.graphStats.getStopReason();
        }
        return output;
    }

    private void mergeChannel(String name, ChannelOutput output, String requestId,
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
                    if (!value.paths.contains(hit.getPath())) {
                        value.paths.add(hit.getPath());
                    }
                    value.graphTraces.add(new GraphEvidenceTrace(requestId, plan.getMode(), hit.getSeed(),
                        hit.getAnchor().getMatchType(), hit.getAnchor().getConfidence(),
                        plan.getGraphVersion() == null ? "fixture" : plan.getGraphVersion(), hit.getPath(),
                        Collections.singletonList(id), hit.getPathVertices(), hit.getPathEdges()));
                    if (!value.scores.containsKey(name)) {
                        value.scores.put(name, new ChannelScore(name, hit.getScore(), null, ++rank));
                    }
                }
            }
        }
    }

    private boolean retainTokenEvidence(BudgetLedger tokenLedger, String chunkId, TextChunk chunk) {
        return tokenLedger == null || chunk == null || tokenLedger.tryRetain(chunkId, chunk, tokenEstimator);
    }

    private double cascadeProbeScoreGap(Map<String, EvidenceBuilder> candidates) {
        if (candidates.size() < 2) {
            return 0.0D;
        }
        List<Double> scores = new ArrayList<>();
        for (EvidenceBuilder candidate : candidates.values()) {
            double score = 0.0;
            for (ChannelScore channelScore : candidate.scores.values()) {
                double weight = "BM25".equals(channelScore.getChannel())
                    ? fusion.getConfig().getBm25Weight() : fusion.getConfig().getVectorWeight();
                score += weight / (fusion.getConfig().getRankConstant() + channelScore.getRank());
            }
            scores.add(score);
        }
        if (scores.isEmpty()) {
            return 0.0;
        }
        scores.sort(Comparator.reverseOrder());
        return scores.get(0) - (scores.size() > 1 ? scores.get(1) : 0.0);
    }

    private static final class ChannelOutput {
        private List<Bm25Hit> bm25Hits;
        private List<VectorHit> vectorHits;
        private List<GraphRetriever.GraphHit> graphHits;
        private GraphRetriever.GraphStats graphStats;
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

    private static void timedOut(boolean partial) {
        if (!partial) {
            throw new RetrievalException(RetrievalErrorCode.RETRIEVAL_TIMEOUT, "retrieval deadline exceeded");
        }
    }

    private static long stableGraphSeed(String requestId, String query) {
        String value = requestId + "\u001f" + query;
        long result = 1125899906842597L;
        for (int index = 0; index < value.length(); index++) {
            result = 31L * result + value.charAt(index);
        }
        return result;
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


    private static void requireStructuredVersions(RetrievalRequest request) {
        if (request.getGraphVersion() == null || request.getGraphVersion().trim().isEmpty()
            || request.getIndexVersion() == null || request.getIndexVersion().trim().isEmpty()) {
            throw new RetrievalException(RetrievalErrorCode.INVALID_REQUEST,
                "graphVersion and indexVersion are required for structured retrieval");
        }
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
        private final List<GraphPathRef> paths = new ArrayList<>();
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
            return new Evidence(id, kind, chunk.getText(), Collections.singletonList(chunk), entities, paths,
                Collections.singletonList(source), scores, fused, null, 1, graphTraces,
                chunk.getDataset() == null ? Evidence.LEGACY_SCHEMA_VERSION
                    : Evidence.CURRENT_SCHEMA_VERSION,
                chunk.getDataset(), chunk.getDatasetVersion(),
                chunk.getSplit(), evidenceGraphVersion, indexVersion);
        }
    }
}
