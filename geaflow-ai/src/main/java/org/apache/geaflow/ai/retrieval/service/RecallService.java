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
import org.apache.geaflow.ai.retrieval.channel.graph.GraphNeighborProvider;
import org.apache.geaflow.ai.retrieval.channel.graph.GraphRetriever;
import org.apache.geaflow.ai.retrieval.channel.graph.InMemoryGraphNeighborProvider;
import org.apache.geaflow.ai.retrieval.config.RetrievalProperties;
import org.apache.geaflow.ai.retrieval.execution.RecallPlan;
import org.apache.geaflow.ai.retrieval.execution.RecallStageStatus;
import org.apache.geaflow.ai.retrieval.execution.RecallStopReason;
import org.apache.geaflow.ai.retrieval.fusion.WeightedRrfConfig;
import org.apache.geaflow.ai.retrieval.fusion.WeightedRrfFusion;
import org.apache.geaflow.ai.retrieval.index.ChannelSearchResult;
import org.apache.geaflow.ai.retrieval.index.bm25.Bm25Hit;
import org.apache.geaflow.ai.retrieval.index.bm25.Bm25IndexReader;
import org.apache.geaflow.ai.retrieval.index.vector.VectorHit;
import org.apache.geaflow.ai.retrieval.index.vector.VectorIndexReader;
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

    public RecallService(Bm25IndexReader bm25, VectorIndexReader vector, List<TextChunk> chunks,
                         List<EntityRef> entities, List<GraphEdgeRef> edges) {
        this(bm25, vector, chunks, entities, edges, WeightedRrfConfig.DEFAULT);
    }

    public RecallService(Bm25IndexReader bm25, VectorIndexReader vector, List<TextChunk> chunks,
                         List<EntityRef> entities, List<GraphEdgeRef> edges, WeightedRrfConfig config) {
        this(bm25, vector, chunks, entities, edges, null, new InMemoryGraphNeighborProvider(edges), config,
            System::nanoTime);
    }

    public RecallService(RetrievalFixtureRegistry registry) {
        this(registry, WeightedRrfConfig.DEFAULT);
    }

    public RecallService(RetrievalFixtureRegistry registry, WeightedRrfConfig config) {
        this(registry, config, System::nanoTime);
    }

    /** Accepts a monotonic clock for reproducible deadline verification. */
    public RecallService(RetrievalFixtureRegistry registry, WeightedRrfConfig config, LongSupplier clock) {
        this(null, null, Collections.emptyList(), Collections.emptyList(), Collections.emptyList(),
            Objects.requireNonNull(registry, "registry"), null, config, clock);
    }

    private RecallService(Bm25IndexReader bm25, VectorIndexReader vector, List<TextChunk> chunks,
                          List<EntityRef> entities, List<GraphEdgeRef> edges, RetrievalFixtureRegistry registry,
                          GraphNeighborProvider graphProvider, WeightedRrfConfig config, LongSupplier clock) {
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
    }

    public RetrievalResponse retrieve(RetrievalRequest request) {
        final long started = clock.getAsLong();
        RetrievalCommand command = new RetrievalRequestValidator(new RetrievalProperties()).validate(request);
        requireStructuredVersions(request);
        if (registry == null) {
            throw notReady("fixture registry is required for structured retrieval");
        }
        RetrievalBudget budget = command.getBudget();
        long deadline = started + budget.getTimeoutMs() * 1_000_000L;
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
            RecallService delegate = new RecallService(fixture, fusion.getConfig(), clock);
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
        request.setBudget(new RetrievalBudget(topK, 3000, maxCandidates, null));
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
        RetrievalBudget budget = command.getBudget();
        RecallPlan plan = new RecallPlan(command.getMode(), budget.getTopK(), budget.getMaxCandidates(), deadline,
            fixture == null ? null : fixture.getGraphVersion(), fixture == null ? null : fixture.getIndexVersion(),
            vector == null ? null : vector.getVectorVersion());
        Map<String, RetrievalException> failures = fixture == null ? Collections.emptyMap() : fixture.getFailures();
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
        trace.setIndexVersion(plan.getIndexVersion());
        trace.setVectorVersion(plan.getVectorVersion());
        trace.setOriginalQuery(command.getQuery());
        trace.setSelectedMode(plan.getMode());
        trace.setExecutionMode(ExecutionMode.SEQUENTIAL);
        trace.setSelectedChannels(new ArrayList<>(plan.getChannelBudgets().keySet()));
        trace.setChannelBudgets(plan.getChannelBudgets());
        trace.setEffectiveCandidateBudget(budget.getMaxCandidates());
        trace.setEffectiveTopK(plan.getTopK());
        trace.setBm25Weight(fusion.getConfig().getBm25Weight());
        trace.setVectorWeight(fusion.getConfig().getVectorWeight());
        trace.setGraphWeight(fusion.getConfig().getGraphWeight());
        trace.setRrfRankConstant(fusion.getConfig().getRankConstant());
        Map<String, Long> channelElapsedNanos = new LinkedHashMap<>();
        for (Map.Entry<String, Integer> channel : plan.getChannelBudgets().entrySet()) {
            String name = channel.getKey();
            long channelStarted = clock.getAsLong();
            trace.getEvaluatedCounts().put(name, 0);
            trace.getCandidateCounts().put(name, 0);
            if (expired(deadline)) {
                timedOut(partial);
                record(trace, name, RecallStageStatus.NOT_RUN, RecallStopReason.DEADLINE, "deadline reached before channel execution");
                channelElapsedNanos.put(name, clock.getAsLong() - channelStarted);
                continue;
            }
            try {
                RetrievalException loadFailure = failures.get(name);
                if (loadFailure != null) {
                    throw loadFailure;
                }
                RecallStopReason reason;
                if ("BM25".equals(name)) {
                    if (bm25 == null) {
                        throw notReady("artifact unavailable");
                    }
                    ChannelSearchResult<Bm25Hit> search = bm25.searchWithStats(command.getQuery(), channel.getValue(),
                        plan.getTopK(), deadline, clock);
                    trace.getEvaluatedCounts().put(name, search.getCandidatesEvaluated());
                    trace.getCandidateCounts().put(name, search.getHits().size());
                    for (Bm25Hit hit : search.getHits()) {
                        EvidenceBuilder builder = builder(merged, byId, hit.getChunkId(), hit.getDocumentId(), EvidenceKind.CHUNK);
                        if (!builder.chunk.getText().equals(hit.getText())) {
                            throw notReady("BM25 text does not match fixture");
                        }
                        builder.scores.put(name, new ChannelScore(name, hit.getScore(), null, hit.getRank()));
                    }
                    reason = search.getStopReason();
                } else if ("VECTOR".equals(name)) {
                    if (vector == null) {
                        throw notReady("artifact unavailable");
                    }
                    ChannelSearchResult<VectorHit> search = vector.searchWithStats(queryVector, channel.getValue(),
                        plan.getTopK(), deadline, clock);
                    trace.getEvaluatedCounts().put(name, search.getCandidatesEvaluated());
                    trace.getCandidateCounts().put(name, search.getHits().size());
                    for (VectorHit hit : search.getHits()) {
                        EvidenceBuilder builder = builder(merged, byId, hit.getChunkId(), hit.getDocumentId(), EvidenceKind.CHUNK);
                        builder.scores.put(name, new ChannelScore(name, hit.getSimilarity(), null, hit.getRank()));
                    }
                    reason = search.getStopReason();
                } else {
                    GraphRetriever retriever = new GraphRetriever();
                    final List<GraphRetriever.GraphHit> hits = retriever.retrieve(command.getQuery(), entities, graphProvider, byId,
                        channel.getValue(), channel.getValue(), deadline, clock);
                    GraphRetriever.GraphStats stats = retriever.getLastStats();
                    trace.setAnchorsConsidered(stats.getAnchorsConsidered());
                    trace.setEdgesExamined(stats.getEdgesExamined());
                    trace.setNeighborsSampled(stats.getNeighborsSampled());
                    trace.setVerticesReached(stats.getVerticesReached());
                    trace.setGraphCandidatesProduced(stats.getCandidatesProduced());
                    trace.getValidationErrors().addAll(stats.getValidationErrors());
                    trace.getEvaluatedCounts().put(name, stats.getEdgesExamined());
                    trace.getCandidateCounts().put(name, stats.getCandidatesProduced());
                    int rank = 0;
                    for (GraphRetriever.GraphHit hit : hits) {
                        for (String id : hit.getSourceChunkIds()) {
                            EvidenceBuilder builder = builder(merged, byId, id, byId.get(id).getDocumentId(), EvidenceKind.GRAPH_PATH);
                            if (!builder.entities.contains(hit.getAnchor().getEntity())) {
                                builder.entities.add(hit.getAnchor().getEntity());
                            }
                            if (!builder.paths.contains(hit.getPath())) {
                                builder.paths.add(hit.getPath());
                            }
                            builder.graphTraces.add(new GraphEvidenceTrace(requestId,
                                plan.getGraphVersion() == null ? "fixture" : plan.getGraphVersion(),
                                hit.getAnchor().getEntity().getEntityId(), hit.getAnchor().getMatchType(),
                                hit.getAnchor().getConfidence(), hit.getPath(),
                                hit.getSourceChunkIds()));
                            if (!builder.scores.containsKey(name)) {
                                builder.scores.put(name, new ChannelScore(name, hit.getScore(), null, ++rank));
                            }
                        }
                    }
                    reason = stats.getStopReason();
                }
                if (reason == RecallStopReason.DEADLINE) {
                    timedOut(partial);
                }
                record(trace, name, reason == RecallStopReason.DEADLINE
                        || reason == RecallStopReason.TRACE_VALIDATION_ERROR
                        ? RecallStageStatus.DEGRADED : RecallStageStatus.SUCCESS,
                    reason, null);
                channelElapsedNanos.put(name, clock.getAsLong() - channelStarted);
            } catch (RetrievalException error) {
                if (!partial || error.getCode() != RetrievalErrorCode.INDEX_NOT_READY) {
                    throw error;
                }
                for (EvidenceBuilder value : merged.values()) {
                    value.scores.remove(name);
                }
                merged.values().removeIf(value -> value.scores.isEmpty());
                record(trace, name, RecallStageStatus.DEGRADED, RecallStopReason.INDEX_NOT_READY, error.getMessage());
                channelElapsedNanos.put(name, clock.getAsLong() - channelStarted);
            }
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
        boolean truncated = evidence.size() > plan.getTopK();
        List<Evidence> ranked = new ArrayList<>();
        for (Evidence value : evidence.subList(0, Math.min(plan.getTopK(), evidence.size()))) {
            ranked.add(new Evidence(value.getEvidenceId(), value.getKind(), value.getText(), value.getChunks(),
                value.getEntities(), value.getPaths(), value.getSources(), value.getStageScores(), value.getFusedScore(),
                value.getFinalScore(), ranked.size() + 1, value.getGraphTraces()));
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

    private boolean expired(long deadline) {
        return deadline != 0L && clock.getAsLong() - deadline >= 0L;
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


    private static void requireStructuredVersions(RetrievalRequest request) {
        if (request.getGraphVersion() == null || request.getGraphVersion().trim().isEmpty()
            || request.getIndexVersion() == null || request.getIndexVersion().trim().isEmpty()) {
            throw new RetrievalException(RetrievalErrorCode.INVALID_REQUEST,
                "graphVersion and indexVersion are required for structured retrieval");
        }
    }

    private static EvidenceBuilder builder(Map<String, EvidenceBuilder> merged, Map<String, TextChunk> chunks,
                                            String chunkId, String documentId, EvidenceKind kind) {
        TextChunk chunk = chunks.get(chunkId);
        if (chunk == null || !chunk.getDocumentId().equals(documentId)) {
            throw notReady("channel chunk does not match fixture");
        }
        String id = documentId.length() + ":" + documentId + chunkId;
        return merged.computeIfAbsent(id, key -> new EvidenceBuilder(id, chunk, kind));
    }

    private static final class EvidenceBuilder {
        private final String id;
        private final TextChunk chunk;
        private final EvidenceKind kind;
        private final Map<String, ChannelScore> scores = new LinkedHashMap<>();
        private final List<EntityRef> entities = new ArrayList<>();
        private final List<GraphPathRef> paths = new ArrayList<>();
        private final List<GraphEvidenceTrace> graphTraces = new ArrayList<>();

        private EvidenceBuilder(String id, TextChunk chunk, EvidenceKind kind) {
            this.id = id;
            this.chunk = chunk;
            this.kind = kind;
        }

        private Evidence build() {
            double fused = 0.0;
            for (ChannelScore score : scores.values()) {
                fused += 1.0 / (60.0 + score.getRank());
            }
            if (chunk.getSourceUri() == null) {
                throw notReady("chunk source URI is missing");
            }
            SourceRef source = new SourceRef(chunk.getDocumentId(), chunk.getSourceUri(),
                chunk.getStartOffset(), chunk.getEndOffset());
            return new Evidence(id, kind, chunk.getText(), Collections.singletonList(chunk), entities, paths,
                Collections.singletonList(source), scores, fused, null, 1, graphTraces);
        }
    }
}
