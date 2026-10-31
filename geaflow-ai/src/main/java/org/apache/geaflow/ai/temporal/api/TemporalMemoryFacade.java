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

package org.apache.geaflow.ai.temporal.api;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import org.apache.geaflow.ai.temporal.adapter.MemoryGraphAdapter;
import org.apache.geaflow.ai.temporal.candidate.RetrievalProfile;
import org.apache.geaflow.ai.temporal.candidate.TemporalCandidateService;
import org.apache.geaflow.ai.temporal.codec.TemporalJsonCodec;
import org.apache.geaflow.ai.temporal.integration.IncrementalTemporalIntegrator;
import org.apache.geaflow.ai.temporal.model.FactKey;
import org.apache.geaflow.ai.temporal.model.MemoryFactVersion;
import org.apache.geaflow.ai.temporal.oracle.FullReplayOracle;
import org.apache.geaflow.ai.temporal.query.TemporalQueryService;
import org.apache.geaflow.ai.temporal.semantics.CanonicalSnapshot;
import org.apache.geaflow.ai.temporal.semantics.EventLedgerDecision;
import org.apache.geaflow.ai.temporal.semantics.NormalizedMemoryEvent;
import org.apache.geaflow.ai.temporal.semantics.SnapshotComparator;
import org.apache.geaflow.ai.temporal.semantics.TemporalErrorCode;
import org.apache.geaflow.ai.temporal.semantics.TemporalValidationException;
import org.apache.geaflow.ai.temporal.trace.DecisionTrace;
import org.apache.geaflow.ai.temporal.trace.QueryTrace;
import org.apache.geaflow.ai.temporal.trace.TraceBuilder;

/**
 * Single-session, finite-batch API. A staged graph and explanations publish together.
 * Staging rebuilds Java state; this facade makes no incremental-performance claim.
 */
public final class TemporalMemoryFacade {

    private static final Comparator<NormalizedMemoryEvent> EVENT_ORDER =
        Comparator.comparing(NormalizedMemoryEvent::getRecordedAt)
            .thenComparing(NormalizedMemoryEvent::getEventId);

    private final TemporalJsonCodec codec = new TemporalJsonCodec();
    private final List<DecisionTrace> decisions = new ArrayList<>();
    private final List<QueryTrace> queries = new ArrayList<>();
    private final List<JsonObject> operations = new ArrayList<>();
    private CanonicalSnapshot snapshot = new IncrementalTemporalIntegrator().canonicalSnapshot();
    private int revision;
    private int attemptSequence;

    public ReplayResult replay(List<NormalizedMemoryEvent> events) {
        return commit(events, true, null, 20);
    }

    public ReplayResult applyBatch(List<NormalizedMemoryEvent> events) {
        return commit(events, false, null, 20);
    }

    public ReplayResult replay(List<NormalizedMemoryEvent> events, RetrievalProfile profile, int topK) {
        validateDecisionRetrieval(profile, topK);
        return commit(events, true, profile, topK);
    }

    public ReplayResult applyBatch(List<NormalizedMemoryEvent> events, RetrievalProfile profile, int topK) {
        validateDecisionRetrieval(profile, topK);
        return commit(events, false, profile, topK);
    }

    private ReplayResult commit(List<NormalizedMemoryEvent> events, boolean reset,
                                 RetrievalProfile profile, int topK) {
        if (events == null) {
            throw new TemporalValidationException(TemporalErrorCode.REJECT_INPUT,
                "events must not be null");
        }
        List<NormalizedMemoryEvent> ordered = new ArrayList<>(events);
        for (NormalizedMemoryEvent event : ordered) {
            if (event == null) {
                throw new TemporalValidationException(TemporalErrorCode.REJECT_INPUT,
                    "event must not be null");
            }
        }
        ordered.sort(EVENT_ORDER);
        JsonObject operation = eventOperation(reset ? "REPLAY" : "APPLY_BATCH", ordered);
        if (profile != null) {
            operation.addProperty("candidateProfile", profile.name());
            operation.addProperty("candidateTopK", topK);
        }
        List<DecisionTrace> attempts = new ArrayList<>();
        try {
            IncrementalTemporalIntegrator staged = new IncrementalTemporalIntegrator();
            if (!reset) {
                staged.applyBatch(snapshot.getEvents());
            }
            List<EventLedgerDecision> codes = staged.applyBatch(ordered);
            CanonicalSnapshot stagedSnapshot = reset && profile == null
                ? new FullReplayOracle().replayToSnapshot(ordered)
                : roundTrip(staged.canonicalSnapshot());
            if (!new SnapshotComparator().compare(staged.canonicalSnapshot(), stagedSnapshot)
                .isEquivalent()) {
                throw new IllegalStateException("Java and formal Oracle snapshots differ");
            }
            if (profile != null && !new SnapshotComparator().compare(stagedSnapshot,
                new FullReplayOracle().replayToSnapshot(stagedSnapshot.getEvents())).isEquivalent()) {
                throw new IllegalStateException("Conflict resolver and independent Oracle snapshots differ");
            }
            boolean replaySemantics = profile == null || codes.contains(EventLedgerDecision.ACCEPTED);
            Map<String, JsonObject> semantics = profile == null
                ? TraceBuilder.replay(stagedSnapshot.getEvents())
                : replaySemantics
                    ? TraceBuilder.replay(stagedSnapshot.getEvents(), profile, topK)
                    : Collections.emptyMap();
            int nextRevision = revision + 1;
            int nextAttempt = attemptSequence;
            Set<String> acceptedIds = new TreeSet<>();
            int accepted = 0;
            for (int index = 0; index < ordered.size(); index++) {
                NormalizedMemoryEvent event = ordered.get(index);
                String code = codes.get(index).name();
                boolean changed = codes.get(index) == EventLedgerDecision.ACCEPTED;
                if (changed) {
                    accepted++;
                    acceptedIds.add(event.getEventId());
                }
                attempts.add(TraceBuilder.decision(event,
                    changed ? semantics.get(event.getEventId()) : null,
                    "attempt-" + (++nextAttempt), nextRevision, "INPUT", code, code, null, profile, topK));
            }
            List<DecisionTrace> replayed = new ArrayList<>();
            if (!reset && accepted > 0) {
                for (NormalizedMemoryEvent event : stagedSnapshot.getEvents()) {
                    if (!acceptedIds.contains(event.getEventId())) {
                        replayed.add(TraceBuilder.decision(event, semantics.get(event.getEventId()),
                            "replay-" + nextRevision + "-" + event.getEventId(), nextRevision,
                            "REPLAY", "ACCEPTED", "ACCEPTED", null, profile, topK));
                    }
                }
            }
            Map<String, Integer> statistics = new LinkedHashMap<>();
            statistics.put("submittedCount", ordered.size());
            statistics.put("acceptedCount", accepted);
            statistics.put("duplicateCount", ordered.size() - accepted);
            statistics.put("stateEventCount", stagedSnapshot.getEvents().size());
            statistics.put("traceReplayEventCount", replaySemantics ? stagedSnapshot.getEvents().size() : 0);
            statistics.put("versionCount", stagedSnapshot.getState().getVersions().size());
            statistics.put("relationCount", stagedSnapshot.getState().getRelations().size());
            final ReplayResult result = new ReplayResult(stagedSnapshot, attempts, statistics,
                profile != null ? "JAVA_CONFLICT_RESOLVER_MEMORY_GRAPH"
                    : reset ? "FULL_REPLAY_MEMORY_GRAPH" : "JAVA_LOCAL_STAGED_REPLAY");
            snapshot = stagedSnapshot;
            revision = nextRevision;
            attemptSequence = nextAttempt;
            decisions.addAll(attempts);
            decisions.addAll(replayed);
            operations.add(operation);
            return result;
        } catch (IllegalArgumentException error) {
            TemporalValidationException failure = error instanceof TemporalValidationException
                ? (TemporalValidationException) error
                : new TemporalValidationException(TemporalErrorCode.REJECT_REFERENCE_INTEGRITY,
                    error.getMessage());
            operation.addProperty("expectedError", failure.getCode().name());
            for (NormalizedMemoryEvent event : ordered) {
                decisions.add(TraceBuilder.decision(event, null,
                    "attempt-" + (++attemptSequence), revision, "INPUT", "BATCH_ROLLED_BACK",
                    failure.getCode().name(), failure.getMessage(), profile, topK));
            }
            operations.add(operation);
            throw failure;
        }
    }

    private static void validateDecisionRetrieval(RetrievalProfile profile, int topK) {
        if (profile == null || !profile.isCandidateStrategy() || topK <= 0) {
            throw new TemporalValidationException(TemporalErrorCode.REJECT_INPUT,
                "Decision retrieval requires RULE_ONLY, RULE_BM25, RULE_MINHASH or RULE_ALL and positive topK");
        }
    }

    public CanonicalSnapshot getSnapshot() {
        return snapshot;
    }

    public CandidateResult candidates(QueryMode mode, FactKey factKey, String text,
                                      Instant validAt, Instant systemAt, int topK)
        throws IOException {
        return candidates(mode, factKey, text, validAt, systemAt, topK, RetrievalProfile.BASELINE);
    }

    public CandidateResult candidates(QueryMode mode, FactKey factKey, String text,
                                      Instant validAt, Instant systemAt, int topK,
                                      RetrievalProfile profile) throws IOException {
        if (mode == null || factKey == null || text == null || validAt == null || topK <= 0) {
            throw new TemporalValidationException(TemporalErrorCode.REJECT_INPUT,
                "mode, factKey, text and validAt are required; topK must be positive");
        }
        if ((mode == QueryMode.AS_OF) != (systemAt != null)) {
            throw new TemporalValidationException(TemporalErrorCode.REJECT_INPUT,
                "systemAt is required only for AS_OF");
        }
        if (profile == null) {
            throw new TemporalValidationException(TemporalErrorCode.REJECT_INPUT,
                "profile must not be null");
        }
        TemporalCandidateService service = new TemporalCandidateService(profile);
        return new CandidateResult(mode, factKey, text, validAt, systemAt, topK,
            service.facts(snapshot, mode, factKey, validAt, systemAt, text, topK),
            service.evidence(snapshot, mode, factKey, validAt, systemAt, text, topK),
            snapshot.getGeneratingEventIds(), profile);
    }

    public QueryResult query(QueryMode mode, FactKey factKey, Instant validAt, Instant systemAt) {
        Objects.requireNonNull(mode, "mode");
        Objects.requireNonNull(factKey, "factKey");
        Objects.requireNonNull(validAt, "validAt");
        if ((mode == QueryMode.AS_OF) != (systemAt != null)) {
            throw new TemporalValidationException(TemporalErrorCode.REJECT_INPUT,
                "systemAt is required only for AS_OF");
        }
        TemporalQueryService service = new TemporalQueryService();
        List<MemoryFactVersion> matches;
        if (mode == QueryMode.CURRENT) {
            matches = service.current(snapshot.getState(), factKey, validAt);
        } else if (mode == QueryMode.HISTORICAL) {
            matches = service.historical(snapshot.getState(), factKey, validAt);
        } else {
            matches = service.asOf(snapshot.getState(), factKey, validAt, systemAt);
        }
        final QueryTrace trace = TraceBuilder.query("query-" + (queries.size() + 1), revision,
            mode, factKey, validAt, systemAt, matches, snapshot.getState());
        JsonObject operation = new JsonObject();
        operation.addProperty("method", "QUERY");
        operation.addProperty("mode", mode.name());
        operation.add("factKey", codec.factKeyToJson(factKey));
        operation.addProperty("validAt", validAt.toString());
        operation.addProperty("systemAt", systemAt == null ? null : systemAt.toString());
        queries.add(trace);
        operations.add(operation);
        return new QueryResult(matches, trace);
    }

    public List<DecisionTrace> getDecisionTraces() {
        return Collections.unmodifiableList(new ArrayList<>(decisions));
    }

    public List<QueryTrace> getQueryTraces() {
        return Collections.unmodifiableList(new ArrayList<>(queries));
    }

    public List<DecisionTrace> getTrace(String eventId) {
        Objects.requireNonNull(eventId, "eventId");
        List<DecisionTrace> matches = new ArrayList<>();
        for (DecisionTrace trace : decisions) {
            if (eventId.equals(trace.getEventId()) || eventId.equals(trace.getId())) {
                matches.add(trace);
            }
        }
        return Collections.unmodifiableList(matches);
    }

    public QueryTrace getQueryTrace(String queryId) {
        for (QueryTrace trace : queries) {
            if (trace.getId().equals(queryId)) {
                return trace;
            }
        }
        return null;
    }

    public JsonArray getOperations() {
        JsonArray result = new JsonArray();
        for (JsonObject operation : operations) {
            result.add(codec.parseObject(codec.toJson(operation)));
        }
        return result;
    }

    public ValidationResult validateInvariants() {
        List<String> violations = new ArrayList<>();
        try {
            CanonicalSnapshot reloaded = roundTrip(snapshot);
            if (!new SnapshotComparator().compare(snapshot, reloaded).isEquivalent()) {
                violations.add("GRAPH_ROUND_TRIP_DIFF");
            }
            CanonicalSnapshot oracle = new FullReplayOracle().replayToSnapshot(snapshot.getEvents());
            if (!new SnapshotComparator().compare(snapshot, oracle).isEquivalent()) {
                violations.add("ORACLE_DIFF");
            }
        } catch (IllegalArgumentException error) {
            violations.add("REFERENCE_INTEGRITY: " + error.getMessage());
        }
        return new ValidationResult(violations);
    }

    private JsonObject eventOperation(String method, List<NormalizedMemoryEvent> events) {
        JsonObject operation = new JsonObject();
        operation.addProperty("method", method);
        JsonArray encoded = new JsonArray();
        for (NormalizedMemoryEvent event : events) {
            encoded.add(codec.eventToJson(event));
        }
        operation.add("events", encoded);
        operation.addProperty("expectedError", (String) null);
        return operation;
    }

    private static CanonicalSnapshot roundTrip(CanonicalSnapshot value) {
        MemoryGraphAdapter adapter = new MemoryGraphAdapter();
        return adapter.fromGraph(adapter.toGraph(value));
    }
}
