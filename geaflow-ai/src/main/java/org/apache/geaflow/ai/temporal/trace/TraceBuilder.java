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

package org.apache.geaflow.ai.temporal.trace;

import com.google.gson.JsonArray;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import org.apache.geaflow.ai.temporal.api.QueryMode;
import org.apache.geaflow.ai.temporal.candidate.FactCandidate;
import org.apache.geaflow.ai.temporal.candidate.RetrievalProfile;
import org.apache.geaflow.ai.temporal.candidate.TemporalCandidateService;
import org.apache.geaflow.ai.temporal.codec.TemporalJsonCodec;
import org.apache.geaflow.ai.temporal.integration.ConflictResolver;
import org.apache.geaflow.ai.temporal.integration.IncrementalTemporalIntegrator;
import org.apache.geaflow.ai.temporal.integration.ResolutionDecision;
import org.apache.geaflow.ai.temporal.model.Evidence;
import org.apache.geaflow.ai.temporal.model.FactKey;
import org.apache.geaflow.ai.temporal.model.MemoryEventOperation;
import org.apache.geaflow.ai.temporal.model.MemoryFact;
import org.apache.geaflow.ai.temporal.model.MemoryFactVersion;
import org.apache.geaflow.ai.temporal.model.MemoryFactVersionStatus;
import org.apache.geaflow.ai.temporal.model.TimeInterval;
import org.apache.geaflow.ai.temporal.model.VersionRelation;
import org.apache.geaflow.ai.temporal.semantics.NormalizedMemoryEvent;
import org.apache.geaflow.ai.temporal.semantics.TemporalState;

/** Derives audit deltas by replaying accepted events, without sharing Oracle transitions. */
public final class TraceBuilder {

    private static final TemporalJsonCodec CODEC = new TemporalJsonCodec();

    private TraceBuilder() {
    }

    public static Map<String, JsonObject> replay(List<NormalizedMemoryEvent> events) {
        return replayInternal(events, null, 0);
    }

    public static Map<String, JsonObject> replay(List<NormalizedMemoryEvent> events,
                                                RetrievalProfile profile, int topK) {
        validateRetrieval(profile, topK);
        return replayInternal(events, profile, topK);
    }

    private static Map<String, JsonObject> replayInternal(List<NormalizedMemoryEvent> events,
                                                          RetrievalProfile profile, int topK) {
        IncrementalTemporalIntegrator replay = new IncrementalTemporalIntegrator();
        Map<String, JsonObject> result = new HashMap<>();
        List<NormalizedMemoryEvent> ordered = new ArrayList<>(events);
        ordered.sort(Comparator.comparing(NormalizedMemoryEvent::getRecordedAt)
            .thenComparing(NormalizedMemoryEvent::getEventId));
        for (NormalizedMemoryEvent event : ordered) {
            TemporalState before = replay.stateSnapshot();
            ResolutionDecision decision = profile == null ? null : new ConflictResolver().resolve(
                event, before.getVersionsByFactKey()
                    .getOrDefault(event.getFactKey(), Collections.emptyList()));
            final List<FactCandidate> retrieved = profile == null ? Collections.emptyList()
                : retrieve(before, event, decision, profile, topK);
            replay.apply(event);
            TemporalState after = replay.stateSnapshot();
            JsonObject trace = base(event);
            Map<String, MemoryFactVersion> previous = byId(before);
            final Map<String, MemoryFactVersion> current = byId(after);
            if (profile == null) {
                TimeInterval candidateInterval = event.getValidTime();
                if (event.getOperation() == MemoryEventOperation.ADD) {
                    for (MemoryFactVersion version : after.getVersions()) {
                        if (!previous.containsKey(version.getId())
                            && version.getFact().equals(event.getEvent().getFact().get())) {
                            candidateInterval = version.getValidTime();
                        }
                    }
                }
                List<MemoryFactVersion> candidates = candidates(before, event, candidateInterval);
                trace.add("candidateVersionIds", ids(candidates));
                trace.add("readVersionIds", ids(event.getOperation() == MemoryEventOperation.ADD
                    ? candidates : targets(candidates, event)));
                List<String> rules = new ArrayList<>();
                if (event.getOperation() == MemoryEventOperation.ADD) {
                    boolean equal = candidates.stream().anyMatch(v ->
                        sameValue(v.getFact(), event.getEvent().getFact().get()));
                    boolean conflict = candidates.stream().anyMatch(v ->
                        !sameValue(v.getFact(), event.getEvent().getFact().get()));
                    rules.add(equal ? "MERGE_EQUAL_VALUE" : "ASSERT_NEW");
                    if (conflict) {
                        rules.add("COEXIST_CONFLICT");
                    }
                } else {
                    rules.add(event.getOperation() == MemoryEventOperation.CORRECT
                        ? "CORRECT_TARGET" : "RETRACT_TARGET");
                }
                trace.add("rules", strings(rules));
            } else {
                explainRetrieval(trace, retrieved, decision, profile, topK);
            }
            List<String> generated = new ArrayList<>();
            List<String> closed = new ArrayList<>();
            for (MemoryFactVersion version : after.getVersions()) {
                MemoryFactVersion old = previous.get(version.getId());
                if (old == null) {
                    generated.add(version.getId());
                } else if (!old.getTransactionTime().equals(version.getTransactionTime())) {
                    closed.add(version.getId());
                }
            }
            trace.add("generatedVersionIds", strings(generated));
            trace.add("closedVersionIds", strings(closed));
            trace.add("discardedVersionIds", difference(previous.keySet(), current.keySet()));
            trace.add("addedRelations", relationDifference(after, before));
            trace.add("removedRelations", relationDifference(before, after));
            if (profile != null) {
                Set<String> relationInputs = new TreeSet<>();
                for (VersionRelation relation : after.getRelations()) {
                    if (!before.getRelations().contains(relation)) {
                        if (previous.containsKey(relation.getFromVersionId())) {
                            relationInputs.add(relation.getFromVersionId());
                        }
                        if (previous.containsKey(relation.getToVersionId())) {
                            relationInputs.add(relation.getToVersionId());
                        }
                    }
                }
                trace.add("relationReadVersionIds", strings(relationInputs));
                trace.addProperty("relationReadSource", "EXISTING_ENDPOINTS_OF_ADDED_RELATIONS");
            }
            result.put(event.getEventId(), trace);
        }
        Set<String> materialized = byId(replay.stateSnapshot()).keySet();
        for (JsonObject trace : result.values()) {
            List<String> generated = new ArrayList<>();
            List<String> omitted = new ArrayList<>();
            for (com.google.gson.JsonElement value : trace.getAsJsonArray("generatedVersionIds")) {
                String id = value.getAsString();
                (materialized.contains(id) ? generated : omitted).add(id);
            }
            trace.add("generatedVersionIds", strings(generated));
            trace.add("omittedVersionIds", strings(omitted));
        }
        return result;
    }

    public static DecisionTrace decision(NormalizedMemoryEvent event, JsonObject semantic,
                                         String id, int revision, String type, String code,
                                         String validationCode, String error) {
        JsonObject json = semantic == null ? base(event)
            : CODEC.parseObject(CODEC.toJson(semantic));
        json.addProperty("attemptId", id);
        json.addProperty("revision", revision);
        json.addProperty("traceType", type);
        json.addProperty("code", code);
        json.addProperty("validationCode", validationCode);
        json.addProperty("errorMessage", error);
        if (!"ACCEPTED".equals(code)) {
            json.add("rules", strings(Collections.singletonList(code)));
            json.addProperty("targetResolution", "DUPLICATE_NOOP".equals(code)
                ? "NOT_EVALUATED_DUPLICATE" : "BATCH_NOT_COMMITTED");
        }
        return new DecisionTrace(json);
    }

    public static DecisionTrace decision(NormalizedMemoryEvent event, JsonObject semantic,
                                         String id, int revision, String type, String code,
                                         String validationCode, String error,
                                         RetrievalProfile profile, int topK) {
        if (profile == null) {
            return decision(event, semantic, id, revision, type, code, validationCode, error);
        }
        validateRetrieval(profile, topK);
        if ("ACCEPTED".equals(code)) {
            if (semantic == null || !semantic.has("decision")) {
                throw new IllegalArgumentException("Accepted candidate trace requires a resolution");
            }
            return decision(event, semantic, id, revision, type, code, validationCode, error);
        }
        JsonObject json = decision(event, null, id, revision, type, code, validationCode, error)
            .toJson();
        json.addProperty("retrievalProfile", profile.name());
        json.addProperty("candidateTopK", topK);
        json.addProperty("retrievalStatus", "DUPLICATE_NOOP".equals(code)
            ? "NOT_EVALUATED_DUPLICATE" : "BATCH_NOT_COMMITTED");
        json.add("softCandidateVersionIds", new JsonArray());
        json.add("relationReadVersionIds", new JsonArray());
        json.addProperty("relationReadSource", "NOT_EVALUATED");
        json.add("candidateEffectiveInterval", JsonNull.INSTANCE);
        json.add("decision", JsonNull.INSTANCE);
        return new DecisionTrace(json);
    }

    private static List<FactCandidate> retrieve(TemporalState before, NormalizedMemoryEvent event,
                                                ResolutionDecision decision,
                                                RetrievalProfile profile, int topK) {
        try {
            return new TemporalCandidateService(profile).eventFacts(before, event,
                decision.getEffectiveValidTime(), topK);
        } catch (IOException error) {
            throw new IllegalStateException("Candidate retrieval failed for " + event.getEventId(),
                error);
        }
    }

    private static void explainRetrieval(JsonObject trace, List<FactCandidate> candidates,
                                           ResolutionDecision decision,
                                           RetrievalProfile profile, int topK) {
        final Set<String> mandatory = new TreeSet<>();
        final List<String> soft = new ArrayList<>();
        final JsonObject candidateScores = new JsonObject();
        for (FactCandidate candidate : candidates) {
            String id = candidate.getVersion().getId();
            if (candidate.isMandatory()) {
                mandatory.add(id);
            } else {
                soft.add(id);
            }
            JsonObject scores = new JsonObject();
            for (Map.Entry<String, Double> entry : candidate.getScores().entrySet()) {
                scores.addProperty(entry.getKey(), entry.getValue());
            }
            JsonObject ranks = new JsonObject();
            for (Map.Entry<String, Integer> entry : candidate.getChannelRanks().entrySet()) {
                ranks.addProperty(entry.getKey(), entry.getValue());
            }
            JsonObject detail = new JsonObject();
            detail.addProperty("mandatory", candidate.isMandatory());
            detail.addProperty("rank", candidate.getRank());
            detail.addProperty("score", candidate.getScore());
            detail.add("scores", scores);
            detail.add("channelRanks", ranks);
            candidateScores.add(id, detail);
        }
        if (!mandatory.equals(new TreeSet<>(decision.getMandatoryVersionIds()))) {
            throw new IllegalStateException("Candidate mandatory set differs from resolution");
        }
        JsonObject resolved = new JsonObject();
        resolved.addProperty("status", "COMMITTED");
        resolved.add("effectiveValidInterval", interval(decision.getEffectiveValidTime()));
        resolved.add("mandatoryVersionIds", strings(decision.getMandatoryVersionIds()));
        resolved.add("readVersionIds", strings(decision.getReadVersionIds()));
        resolved.add("rules", strings(decision.getRules()));
        resolved.addProperty("targetVersionId", decision.getTargetVersionId());
        trace.add("candidateVersionIds", strings(mandatory));
        trace.add("softCandidateVersionIds", strings(soft));
        trace.add("candidateScores", candidateScores);
        trace.add("readVersionIds", strings(decision.getReadVersionIds()));
        trace.add("rules", strings(decision.getRules()));
        trace.addProperty("retrievalStatus", "COMPLETED");
        trace.addProperty("retrievalProfile", profile.name());
        trace.addProperty("candidateTopK", topK);
        trace.add("candidateEffectiveInterval", interval(decision.getEffectiveValidTime()));
        trace.add("decision", resolved);
    }

    private static JsonObject interval(TimeInterval interval) {
        JsonObject json = new JsonObject();
        json.addProperty("validFrom", interval.getStart().toString());
        json.addProperty("validTo", interval.getEnd().map(Instant::toString).orElse(null));
        return json;
    }

    private static void validateRetrieval(RetrievalProfile profile, int topK) {
        Objects.requireNonNull(profile, "profile");
        if (!profile.isCandidateStrategy() || topK <= 0) {
            throw new IllegalArgumentException("Use a RULE candidate profile and positive topK");
        }
    }

    private static JsonObject base(NormalizedMemoryEvent event) {
        JsonObject json = new JsonObject();
        json.addProperty("schemaVersion", NormalizedMemoryEvent.SCHEMA_VERSION);
        json.addProperty("attemptId", "");
        json.addProperty("revision", 0);
        json.addProperty("traceType", "INPUT");
        json.addProperty("eventId", event.getEventId());
        json.addProperty("payloadHash", event.getPayloadHash());
        json.addProperty("recordedAt", event.getRecordedAt().toString());
        json.addProperty("operation", event.getOperation() == MemoryEventOperation.ADD
            ? "ASSERT" : event.getOperation().name());
        json.add("factKey", CODEC.factKeyToJson(event.getFactKey()));
        json.addProperty("targetVersionId", event.getTargetVersionId().orElse(null));
        json.addProperty("targetResolution", event.getOperation() == MemoryEventOperation.ADD
            ? "NOT_REQUIRED" : "RESOLVED");
        json.addProperty("code", "ACCEPTED");
        json.addProperty("validationCode", "ACCEPTED");
        for (String field : new String[] {"rules", "readVersionIds", "closedVersionIds",
            "generatedVersionIds", "omittedVersionIds", "discardedVersionIds",
            "candidateVersionIds", "addedRelations", "removedRelations"}) {
            json.add(field, new JsonArray());
        }
        json.add("candidateScores", JsonNull.INSTANCE);
        json.addProperty("retrievalStatus", "NOT_APPLICABLE_WEEK6");
        json.addProperty("affectedFactKeyCount", 1);
        json.addProperty("errorMessage", (String) null);
        return json;
    }

    public static QueryTrace query(String id, int revision, QueryMode mode, FactKey key,
                                   Instant validAt, Instant systemAt,
                                   List<MemoryFactVersion> versions, TemporalState state) {
        JsonObject json = new JsonObject();
        json.addProperty("schemaVersion", NormalizedMemoryEvent.SCHEMA_VERSION);
        json.addProperty("queryId", id);
        json.addProperty("revision", revision);
        json.addProperty("mode", mode.name());
        json.add("factKey", CODEC.factKeyToJson(key));
        json.addProperty("validAt", validAt.toString());
        json.addProperty("systemAt", systemAt == null ? null : systemAt.toString());
        json.add("hitVersionIds", ids(versions));
        List<MemoryFactVersion> tombstones = new ArrayList<>();
        for (MemoryFactVersion version : state.getVersionsByFactKey()
            .getOrDefault(key, Collections.emptyList())) {
            if (version.getStatus() == MemoryFactVersionStatus.RETRACTED
                && version.getValidTime().contains(validAt)
                && (mode == QueryMode.HISTORICAL
                    || (mode == QueryMode.CURRENT
                        ? !version.getTransactionTime().getEnd().isPresent()
                        : version.getTransactionTime().contains(systemAt)))) {
                tombstones.add(version);
            }
        }
        json.add("tombstoneVersionIds", ids(tombstones));
        JsonArray evidence = new JsonArray();
        JsonArray payloads = new JsonArray();
        for (MemoryFactVersion version : versions) {
            payloads.add(CODEC.versionToJson(key, version, null));
            for (Evidence item : version.getEvidence()) {
                JsonObject source = new JsonObject();
                source.addProperty("versionId", version.getId());
                source.addProperty("evidenceId", item.getId());
                source.addProperty("sourceId", item.getSource().getId());
                source.addProperty("sourceName", item.getSource().getName());
                evidence.add(source);
            }
        }
        json.add("evidenceSources", evidence);
        json.addProperty("reason", !versions.isEmpty() ? "MATCHED"
            : !tombstones.isEmpty() ? "RETRACTED"
            : state.getVersionsByFactKey().containsKey(key) ? "OUTSIDE_INTERVALS" : "UNKNOWN_FACT_KEY");
        json.addProperty("resultDigest", digest(CODEC.toJson(payloads)));
        return new QueryTrace(json);
    }

    private static List<MemoryFactVersion> candidates(TemporalState state,
                                                     NormalizedMemoryEvent event,
                                                     TimeInterval interval) {
        List<MemoryFactVersion> candidates = new ArrayList<>();
        for (MemoryFactVersion version : state.getVersionsByFactKey()
            .getOrDefault(event.getFactKey(), Collections.emptyList())) {
            if (version.getStatus() == MemoryFactVersionStatus.ACTIVE
                && !version.getTransactionTime().getEnd().isPresent()
                && version.getValidTime().overlaps(interval)) {
                candidates.add(version);
            }
        }
        return candidates;
    }

    private static List<MemoryFactVersion> targets(List<MemoryFactVersion> candidates,
                                                   NormalizedMemoryEvent event) {
        List<MemoryFactVersion> result = new ArrayList<>();
        for (MemoryFactVersion version : candidates) {
            if (event.getTargetVersionId().get().equals(version.getId())) {
                result.add(version);
            }
        }
        return result;
    }

    private static boolean sameValue(MemoryFact left, MemoryFact right) {
        return left.getLiteralValue().equals(right.getLiteralValue())
            && left.getTarget().map(e -> e.getId()).equals(right.getTarget().map(e -> e.getId()));
    }

    private static Map<String, MemoryFactVersion> byId(TemporalState state) {
        Map<String, MemoryFactVersion> result = new HashMap<>();
        for (MemoryFactVersion version : state.getVersions()) {
            result.put(version.getId(), version);
        }
        return result;
    }

    private static JsonArray relationDifference(TemporalState left, TemporalState right) {
        JsonArray result = new JsonArray();
        for (VersionRelation relation : left.getRelations()) {
            if (!right.getRelations().contains(relation)) {
                result.add(CODEC.relationToJson(relation));
            }
        }
        return result;
    }

    private static JsonArray difference(Set<String> left, Set<String> right) {
        Set<String> result = new TreeSet<>(left);
        result.removeAll(right);
        return strings(result);
    }

    public static JsonArray ids(List<MemoryFactVersion> versions) {
        JsonArray values = new JsonArray();
        for (MemoryFactVersion version : versions) {
            values.add(new JsonPrimitive(version.getId()));
        }
        return values;
    }

    private static JsonArray strings(Iterable<String> values) {
        Set<String> ordered = new TreeSet<>();
        for (String value : values) {
            ordered.add(value);
        }
        JsonArray result = new JsonArray();
        for (String value : ordered) {
            result.add(new JsonPrimitive(value));
        }
        return result;
    }

    private static String digest(String value) {
        try {
            byte[] hash = MessageDigest.getInstance("SHA-256")
                .digest(value.getBytes(StandardCharsets.UTF_8));
            StringBuilder result = new StringBuilder();
            for (byte item : hash) {
                result.append(String.format(java.util.Locale.ROOT, "%02x", item & 0xff));
            }
            return result.toString();
        } catch (NoSuchAlgorithmException error) {
            throw new IllegalStateException("SHA-256 unavailable", error);
        }
    }
}
