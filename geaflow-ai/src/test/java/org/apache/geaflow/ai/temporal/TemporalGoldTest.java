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

package org.apache.geaflow.ai.temporal;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.geaflow.ai.temporal.api.QueryMode;
import org.apache.geaflow.ai.temporal.api.QueryResult;
import org.apache.geaflow.ai.temporal.api.ReplayResult;
import org.apache.geaflow.ai.temporal.api.TemporalMemoryFacade;
import org.apache.geaflow.ai.temporal.candidate.RetrievalProfile;
import org.apache.geaflow.ai.temporal.integration.IncrementalTemporalIntegrator;
import org.apache.geaflow.ai.temporal.model.Evidence;
import org.apache.geaflow.ai.temporal.model.FactKey;
import org.apache.geaflow.ai.temporal.model.MemoryEntity;
import org.apache.geaflow.ai.temporal.model.MemoryEvent;
import org.apache.geaflow.ai.temporal.model.MemoryFact;
import org.apache.geaflow.ai.temporal.model.MemoryFactVersion;
import org.apache.geaflow.ai.temporal.model.MemoryFactVersionStatus;
import org.apache.geaflow.ai.temporal.model.Source;
import org.apache.geaflow.ai.temporal.model.TimeInterval;
import org.apache.geaflow.ai.temporal.model.VersionRelation;
import org.apache.geaflow.ai.temporal.model.VersionRelationType;
import org.apache.geaflow.ai.temporal.oracle.FullReplayOracle;
import org.apache.geaflow.ai.temporal.query.TemporalQueryService;
import org.apache.geaflow.ai.temporal.semantics.CanonicalSnapshot;
import org.apache.geaflow.ai.temporal.semantics.EventLedgerDecision;
import org.apache.geaflow.ai.temporal.semantics.EventNormalizer;
import org.apache.geaflow.ai.temporal.semantics.NormalizedMemoryEvent;
import org.apache.geaflow.ai.temporal.semantics.TemporalState;
import org.apache.geaflow.ai.temporal.semantics.TemporalValidationException;
import org.apache.geaflow.ai.temporal.semantics.VersionIdentity;
import org.apache.geaflow.ai.temporal.trace.DecisionTrace;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Hand-authored semantic expectations, independent of either replay implementation. */
public class TemporalGoldTest {

    private static final List<String> RESOURCES = Arrays.asList(
        "01-assert-entity-reference", "02-identical-duplicate", "03-event-id-reuse",
        "04-evidence-merge", "05-conflict-coexistence", "06-adjacent-intervals",
        "07-partial-correction", "08-partial-retraction", "09-valid-time-late",
        "10-system-time-disorder", "11-same-timestamp", "12-invalid-target-atomicity");

    private static final Comparator<Instant> END_ORDER =
        Comparator.nullsLast(Comparator.naturalOrder());

    // Check the documented query order independently of the query service.
    private static final Comparator<MemoryFactVersion> QUERY_ORDER =
        Comparator.comparing((MemoryFactVersion v) -> v.getValidTime().getStart())
            .thenComparing(v -> v.getValidTime().getEnd().orElse(null), END_ORDER)
            .thenComparing(v -> v.getTransactionTime().getStart())
            .thenComparing(v -> v.getTransactionTime().getEnd().orElse(null), END_ORDER)
            .thenComparing(MemoryFactVersion::getId);

    @Test
    public void testHandAuthoredGoldStreams() throws Exception {
        for (String name : RESOURCES) {
            try (InputStream input = getClass().getResourceAsStream(
                "/temporal/gold/" + name + ".json")) {
                Assertions.assertNotNull(input, name);
                try (Reader reader = new InputStreamReader(input, StandardCharsets.UTF_8)) {
                    Fixture fixture = new Gson().fromJson(reader, Fixture.class);
                    Assertions.assertEquals(name, fixture.name);
                    TemporalMemoryFacade legacy = new GoldCase(fixture, null).verify();
                    for (RetrievalProfile profile : Arrays.asList(RetrievalProfile.RULE_ONLY,
                        RetrievalProfile.RULE_BM25, RetrievalProfile.RULE_MINHASH,
                        RetrievalProfile.RULE_ALL)) {
                        TemporalMemoryFacade actual = new GoldCase(fixture, profile).verify();
                        Assertions.assertEquals(legacy.getSnapshot(), actual.getSnapshot(),
                            name + ": candidate strategy changed committed facts or ledger " + profile);
                        Assertions.assertEquals(semanticTraces(legacy), semanticTraces(actual),
                            name + ": candidate strategy changed semantic decisions " + profile);
                        Assertions.assertEquals(legacy.getQueryTraces().size(),
                            actual.getQueryTraces().size(), name);
                        for (int index = 0; index < legacy.getQueryTraces().size(); index++) {
                            Assertions.assertEquals(legacy.getQueryTraces().get(index).toJson(),
                                actual.getQueryTraces().get(index).toJson(),
                                name + ": candidate strategy changed query explanation " + profile);
                        }
                    }
                }
            }
        }
    }

    private static List<JsonObject> semanticTraces(TemporalMemoryFacade facade) {
        final List<JsonObject> result = new ArrayList<>();
        for (DecisionTrace trace : facade.getDecisionTraces()) {
            JsonObject source = trace.toJson();
            JsonObject semantic = new JsonObject();
            for (String field : Arrays.asList("schemaVersion", "attemptId", "revision", "traceType",
                "eventId", "payloadHash", "recordedAt", "operation", "factKey", "targetVersionId",
                "targetResolution", "code", "validationCode", "errorMessage", "affectedFactKeyCount",
                "addedRelations", "removedRelations")) {
                semantic.add(field, source.get(field));
            }
            for (String field : Arrays.asList("rules", "readVersionIds", "closedVersionIds",
                "generatedVersionIds", "omittedVersionIds", "discardedVersionIds")) {
                TreeSet<String> values = new TreeSet<>();
                for (JsonElement value : source.getAsJsonArray(field)) {
                    Assertions.assertTrue(values.add(value.getAsString()), field);
                }
                JsonArray ordered = new JsonArray();
                for (String value : values) {
                    ordered.add(new JsonPrimitive(value));
                }
                semantic.add(field, ordered);
            }
            result.add(semantic);
        }
        return result;
    }

    private static final class GoldCase {

        private final Fixture fixture;
        private final RetrievalProfile profile;
        private final EventNormalizer normalizer = new EventNormalizer();
        private final Map<String, EventSpec> events = new HashMap<>();
        private final Map<String, VersionSpec> definitions = new HashMap<>();
        private final Map<String, NormalizedMemoryEvent> normalized = new HashMap<>();
        private final Map<String, String> identities = new HashMap<>();

        private GoldCase(Fixture fixture, RetrievalProfile profile) {
            this.fixture = fixture;
            this.profile = profile;
            Assertions.assertEquals(2, fixture.schemaVersion, fixture.name);
            for (EventSpec event : fixture.events) {
                Assertions.assertNull(events.put(event.alias, event), event.alias);
            }
            for (VersionSpec version : fixture.expected.identities) {
                Assertions.assertNull(definitions.put(version.alias, version), version.alias);
            }
            for (VersionSpec version : fixture.expected.versions) {
                Assertions.assertNull(definitions.put(version.alias, version), version.alias);
            }
        }

        private TemporalMemoryFacade verify() {
            IncrementalTemporalIntegrator incremental = new IncrementalTemporalIntegrator();
            FullReplayOracle oracle = new FullReplayOracle();
            TemporalMemoryFacade facade = new TemporalMemoryFacade();
            List<NormalizedMemoryEvent> committed = new ArrayList<>();
            int start = 0;
            for (BatchSpec batch : fixture.batches) {
                Assertions.assertTrue(batch.endExclusive > start
                    && batch.endExclusive <= fixture.events.size(), fixture.name);
                List<NormalizedMemoryEvent> attempted = new ArrayList<>();
                for (int index = start; index < batch.endExclusive; index++) {
                    attempted.add(normalize(fixture.events.get(index).alias));
                }
                TemporalState before = incremental.stateSnapshot();
                CanonicalSnapshot facadeBefore = facade.getSnapshot();
                if (batch.expectedFailure != null) {
                    TemporalValidationException error = Assertions.assertThrows(
                        TemporalValidationException.class, () -> incremental.applyBatch(attempted),
                        fixture.name);
                    Assertions.assertEquals(batch.expectedFailure, error.getCode().name(),
                        fixture.name);
                    Assertions.assertEquals(before, incremental.stateSnapshot(),
                        fixture.name + ": rejected batch changed committed state");
                    List<NormalizedMemoryEvent> rejectedReplay = new ArrayList<>(committed);
                    rejectedReplay.addAll(attempted);
                    TemporalValidationException oracleError = Assertions.assertThrows(
                        TemporalValidationException.class,
                        () -> oracle.replayNormalized(rejectedReplay), fixture.name);
                    Assertions.assertEquals(batch.expectedFailure, oracleError.getCode().name(),
                        fixture.name);
                    int traceCount = facade.getDecisionTraces().size();
                    TemporalValidationException facadeError = Assertions.assertThrows(
                        TemporalValidationException.class, () -> apply(facade, attempted),
                        fixture.name);
                    Assertions.assertEquals(batch.expectedFailure, facadeError.getCode().name());
                    Assertions.assertEquals(before, facade.getSnapshot().getState(), fixture.name);
                    Assertions.assertEquals(facadeBefore, facade.getSnapshot(), fixture.name);
                    Assertions.assertEquals(traceCount + attempted.size(),
                        facade.getDecisionTraces().size(), fixture.name);
                    for (DecisionTrace trace : facade.getDecisionTraces().subList(
                        traceCount, facade.getDecisionTraces().size())) {
                        JsonObject json = trace.toJson();
                        Assertions.assertEquals("BATCH_ROLLED_BACK", json.get("code").getAsString());
                        Assertions.assertEquals(batch.expectedFailure,
                            json.get("validationCode").getAsString());
                        Assertions.assertEquals(Collections.emptySet(), strings(json,
                            "generatedVersionIds"));
                    }
                } else {
                    List<EventLedgerDecision> decisions = incremental.applyBatch(attempted);
                    ReplayResult result = apply(facade, attempted);
                    attempted.sort(Comparator.comparing(NormalizedMemoryEvent::getRecordedAt)
                        .thenComparing(NormalizedMemoryEvent::getEventId));
                    Assertions.assertEquals(batch.decisions.size(), decisions.size(), fixture.name);
                    Assertions.assertEquals(batch.decisions.size(),
                        result.getDecisionTraces().size(), fixture.name);
                    for (int index = 0; index < decisions.size(); index++) {
                        DecisionSpec expected = batch.decisions.get(index);
                        Assertions.assertEquals(normalize(expected.event), attempted.get(index),
                            fixture.name + ": decision event order");
                        Assertions.assertEquals(expected.code, decisions.get(index).name(),
                            fixture.name + ": decision code");
                        JsonObject trace = result.getDecisionTraces().get(index).toJson();
                        Assertions.assertEquals(normalize(expected.event).getEventId(),
                            trace.get("eventId").getAsString(), fixture.name);
                        Assertions.assertEquals(expected.code, trace.get("code").getAsString(),
                            fixture.name);
                    }
                    committed.addAll(attempted);
                    Assertions.assertEquals(oracle.replayNormalized(committed),
                        incremental.stateSnapshot(), fixture.name + ": successful batch diff");
                    Assertions.assertEquals(incremental.stateSnapshot(),
                        facade.getSnapshot().getState(), fixture.name + ": facade batch diff");
                    Assertions.assertTrue(facade.validateInvariants().isValid(), fixture.name);
                }
                start = batch.endExclusive;
            }
            Assertions.assertEquals(fixture.events.size(), start, fixture.name);
            TemporalState expected = expectedState();
            Assertions.assertEquals(expected, incremental.stateSnapshot(),
                fixture.name + ": hand-authored incremental expectation");
            Assertions.assertEquals(expected, oracle.replayNormalized(committed),
                fixture.name + ": hand-authored Oracle expectation");
            Collections.reverse(committed);
            Assertions.assertEquals(expected, oracle.replayNormalized(committed),
                fixture.name + ": reversed input changes Oracle result");
            Assertions.assertEquals(expected, facade.getSnapshot().getState(), fixture.name);
            verifyTraces(facade);
            verifyQueries(incremental.stateSnapshot(), facade);
            return facade;
        }

        private ReplayResult apply(TemporalMemoryFacade facade,
                                     List<NormalizedMemoryEvent> attempted) {
            return profile == null ? facade.applyBatch(attempted)
                : facade.applyBatch(attempted, profile, 20);
        }

        private void verifyTraces(TemporalMemoryFacade facade) {
            Map<String, DecisionTrace> latestAccepted = new HashMap<>();
            Set<String> attemptIds = new HashSet<>();
            for (DecisionTrace trace : facade.getDecisionTraces()) {
                JsonObject json = trace.toJson();
                Assertions.assertTrue(attemptIds.add(trace.getId()), fixture.name);
                Assertions.assertEquals(trace.getId(), json.get("attemptId").getAsString());
                verifyRetrievalTrace(json);
                Assertions.assertTrue(json.has("revision"));
                if ("ACCEPTED".equals(json.get("code").getAsString())) {
                    latestAccepted.put(trace.getEventId(), trace);
                }
            }
            Assertions.assertEquals(facade.getSnapshot().getEvents().size(),
                latestAccepted.size(), fixture.name);
            for (NormalizedMemoryEvent event : facade.getSnapshot().getEvents()) {
                DecisionTrace trace = latestAccepted.get(event.getEventId());
                Assertions.assertNotNull(trace, fixture.name + ": missing accepted trace");
                Assertions.assertTrue(facade.getTrace(event.getEventId()).contains(trace));
                JsonObject json = trace.toJson();
                Assertions.assertEquals(event.getPayloadHash(),
                    json.get("payloadHash").getAsString());
                Assertions.assertEquals(event.getRecordedAt().toString(),
                    json.get("recordedAt").getAsString());
                if (profile != null) {
                    verifyEffectiveInterval(event, json);
                }
                Set<String> generated = new HashSet<>();
                Set<String> omitted = new HashSet<>();
                Set<String> closed = new HashSet<>();
                for (VersionSpec version : fixture.expected.versions) {
                    if (normalize(version.event).getEventId().equals(event.getEventId())) {
                        generated.add(identity(version.alias));
                        Assertions.assertEquals(event.getEventId(), facade.getSnapshot()
                            .getGeneratingEventIds().get(identity(version.alias)), fixture.name);
                    }
                    if (version.systemTo != null
                        && time(version.systemTo).equals(event.getRecordedAt())) {
                        closed.add(identity(version.alias));
                    }
                }
                for (VersionSpec version : fixture.expected.identities) {
                    if (normalize(version.event).getEventId().equals(event.getEventId())) {
                        omitted.add(identity(version.alias));
                    }
                }
                Assertions.assertEquals(generated, strings(json, "generatedVersionIds"),
                    fixture.name + ": materialized generated versions for " + event.getEventId());
                Assertions.assertEquals(omitted, strings(json, "omittedVersionIds"),
                    fixture.name + ": same-time omitted versions for " + event.getEventId());
                // A closed system interval is an observable transition at this event time.
                if (facade.getSnapshot().getEvents().stream().filter(candidate ->
                    candidate.getRecordedAt().equals(event.getRecordedAt())).count() == 1) {
                    Assertions.assertEquals(closed, strings(json, "closedVersionIds"),
                        fixture.name + ": closed versions for " + event.getEventId());
                }
                if (event.getTargetVersionId().isPresent()) {
                    Assertions.assertEquals(event.getTargetVersionId().get(),
                        json.get("targetVersionId").getAsString(), fixture.name);
                    Assertions.assertTrue(strings(json, "readVersionIds")
                        .contains(event.getTargetVersionId().get()), fixture.name);
                }
                Assertions.assertFalse(strings(json, "rules").isEmpty(), fixture.name);
            }
        }

        private void verifyRetrievalTrace(JsonObject json) {
            if (profile == null) {
                Assertions.assertEquals("NOT_APPLICABLE_WEEK6",
                    json.get("retrievalStatus").getAsString());
                Assertions.assertTrue(json.get("candidateScores").isJsonNull());
                Assertions.assertFalse(json.has("decision"));
                Assertions.assertFalse(json.has("retrievalProfile"));
                return;
            }
            Assertions.assertEquals(profile.name(), json.get("retrievalProfile").getAsString());
            Assertions.assertEquals(20, json.get("candidateTopK").getAsInt());
            String code = json.get("code").getAsString();
            if (!"ACCEPTED".equals(code)) {
                Assertions.assertEquals("DUPLICATE_NOOP".equals(code)
                    ? "NOT_EVALUATED_DUPLICATE" : "BATCH_NOT_COMMITTED",
                    json.get("retrievalStatus").getAsString());
                Assertions.assertTrue(json.get("candidateScores").isJsonNull());
                Assertions.assertTrue(json.get("decision").isJsonNull());
                Assertions.assertTrue(json.get("candidateEffectiveInterval").isJsonNull());
                for (String field : Arrays.asList("candidateVersionIds", "softCandidateVersionIds",
                    "readVersionIds", "relationReadVersionIds", "closedVersionIds",
                    "generatedVersionIds", "omittedVersionIds", "discardedVersionIds",
                    "addedRelations", "removedRelations")) {
                    Assertions.assertEquals(0, json.getAsJsonArray(field).size(),
                        fixture.name + ": uncommitted trace published " + field);
                }
                return;
            }
            Assertions.assertEquals("COMPLETED", json.get("retrievalStatus").getAsString());
            final Set<String> mandatory = strings(json, "candidateVersionIds");
            final Set<String> soft = strings(json, "softCandidateVersionIds");
            Assertions.assertTrue(Collections.disjoint(mandatory, soft), fixture.name);
            Assertions.assertTrue(soft.size() <= 20, fixture.name);
            if (profile == RetrievalProfile.RULE_ONLY) {
                Assertions.assertTrue(soft.isEmpty(), fixture.name);
            }
            final Set<String> scored = new HashSet<>();
            final Set<Integer> ranks = new HashSet<>();
            for (Map.Entry<String, JsonElement> entry
                : json.getAsJsonObject("candidateScores").entrySet()) {
                Assertions.assertTrue(scored.add(entry.getKey()), fixture.name);
                JsonObject detail = entry.getValue().getAsJsonObject();
                Assertions.assertEquals(mandatory.contains(entry.getKey()),
                    detail.get("mandatory").getAsBoolean(), fixture.name);
                int rank = detail.get("rank").getAsInt();
                Assertions.assertTrue(rank > 0 && ranks.add(rank), fixture.name);
                double score = detail.get("score").getAsDouble();
                Assertions.assertTrue(Double.isFinite(score) && score >= 0, fixture.name);
                JsonObject channels = detail.getAsJsonObject("scores");
                if (mandatory.contains(entry.getKey())) {
                    Assertions.assertEquals(1.0, channels.get("RULE").getAsDouble());
                    Assertions.assertTrue(detail.getAsJsonObject("channelRanks").has("RULE"));
                } else {
                    Assertions.assertFalse(channels.has("RULE"), fixture.name);
                }
                if (!profile.usesBm25()) {
                    Assertions.assertFalse(channels.has("BM25"), fixture.name);
                }
                if (!profile.usesMinHash()) {
                    Assertions.assertFalse(channels.has("MINHASH"), fixture.name);
                }
                for (Map.Entry<String, JsonElement> channel : channels.entrySet()) {
                    double channelScore = channel.getValue().getAsDouble();
                    Assertions.assertTrue(Double.isFinite(channelScore) && channelScore >= 0,
                        fixture.name);
                }
                for (Map.Entry<String, JsonElement> channel
                    : detail.getAsJsonObject("channelRanks").entrySet()) {
                    Assertions.assertTrue(channels.has(channel.getKey()), fixture.name);
                    Assertions.assertTrue(channel.getValue().getAsInt() > 0, fixture.name);
                }
            }
            Set<String> expectedScored = new HashSet<>(mandatory);
            expectedScored.addAll(soft);
            Assertions.assertEquals(expectedScored, scored, fixture.name);
            for (int rank = 1; rank <= scored.size(); rank++) {
                Assertions.assertTrue(ranks.contains(rank), fixture.name);
            }
            JsonObject decision = json.getAsJsonObject("decision");
            Assertions.assertEquals("COMMITTED", decision.get("status").getAsString());
            Assertions.assertEquals(mandatory, strings(decision, "mandatoryVersionIds"));
            Assertions.assertEquals(strings(json, "readVersionIds"),
                strings(decision, "readVersionIds"));
            Assertions.assertEquals(strings(json, "rules"), strings(decision, "rules"));
            Assertions.assertEquals(json.get("targetVersionId"), decision.get("targetVersionId"));
            Assertions.assertEquals(json.get("candidateEffectiveInterval"),
                decision.get("effectiveValidInterval"));
            Assertions.assertTrue(Collections.disjoint(soft, strings(json, "readVersionIds")),
                fixture.name + ": soft candidates became authoritative reads");
            for (JsonElement element : json.getAsJsonArray("addedRelations")) {
                JsonObject relation = element.getAsJsonObject();
                Assertions.assertFalse(soft.contains(relation.get("from").getAsString()),
                    fixture.name + ": soft candidate created a relation");
                Assertions.assertFalse(soft.contains(relation.get("to").getAsString()),
                    fixture.name + ": soft candidate created a relation");
            }
        }

        private void verifyEffectiveInterval(NormalizedMemoryEvent event, JsonObject json) {
            TimeInterval expected = null;
            if (event.getTargetVersionId().isPresent()) {
                for (VersionSpec version : definitions.values()) {
                    if (identity(version.alias).equals(event.getTargetVersionId().get())) {
                        expected = interval(version.validFrom, version.validTo)
                            .intersection(event.getValidTime()).get();
                    }
                }
            } else {
                for (VersionSpec version : definitions.values()) {
                    if (normalize(version.event).getEventId().equals(event.getEventId())) {
                        Assertions.assertNull(expected,
                            fixture.name + ": ambiguous hand-authored ASSERT interval");
                        expected = interval(version.validFrom, version.validTo);
                    }
                }
            }
            Assertions.assertNotNull(expected,
                fixture.name + ": no hand-authored resolution interval for " + event.getEventId());
            JsonObject actual = json.getAsJsonObject("candidateEffectiveInterval");
            Assertions.assertEquals(expected.getStart().toString(),
                actual.get("validFrom").getAsString(), fixture.name);
            Assertions.assertEquals(expected.getEnd().map(Instant::toString).orElse(null),
                actual.get("validTo").isJsonNull() ? null : actual.get("validTo").getAsString(),
                fixture.name);
        }

        private TemporalState expectedState() {
            Map<FactKey, List<MemoryFactVersion>> byKey = new TreeMap<>();
            for (VersionSpec spec : fixture.expected.versions) {
                FactKey key = new FactKey(spec.subject.id, spec.predicate, spec.scope);
                byKey.computeIfAbsent(key, ignored -> new ArrayList<>()).add(version(spec));
            }
            List<VersionRelation> relations = new ArrayList<>();
            for (RelationSpec spec : fixture.expected.relations) {
                relations.add(new VersionRelation(VersionRelationType.valueOf(spec.type),
                    identity(spec.from), identity(spec.to)));
            }
            return new TemporalState(byKey, relations);
        }

        private void verifyQueries(TemporalState state, TemporalMemoryFacade facade) {
            TemporalQueryService query = new TemporalQueryService();
            List<String> modes = new ArrayList<>();
            for (QuerySpec spec : fixture.expected.queries) {
                modes.add(spec.mode);
                FactKey key = new FactKey(spec.subjectId, spec.predicate, spec.scope);
                List<MemoryFactVersion> actual;
                if ("CURRENT".equals(spec.mode)) {
                    actual = query.current(state, key, time(spec.validAt));
                } else if ("HISTORICAL".equals(spec.mode)) {
                    actual = query.historical(state, key, time(spec.validAt));
                } else if ("AS_OF".equals(spec.mode)) {
                    actual = query.asOf(state, key, time(spec.validAt), time(spec.systemAt));
                } else {
                    throw new AssertionError("Unknown query mode: " + spec.mode);
                }
                List<MemoryFactVersion> expected = new ArrayList<>();
                for (String alias : spec.versions) {
                    Assertions.assertTrue(fixture.expected.versions.stream()
                        .anyMatch(item -> alias.equals(item.alias)),
                        "Query references an unmaterialized identity: " + alias);
                    expected.add(version(definitions.get(alias)));
                }
                expected.sort(QUERY_ORDER);
                Assertions.assertEquals(expected, actual,
                    fixture.name + ": " + spec.mode + " at " + spec.validAt
                        + " / " + spec.systemAt);
                QueryResult result = facade.query(QueryMode.valueOf(spec.mode), key,
                    time(spec.validAt), spec.systemAt == null ? null : time(spec.systemAt));
                Assertions.assertEquals(expected, result.getVersions(), fixture.name);
                Assertions.assertEquals(result.getTrace().toJson(),
                    facade.getQueryTrace(result.getTrace().getId()).toJson(), fixture.name);
                JsonObject trace = result.getTrace().toJson();
                Set<String> hits = new HashSet<>();
                Set<String> evidenceSources = new HashSet<>();
                Set<String> tombstones = new HashSet<>();
                for (VersionSpec version : fixture.expected.versions) {
                    if ("RETRACTED".equals(version.status)
                        && key.equals(new FactKey(version.subject.id, version.predicate,
                            version.scope))
                        && interval(version.validFrom, version.validTo).contains(time(spec.validAt))
                        && ("HISTORICAL".equals(spec.mode)
                            || ("CURRENT".equals(spec.mode) && version.systemTo == null)
                            || ("AS_OF".equals(spec.mode) && interval(version.systemFrom,
                                version.systemTo).contains(time(spec.systemAt))))) {
                        tombstones.add(identity(version.alias));
                    }
                }
                for (MemoryFactVersion version : expected) {
                    hits.add(version.getId());
                    for (Evidence item : version.getEvidence()) {
                        evidenceSources.add(version.getId() + "/" + item.getId() + "/"
                            + item.getSource().getId() + "/" + item.getSource().getName());
                    }
                }
                Assertions.assertEquals(hits, strings(trace, "hitVersionIds"), fixture.name);
                Assertions.assertEquals(tombstones, strings(trace, "tombstoneVersionIds"),
                    fixture.name + ": query tombstone visibility");
                Set<String> actualEvidence = new HashSet<>();
                for (JsonElement element : trace.getAsJsonArray("evidenceSources")) {
                    JsonObject item = element.getAsJsonObject();
                    actualEvidence.add(item.get("versionId").getAsString() + "/"
                        + item.get("evidenceId").getAsString() + "/"
                        + item.get("sourceId").getAsString() + "/"
                        + item.get("sourceName").getAsString());
                }
                Assertions.assertEquals(evidenceSources, actualEvidence,
                    fixture.name + ": query evidence must come only from hit versions");
                Assertions.assertFalse(trace.get("reason").getAsString().isEmpty());
                Assertions.assertFalse(trace.get("resultDigest").getAsString().isEmpty());
            }
            Assertions.assertTrue(modes.containsAll(Arrays.asList("CURRENT", "HISTORICAL", "AS_OF")),
                fixture.name + ": all three query modes are required");
        }

        private Set<String> strings(JsonObject json, String field) {
            Set<String> result = new HashSet<>();
            for (JsonElement value : json.getAsJsonArray(field)) {
                Assertions.assertTrue(result.add(value.getAsString()),
                    fixture.name + ": duplicate trace reference in " + field);
            }
            return result;
        }

        private NormalizedMemoryEvent normalize(String alias) {
            NormalizedMemoryEvent cached = normalized.get(alias);
            if (cached != null) {
                return cached;
            }
            EventSpec spec = events.get(alias);
            Assertions.assertNotNull(spec, "Unknown input alias: " + alias);
            MemoryEvent event;
            TimeInterval valid = interval(spec.validFrom, spec.validTo);
            List<Evidence> evidence = evidence(spec.evidence);
            if ("ASSERT".equals(spec.operation)) {
                event = MemoryEvent.add(spec.id, fact(spec), valid, time(spec.recordedAt), evidence);
            } else if ("CORRECT".equals(spec.operation)) {
                event = MemoryEvent.correct(spec.id, fact(spec), valid,
                    time(spec.recordedAt), evidence);
            } else if ("RETRACT".equals(spec.operation)) {
                event = MemoryEvent.retract(spec.id, spec.factId, valid,
                    time(spec.recordedAt), evidence);
            } else {
                throw new AssertionError("Unknown operation: " + spec.operation);
            }
            if (spec.target != null) {
                event = event.targeting(definitions.containsKey(spec.target)
                    ? identity(spec.target) : spec.target);
            }
            NormalizedMemoryEvent result = normalizer.normalize(event,
                new FactKey(spec.subject.id, spec.predicate, spec.scope));
            normalized.put(alias, result);
            return result;
        }

        private String identity(String alias) {
            String cached = identities.get(alias);
            if (cached != null) {
                return cached;
            }
            VersionSpec spec = definitions.get(alias);
            Assertions.assertNotNull(spec, "Unknown version alias: " + alias);
            String id = VersionIdentity.create(normalize(spec.event), fact(spec),
                MemoryFactVersionStatus.valueOf(spec.status),
                interval(spec.validFrom, spec.validTo),
                spec.sourceVersion == null ? null : identity(spec.sourceVersion));
            identities.put(alias, id);
            return id;
        }

        private MemoryFactVersion version(VersionSpec spec) {
            return new MemoryFactVersion(identity(spec.alias), fact(spec),
                MemoryFactVersionStatus.valueOf(spec.status),
                interval(spec.validFrom, spec.validTo),
                interval(spec.systemFrom, spec.systemTo), evidence(spec.evidence));
        }
    }

    private static MemoryFact fact(FactSpec spec) {
        MemoryEntity subject = new MemoryEntity(spec.subject.id, spec.subject.label);
        if ("ENTITY_REF".equals(spec.value.kind)) {
            return MemoryFact.relationship(spec.factId, subject, spec.predicate,
                new MemoryEntity(spec.value.value, spec.value.label));
        }
        Assertions.assertEquals("LITERAL", spec.value.kind);
        return MemoryFact.attribute(spec.factId, subject, spec.predicate, spec.value.value);
    }

    private static List<Evidence> evidence(List<EvidenceSpec> specs) {
        List<Evidence> evidence = new ArrayList<>();
        for (EvidenceSpec spec : specs) {
            evidence.add(new Evidence(spec.id, new Source(spec.source.id, spec.source.name),
                spec.content));
        }
        return evidence;
    }

    private static Instant time(String value) {
        return Instant.parse(value);
    }

    private static TimeInterval interval(String start, String end) {
        return new TimeInterval(time(start), end == null ? null : time(end));
    }

    private static final class Fixture {
        private int schemaVersion;
        private String name;
        private List<EventSpec> events;
        private List<BatchSpec> batches;
        private ExpectedSpec expected;
    }

    private static class FactSpec {
        String factId;
        EntitySpec subject;
        String predicate;
        String scope;
        ValueSpec value;
    }

    private static class TimedFactSpec extends FactSpec {
        String alias;
        String validFrom;
        String validTo;
        List<EvidenceSpec> evidence;
    }

    private static final class EventSpec extends TimedFactSpec {
        private String id;
        private String operation;
        private String recordedAt;
        private String target;
    }

    private static final class VersionSpec extends TimedFactSpec {
        private String event;
        private String sourceVersion;
        private String status;
        private String systemFrom;
        private String systemTo;
    }

    private static final class EntitySpec {
        private String id;
        private String label;
    }

    private static final class ValueSpec {
        private String kind;
        private String value;
        private String label;
    }

    private static final class SourceSpec {
        private String id;
        private String name;
    }

    private static final class EvidenceSpec {
        private String id;
        private SourceSpec source;
        private String content;
    }

    private static final class BatchSpec {
        private int endExclusive;
        private String expectedFailure;
        private List<DecisionSpec> decisions;
    }

    private static final class DecisionSpec {
        private String event;
        private String code;
    }

    private static final class ExpectedSpec {
        private List<VersionSpec> identities;
        private List<VersionSpec> versions;
        private List<RelationSpec> relations;
        private List<QuerySpec> queries;
    }

    private static final class RelationSpec {
        private String type;
        private String from;
        private String to;
    }

    private static final class QuerySpec {
        private String mode;
        private String subjectId;
        private String predicate;
        private String scope;
        private String validAt;
        private String systemAt;
        private List<String> versions;
    }
}
