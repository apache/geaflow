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
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.geaflow.ai.temporal.candidate.RetrievalProfile;
import org.apache.geaflow.ai.temporal.integration.IncrementalTemporalIntegrator;
import org.apache.geaflow.ai.temporal.model.Evidence;
import org.apache.geaflow.ai.temporal.model.FactKey;
import org.apache.geaflow.ai.temporal.model.MemoryEntity;
import org.apache.geaflow.ai.temporal.model.MemoryEvent;
import org.apache.geaflow.ai.temporal.model.MemoryFact;
import org.apache.geaflow.ai.temporal.model.MemoryFactVersionStatus;
import org.apache.geaflow.ai.temporal.model.Source;
import org.apache.geaflow.ai.temporal.model.TimeInterval;
import org.apache.geaflow.ai.temporal.semantics.EventNormalizer;
import org.apache.geaflow.ai.temporal.semantics.NormalizedMemoryEvent;
import org.apache.geaflow.ai.temporal.semantics.TemporalState;
import org.apache.geaflow.ai.temporal.semantics.VersionIdentity;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CandidateDecisionTraceTest {

    private static final Instant BASE = Instant.parse("2024-01-01T00:00:00Z");
    private static final FactKey KEY = new FactKey("alice", "city", "profile");
    private static final String EVIDENCE = "climate temperature increases across the planet each year";
    private static final List<RetrievalProfile> PROFILES = Arrays.asList(RetrievalProfile.RULE_ONLY,
        RetrievalProfile.RULE_BM25, RetrievalProfile.RULE_MINHASH, RetrievalProfile.RULE_ALL);

    @Test
    public void testStrategiesKeepSemanticDeltasAndSoftCandidatesCannotCreateRelations() {
        final NormalizedMemoryEvent first = assertion("first", KEY, "Paris", 0, 100, 1);
        final NormalizedMemoryEvent soft = assertion("soft", new FactKey("bob", "city", "profile"),
            "Paris", 0, 100, 2);
        final NormalizedMemoryEvent conflict = assertion("conflict", KEY, "Berlin", 0, 100, 3);
        final List<NormalizedMemoryEvent> events = Arrays.asList(first, soft, conflict);
        final JsonObject legacy = TraceBuilder.replay(events).get("conflict");
        final String softId = versionId(soft);
        final String firstId = versionId(first);
        JsonObject ruleScores = null;
        for (RetrievalProfile profile : PROFILES) {
            Map<String, JsonObject> traces = TraceBuilder.replay(events, profile, 1);
            Assertions.assertTrue(values(traces.get("first"), "candidateVersionIds").isEmpty());
            Assertions.assertTrue(values(traces.get("first"), "softCandidateVersionIds").isEmpty());
            Assertions.assertTrue(traces.get("first").getAsJsonObject("candidateScores")
                .entrySet().isEmpty());
            JsonObject trace = traces.get("conflict");
            assertDeltasEqual(legacy, trace);
            Assertions.assertEquals(Collections.singletonList(firstId), values(trace,
                "candidateVersionIds"));
            Assertions.assertEquals(Collections.singletonList(firstId), values(trace,
                "readVersionIds"));
            Assertions.assertEquals("COMPLETED", trace.get("retrievalStatus").getAsString());
            Assertions.assertEquals(profile.name(), trace.get("retrievalProfile").getAsString());
            Assertions.assertEquals("COMMITTED", trace.getAsJsonObject("decision")
                .get("status").getAsString());
            JsonObject scores = trace.getAsJsonObject("candidateScores");
            Assertions.assertTrue(scores.getAsJsonObject(firstId).get("mandatory").getAsBoolean());
            if (profile.usesBm25()) {
                Assertions.assertEquals(Collections.singletonList(softId), values(trace,
                    "softCandidateVersionIds"));
                Assertions.assertFalse(scores.getAsJsonObject(softId).get("mandatory")
                    .getAsBoolean());
                Assertions.assertFalse(values(trace, "readVersionIds").contains(softId));
                Assertions.assertFalse(trace.get("addedRelations").toString().contains(softId));
                Assertions.assertTrue(scores.getAsJsonObject(firstId).getAsJsonObject("scores")
                    .has("BM25"));
            }
            if (profile == RetrievalProfile.RULE_ONLY) {
                ruleScores = scores;
                Assertions.assertTrue(values(trace, "softCandidateVersionIds").isEmpty());
            } else if (profile.usesBm25()) {
                Assertions.assertNotEquals(ruleScores, scores);
            }
        }
        Assertions.assertEquals("NOT_APPLICABLE_WEEK6", legacy.get("retrievalStatus").getAsString());
        Assertions.assertTrue(legacy.get("candidateScores").isJsonNull());
        Assertions.assertFalse(legacy.has("decision"));
        Assertions.assertFalse(legacy.has("softCandidateVersionIds"));
    }

    @Test
    public void testMergedEffectiveSpanKeepsMoreThanTopKMandatoryCandidates() {
        final List<NormalizedMemoryEvent> events = new ArrayList<>();
        events.add(assertion("paris", KEY, "Paris", 0, 100, 1));
        for (int i = 0; i < 25; i++) {
            events.add(assertion("conflict-" + i, KEY, "City-" + i, 0, 80, i + 2));
        }
        events.add(assertion("supplement", KEY, "Paris", 90, 95, 30));
        JsonObject trace = TraceBuilder.replay(events, RetrievalProfile.RULE_ALL, 1)
            .get("supplement");
        Assertions.assertEquals(26, trace.getAsJsonArray("candidateVersionIds").size());
        Assertions.assertEquals(26, trace.getAsJsonArray("readVersionIds").size());
        Assertions.assertEquals(26, trace.getAsJsonObject("candidateScores").entrySet().size());
        Assertions.assertEquals(BASE.toString(), trace.getAsJsonObject("candidateEffectiveInterval")
            .get("validFrom").getAsString());
        Assertions.assertEquals(BASE.plusSeconds(100).toString(),
            trace.getAsJsonObject("candidateEffectiveInterval").get("validTo").getAsString());
        Assertions.assertTrue(values(trace, "rules").contains("MERGE_EQUAL_VALUE"));
        Assertions.assertTrue(values(trace, "rules").contains("COEXIST_CONFLICT"));
    }

    @Test
    public void testCorrectionUsesTargetIntersectionAndOnlyTargetIsAuthoritativeRead() {
        final NormalizedMemoryEvent first = assertion("first", KEY, "Paris", 0, 100, 1);
        final NormalizedMemoryEvent conflict = assertion("conflict", KEY, "Berlin", 0, 100, 2);
        final String target = versionId(first);
        final MemoryFact corrected = fact("corrected", KEY, "London");
        final NormalizedMemoryEvent correction = new EventNormalizer().normalize(
            MemoryEvent.correct("correction", corrected, interval(50, 120), BASE.plusSeconds(3),
                evidence("correction")).targeting(target), KEY);
        final List<NormalizedMemoryEvent> events = Arrays.asList(first, conflict, correction);
        final JsonObject trace = TraceBuilder.replay(events, RetrievalProfile.RULE_BM25, 1)
            .get("correction");
        Assertions.assertEquals(2, trace.getAsJsonArray("candidateVersionIds").size());
        Assertions.assertEquals(Collections.singletonList(target), values(trace, "readVersionIds"));
        Assertions.assertEquals(Collections.singletonList(target), values(trace, "closedVersionIds"));
        Assertions.assertEquals(2, trace.getAsJsonArray("generatedVersionIds").size());
        Assertions.assertEquals(Collections.singletonList("CORRECT_TARGET"), values(trace, "rules"));
        Assertions.assertEquals(BASE.plusSeconds(100).toString(),
            trace.getAsJsonObject("candidateEffectiveInterval").get("validTo").getAsString());
        Assertions.assertEquals(target, trace.getAsJsonObject("decision").get("targetVersionId")
            .getAsString());
        Assertions.assertTrue(values(trace, "relationReadVersionIds").contains(target));
        Assertions.assertTrue(values(trace, "relationReadVersionIds").contains(versionId(conflict)));
        Assertions.assertEquals("EXISTING_ENDPOINTS_OF_ADDED_RELATIONS",
            trace.get("relationReadSource").getAsString());
        IncrementalTemporalIntegrator integrator = new IncrementalTemporalIntegrator();
        integrator.apply(first);
        integrator.apply(conflict);
        TemporalState before = integrator.stateSnapshot();
        integrator.apply(correction);
        TemporalState after = integrator.stateSnapshot();
        Assertions.assertEquals(before.getVersions().size() + 2, after.getVersions().size());
        Assertions.assertEquals(after.getRelations().size() - before.getRelations().size(),
            trace.getAsJsonArray("addedRelations").size());
        Assertions.assertEquals(0, trace.getAsJsonArray("removedRelations").size());
    }

    @Test
    public void testRetractionReportsTombstoneAndResidualSupersedesWithoutSoftReads() {
        final NormalizedMemoryEvent first = assertion("first", KEY, "Paris", 0, 100, 1);
        final String target = versionId(first);
        final NormalizedMemoryEvent retract = new EventNormalizer().normalize(MemoryEvent.retract(
            "retract", first.getFactId(), interval(20, 80), BASE.plusSeconds(2), evidence("retract"))
            .targeting(target), KEY);
        JsonObject trace = TraceBuilder.replay(Arrays.asList(first, retract),
            RetrievalProfile.RULE_ALL, 20).get("retract");
        Assertions.assertEquals(Collections.singletonList(target), values(trace, "readVersionIds"));
        Assertions.assertEquals(Collections.singletonList(target), values(trace, "closedVersionIds"));
        Assertions.assertEquals(Collections.singletonList("RETRACT_TARGET"), values(trace, "rules"));
        Assertions.assertEquals(3, trace.getAsJsonArray("generatedVersionIds").size());
        Assertions.assertEquals(3, trace.getAsJsonArray("addedRelations").size());
        Assertions.assertEquals(Collections.singletonList(target),
            values(trace, "relationReadVersionIds"));
        Assertions.assertEquals(0, trace.getAsJsonArray("softCandidateVersionIds").size());
    }

    @Test
    public void testSameTimestampDiscardedVersionRemainsInDecisionButGeneratedIsOmitted() {
        final NormalizedMemoryEvent first = assertion("a-first", KEY, "Paris", 0, 100, 1);
        final NormalizedMemoryEvent second = assertion("b-second", KEY, "Paris", 20, 30, 1);
        Map<String, JsonObject> traces = TraceBuilder.replay(Arrays.asList(second, first),
            RetrievalProfile.RULE_ONLY, 20);
        Assertions.assertTrue(values(traces.get("a-first"), "generatedVersionIds").isEmpty());
        Assertions.assertEquals(Collections.singletonList(versionId(first)),
            values(traces.get("a-first"), "omittedVersionIds"));
        Assertions.assertEquals(Collections.singletonList(versionId(first)),
            values(traces.get("b-second"), "discardedVersionIds"));
        Assertions.assertEquals(Collections.singletonList(versionId(first)),
            values(traces.get("b-second"), "readVersionIds"));
        Assertions.assertTrue(traces.get("b-second").getAsJsonArray("addedRelations").size() == 0);
    }

    @Test
    public void testDuplicateAndRolledBackAttemptsDoNotPublishCandidateDecision() {
        final NormalizedMemoryEvent event = assertion("first", KEY, "Paris", 0, 100, 1);
        final JsonObject semantic = TraceBuilder.replay(Collections.singletonList(event),
            RetrievalProfile.RULE_ALL, 20).get("first");
        for (String code : Arrays.asList("DUPLICATE_NOOP", "REJECT_TARGET_NOT_FOUND")) {
            JsonObject trace = TraceBuilder.decision(event, semantic, "attempt", 2, "INPUT", code,
                code, null, RetrievalProfile.RULE_ALL, 20).toJson();
            Assertions.assertEquals("DUPLICATE_NOOP".equals(code)
                ? "NOT_EVALUATED_DUPLICATE" : "BATCH_NOT_COMMITTED",
                trace.get("retrievalStatus").getAsString());
            Assertions.assertTrue(trace.get("decision").isJsonNull());
            Assertions.assertTrue(trace.get("candidateScores").isJsonNull());
            Assertions.assertTrue(values(trace, "candidateVersionIds").isEmpty());
            Assertions.assertTrue(values(trace, "generatedVersionIds").isEmpty());
        }
        Assertions.assertThrows(IllegalArgumentException.class, () -> TraceBuilder.replay(
            Collections.singletonList(event), RetrievalProfile.ENGLISH, 20));
        Assertions.assertThrows(IllegalArgumentException.class, () -> TraceBuilder.replay(
            Collections.singletonList(event), RetrievalProfile.RULE_ALL, 0));
    }

    private static void assertDeltasEqual(JsonObject expected, JsonObject actual) {
        for (String field : Arrays.asList("rules", "closedVersionIds", "generatedVersionIds",
            "omittedVersionIds", "discardedVersionIds", "addedRelations", "removedRelations")) {
            Assertions.assertEquals(expected.get(field), actual.get(field), field);
        }
    }

    private static NormalizedMemoryEvent assertion(String id, FactKey key, String value,
                                                    int from, int to, int recordedAt) {
        return new EventNormalizer().normalize(MemoryEvent.add(id, fact(id, key, value),
            interval(from, to), BASE.plusSeconds(recordedAt), evidence(id)), key);
    }

    private static MemoryFact fact(String id, FactKey key, String value) {
        return MemoryFact.attribute("fact-" + id, new MemoryEntity(key.getSubjectId(), "person"),
            key.getPredicate(), value);
    }

    private static List<Evidence> evidence(String id) {
        return Collections.singletonList(new Evidence("proof-" + id,
            new Source("source", "Registry"), EVIDENCE));
    }

    private static TimeInterval interval(int from, int to) {
        return new TimeInterval(BASE.plusSeconds(from), BASE.plusSeconds(to));
    }

    private static String versionId(NormalizedMemoryEvent event) {
        return VersionIdentity.create(event, event.getEvent().getFact().get(),
            MemoryFactVersionStatus.ACTIVE, event.getValidTime(), null);
    }

    private static List<String> values(JsonObject json, String field) {
        JsonArray array = json.getAsJsonArray(field);
        List<String> result = new ArrayList<>();
        for (JsonElement value : array) {
            result.add(value.getAsString());
        }
        return result;
    }
}
