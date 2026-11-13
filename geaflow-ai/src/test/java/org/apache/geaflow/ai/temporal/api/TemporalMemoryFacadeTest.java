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

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.apache.geaflow.ai.temporal.codec.TemporalJsonCodec;
import org.apache.geaflow.ai.temporal.model.Evidence;
import org.apache.geaflow.ai.temporal.model.FactKey;
import org.apache.geaflow.ai.temporal.model.MemoryEntity;
import org.apache.geaflow.ai.temporal.model.MemoryEvent;
import org.apache.geaflow.ai.temporal.model.MemoryFact;
import org.apache.geaflow.ai.temporal.model.MemoryFactVersionStatus;
import org.apache.geaflow.ai.temporal.model.Source;
import org.apache.geaflow.ai.temporal.model.TimeInterval;
import org.apache.geaflow.ai.temporal.oracle.FullReplayOracle;
import org.apache.geaflow.ai.temporal.semantics.CanonicalSnapshot;
import org.apache.geaflow.ai.temporal.semantics.EventNormalizer;
import org.apache.geaflow.ai.temporal.semantics.NormalizedMemoryEvent;
import org.apache.geaflow.ai.temporal.semantics.TemporalErrorCode;
import org.apache.geaflow.ai.temporal.semantics.TemporalValidationException;
import org.apache.geaflow.ai.temporal.semantics.VersionIdentity;
import org.apache.geaflow.ai.temporal.trace.DecisionTrace;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TemporalMemoryFacadeTest {

    private static final Instant BASE = Instant.parse("2024-01-01T00:00:00Z");
    private static final FactKey KEY = new FactKey("alice", "city", "profile");
    private final EventNormalizer normalizer = new EventNormalizer();

    @Test
    public void testReplayReplacesOnlyAfterTheWholeInputSucceeds() {
        TemporalMemoryFacade facade = new TemporalMemoryFacade();
        NormalizedMemoryEvent original = add("original", "Beijing", 10);
        CanonicalSnapshot before = facade.applyBatch(Collections.singletonList(original))
            .getSnapshot();
        NormalizedMemoryEvent staged = add("staged", "Shanghai", 20);
        NormalizedMemoryEvent invalid = correct("invalid", 30, "absent-version");

        TemporalValidationException error = Assertions.assertThrows(
            TemporalValidationException.class,
            () -> facade.replay(Arrays.asList(staged, invalid)));

        Assertions.assertEquals(TemporalErrorCode.REJECT_TARGET_NOT_FOUND, error.getCode());
        Assertions.assertEquals(before, facade.getSnapshot());
        Assertions.assertTrue(facade.validateInvariants().isValid());
        ReplayResult replaced = facade.replay(Collections.singletonList(staged));
        Assertions.assertEquals(Collections.singletonList(staged), replaced.getSnapshot().getEvents());
        Assertions.assertEquals(new FullReplayOracle().replayToSnapshot(
            Collections.singletonList(staged)), replaced.getSnapshot());
        Assertions.assertEquals("Shanghai", facade.query(QueryMode.CURRENT, KEY, at(1), null)
            .getVersions().get(0).getFact().getLiteralValue().get());
        Assertions.assertFalse(replaced.getMethod().toString().isEmpty());
        Assertions.assertFalse(replaced.getStatistics().isEmpty());
        Assertions.assertNotNull(replaced.getFallbackStatus());
    }

    @Test
    public void testLateSameTimeInputRevisesPriorMaterializationAndPreservesAttempt() {
        TemporalMemoryFacade facade = new TemporalMemoryFacade();
        NormalizedMemoryEvent first = add("a", "Beijing", 10);
        String target = VersionIdentity.create(first, first.getEvent().getFact().get(),
            MemoryFactVersionStatus.ACTIVE, first.getValidTime(), null);
        facade.applyBatch(Collections.singletonList(first));
        DecisionTrace firstAttempt = latestAccepted(facade, "a");
        Assertions.assertEquals(target, firstAttempt.toJson().getAsJsonArray(
            "generatedVersionIds").get(0).getAsString());
        NormalizedMemoryEvent second = correct("b", 10, target);

        facade.applyBatch(Collections.singletonList(second));

        Assertions.assertEquals(1, facade.getSnapshot().getState().getVersions().size());
        Assertions.assertTrue(facade.getSnapshot().getState().getRelations().isEmpty());
        Assertions.assertTrue(facade.getTrace("a").contains(firstAttempt));
        JsonObject revised = latestAccepted(facade, "a").toJson();
        Assertions.assertTrue(revised.get("revision").getAsLong()
            > firstAttempt.toJson().get("revision").getAsLong());
        Assertions.assertEquals(0, revised.getAsJsonArray("generatedVersionIds").size());
        Assertions.assertEquals(target, revised.getAsJsonArray("omittedVersionIds")
            .get(0).getAsString());
        Assertions.assertTrue(facade.validateInvariants().getViolations().isEmpty());
    }

    @Test
    public void testDuplicateAndRejectedAttemptsDoNotConsumeCommittedIdentity() {
        TemporalMemoryFacade facade = new TemporalMemoryFacade();
        NormalizedMemoryEvent original = add("original", "Beijing", 10);
        facade.applyBatch(Collections.singletonList(original));
        CanonicalSnapshot before = facade.getSnapshot();
        String acceptedId = latestAccepted(facade, "original").getId();
        ReplayResult duplicate = facade.applyBatch(Collections.singletonList(original));
        Assertions.assertEquals("DUPLICATE_NOOP",
            duplicate.getDecisionTraces().get(0).toJson().get("code").getAsString());
        Assertions.assertEquals(acceptedId, latestAccepted(facade, "original").getId());
        Assertions.assertEquals(before, facade.getSnapshot());

        NormalizedMemoryEvent staged = add("retry", "Shanghai", 20);
        Assertions.assertThrows(TemporalValidationException.class, () -> facade.applyBatch(
            Arrays.asList(staged, correct("invalid", 30, "absent-version"))));
        Assertions.assertEquals(before, facade.getSnapshot());
        Assertions.assertEquals("BATCH_ROLLED_BACK",
            facade.getTrace("retry").get(0).toJson().get("code").getAsString());
        NormalizedMemoryEvent changedRetry = add("retry", "Tianjin", 20);
        ReplayResult retry = facade.applyBatch(Collections.singletonList(changedRetry));
        Assertions.assertEquals("ACCEPTED",
            retry.getDecisionTraces().get(0).toJson().get("code").getAsString());
        Assertions.assertEquals(2, facade.getSnapshot().getEvents().size());
    }

    @Test
    public void testEmptyAndUnknownQueriesHaveStableTraceAndNoFutureEvidence() {
        TemporalMemoryFacade facade = new TemporalMemoryFacade();
        QueryResult empty = facade.query(QueryMode.CURRENT, KEY, at(1), null);
        Assertions.assertTrue(empty.getVersions().isEmpty());
        Assertions.assertEquals(empty.getTrace().toJson(),
            facade.getQueryTrace(empty.getTrace().getId()).toJson());
        facade.applyBatch(Collections.singletonList(add("a", "Beijing", 10)));
        QueryResult past = facade.query(QueryMode.AS_OF, KEY, at(1), at(9));
        Assertions.assertTrue(past.getVersions().isEmpty());
        Assertions.assertEquals(0,
            past.getTrace().toJson().getAsJsonArray("evidenceSources").size());
        QueryResult unknown = facade.query(QueryMode.CURRENT,
            new FactKey("alice", "city", "missing"), at(1), null);
        Assertions.assertTrue(unknown.getVersions().isEmpty());
        Assertions.assertEquals(3, facade.getQueryTraces().size());
        Assertions.assertTrue(facade.getTrace("unknown-event").isEmpty());
    }

    @Test
    public void testPublicCityExampleAgainstHandAuthoredQueryExpectations() throws Exception {
        TemporalJsonCodec codec = new TemporalJsonCodec();
        List<NormalizedMemoryEvent> events = new ArrayList<>();
        for (String line : resourceLines("example-events.jsonl")) {
            events.add(codec.decodeEvent(line));
        }
        Assertions.assertEquals(5, events.size());
        NormalizedMemoryEvent supplement = events.get(1);
        String mergedId = VersionIdentity.create(supplement,
            supplement.getEvent().getFact().get(), MemoryFactVersionStatus.ACTIVE,
            events.get(0).getValidTime(), null);
        Assertions.assertEquals(mergedId, events.get(2).getTargetVersionId().get(),
            "The correction targets the supplemented Beijing branch");
        NormalizedMemoryEvent correction = events.get(2);
        String shanghaiId = VersionIdentity.create(correction,
            correction.getEvent().getFact().get(), MemoryFactVersionStatus.ACTIVE,
            correction.getValidTime(), mergedId);
        Assertions.assertEquals(shanghaiId, events.get(4).getTargetVersionId().get(),
            "The retraction targets Shanghai, leaving the Tianjin conflict intact");

        TemporalMemoryFacade facade = new TemporalMemoryFacade();
        ReplayResult result = facade.replay(events);
        Assertions.assertEquals(5, result.getSnapshot().getEvents().size());
        Assertions.assertEquals(9, result.getSnapshot().getState().getVersions().size());
        Assertions.assertTrue(facade.validateInvariants().isValid());
        int queries = 0;
        for (String line : resourceLines("example-queries.jsonl")) {
            JsonObject expected = codec.parseObject(line);
            QueryResult actual = facade.query(QueryMode.valueOf(expected.get("mode").getAsString()),
                KEY, Instant.parse(expected.get("validAt").getAsString()),
                expected.get("systemAt").isJsonNull() ? null
                    : Instant.parse(expected.get("systemAt").getAsString()));
            List<String> expectedValues = new ArrayList<>();
            for (JsonElement value : expected.getAsJsonArray("values")) {
                expectedValues.add(value.getAsString());
            }
            List<String> actualValues = new ArrayList<>();
            Set<String> actualEvidence = new TreeSet<>();
            actual.getVersions().forEach(version -> {
                actualValues.add(version.getFact().getLiteralValue().get());
                version.getEvidence().forEach(item -> actualEvidence.add(item.getId()));
            });
            Collections.sort(actualValues);
            Assertions.assertEquals(expectedValues, actualValues, line);
            List<String> expectedEvidence = new ArrayList<>();
            for (JsonElement value : expected.getAsJsonArray("evidenceIds")) {
                expectedEvidence.add(value.getAsString());
            }
            Assertions.assertEquals(expectedEvidence, new ArrayList<>(actualEvidence), line);
            queries++;
        }
        Assertions.assertEquals(13, queries);
    }

    private List<String> resourceLines(String name) throws Exception {
        List<String> lines = new ArrayList<>();
        try (InputStream input = getClass().getResourceAsStream("/temporal/" + name)) {
            Assertions.assertNotNull(input, name);
            try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(input, StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    lines.add(line);
                }
            }
        }
        return lines;
    }

    private DecisionTrace latestAccepted(TemporalMemoryFacade facade, String eventId) {
        List<DecisionTrace> traces = facade.getTrace(eventId);
        for (int index = traces.size() - 1; index >= 0; index--) {
            DecisionTrace trace = traces.get(index);
            if ("ACCEPTED".equals(trace.toJson().get("code").getAsString())) {
                return trace;
            }
        }
        throw new AssertionError("No accepted trace for " + eventId);
    }

    private NormalizedMemoryEvent add(String id, String value, long recordedAt) {
        return normalizer.normalize(MemoryEvent.add(id, fact(value), interval(), at(recordedAt),
            evidence(id)), KEY);
    }

    private NormalizedMemoryEvent correct(String id, long recordedAt, String target) {
        return normalizer.normalize(MemoryEvent.correct(id, fact("Shanghai"), interval(),
            at(recordedAt), evidence(id)).targeting(target), KEY);
    }

    private static MemoryFact fact(String value) {
        return MemoryFact.attribute("fact-city", new MemoryEntity("alice", "person"),
            "city", value);
    }

    private static List<Evidence> evidence(String id) {
        return Collections.singletonList(new Evidence("evidence-" + id,
            new Source("registry", "Registry"), "Evidence for " + id));
    }

    private static TimeInterval interval() {
        return new TimeInterval(BASE, at(5));
    }

    private static Instant at(long nanos) {
        return BASE.plusNanos(nanos);
    }
}
