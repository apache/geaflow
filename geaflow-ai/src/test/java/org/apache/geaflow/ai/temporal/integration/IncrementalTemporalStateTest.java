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

package org.apache.geaflow.ai.temporal.integration;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.geaflow.ai.temporal.model.Evidence;
import org.apache.geaflow.ai.temporal.model.FactKey;
import org.apache.geaflow.ai.temporal.model.MemoryEntity;
import org.apache.geaflow.ai.temporal.model.MemoryEvent;
import org.apache.geaflow.ai.temporal.model.MemoryFact;
import org.apache.geaflow.ai.temporal.model.MemoryFactVersion;
import org.apache.geaflow.ai.temporal.model.MemoryFactVersionStatus;
import org.apache.geaflow.ai.temporal.model.Source;
import org.apache.geaflow.ai.temporal.model.TimeInterval;
import org.apache.geaflow.ai.temporal.oracle.FullReplayOracle;
import org.apache.geaflow.ai.temporal.semantics.EventNormalizer;
import org.apache.geaflow.ai.temporal.semantics.NormalizedMemoryEvent;
import org.apache.geaflow.ai.temporal.semantics.TemporalState;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class IncrementalTemporalStateTest {

    private static final FactKey ALICE_KEY = new FactKey(
        "person:alice",
        "city",
        "profile");
    private static final FactKey BOB_KEY = new FactKey(
        "person:bob",
        "city",
        "profile");

    private final EventNormalizer normalizer = new EventNormalizer();
    private final FullReplayOracle oracle = new FullReplayOracle();

    @Test
    public void testLateEventMatchesOracleAndKeepsOtherFactKey() {
        IncrementalTemporalIntegrator integrator =
            new IncrementalTemporalIntegrator();
        TimeInterval validTime = interval(
            "2024-01-01T00:00:00Z",
            "2025-01-01T00:00:00Z");
        NormalizedMemoryEvent aliceAdd = add(
            "event-alice-add",
            ALICE_KEY,
            "Beijing",
            validTime,
            "2024-03-01T00:00:00Z");
        NormalizedMemoryEvent bobAdd = add(
            "event-bob-add",
            BOB_KEY,
            "Paris",
            validTime,
            "2024-04-01T00:00:00Z");
        NormalizedMemoryEvent laterCorrection = correct(
            "event-alice-later",
            ALICE_KEY,
            "Rome",
            validTime,
            "2024-08-01T00:00:00Z");
        NormalizedMemoryEvent retraction = retract(
            "event-alice-retract",
            ALICE_KEY,
            validTime,
            "2024-10-01T00:00:00Z");
        NormalizedMemoryEvent lateCorrection = correct(
            "event-alice-late",
            ALICE_KEY,
            "Shanghai",
            validTime,
            "2024-06-01T00:00:00Z");
        List<NormalizedMemoryEvent> received = new ArrayList<>();

        for (NormalizedMemoryEvent event : Arrays.asList(
            aliceAdd,
            bobAdd,
            laterCorrection,
            retraction)) {
            integrator.apply(event);
            received.add(event);
            Assertions.assertEquals(
                oracle.replayNormalized(received),
                integrator.stateSnapshot());
        }

        List<MemoryFactVersion> bobBefore =
            integrator.stateSnapshot()
                .getVersionsByFactKey().get(BOB_KEY);
        integrator.apply(lateCorrection);
        received.add(lateCorrection);

        TemporalState state = integrator.stateSnapshot();
        Assertions.assertEquals(
            oracle.replayNormalized(received),
            state);
        Assertions.assertEquals(
            bobBefore,
            state.getVersionsByFactKey().get(BOB_KEY));
        Assertions.assertEquals(
            MemoryFactVersionStatus.RETRACTED,
            version(state, "event-alice-retract:version:0")
                .getStatus());
    }

    @Test
    public void testFailedApplyDoesNotCommitLedgerOrState() {
        IncrementalTemporalIntegrator integrator =
            new IncrementalTemporalIntegrator();
        TimeInterval validTime = interval(
            "2024-01-01T00:00:00Z",
            "2025-01-01T00:00:00Z");
        NormalizedMemoryEvent add = add(
            "event-add",
            ALICE_KEY,
            "Beijing",
            validTime,
            "2024-03-01T00:00:00Z");
        NormalizedMemoryEvent invalid = correct(
            "event-change",
            ALICE_KEY,
            "Shanghai",
            interval(
                "2023-12-01T00:00:00Z",
                "2024-02-01T00:00:00Z"),
            "2024-06-01T00:00:00Z");
        NormalizedMemoryEvent validRetry = correct(
            "event-change",
            ALICE_KEY,
            "Shanghai",
            validTime,
            "2024-06-01T00:00:00Z");
        NormalizedMemoryEvent reused = correct(
            "event-change",
            ALICE_KEY,
            "London",
            validTime,
            "2024-06-01T00:00:00Z");

        integrator.apply(add);
        TemporalState before = integrator.stateSnapshot();

        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> integrator.apply(invalid));
        Assertions.assertEquals(before, integrator.stateSnapshot());

        integrator.apply(validRetry);
        TemporalState afterRetry = integrator.stateSnapshot();
        Assertions.assertEquals(
            oracle.replayNormalized(Arrays.asList(add, validRetry)),
            afterRetry);

        integrator.apply(validRetry);
        Assertions.assertEquals(afterRetry, integrator.stateSnapshot());
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> integrator.apply(reused));
        Assertions.assertEquals(afterRetry, integrator.stateSnapshot());
    }

    @Test
    public void testFactKeyScopeIsolationAndImmutableSnapshot() {
        IncrementalTemporalIntegrator integrator =
            new IncrementalTemporalIntegrator();
        FactKey accountKey = new FactKey(
            "person:alice",
            "city",
            "account");
        TimeInterval validTime = interval(
            "2024-01-01T00:00:00Z",
            "2025-01-01T00:00:00Z");
        NormalizedMemoryEvent profile = add(
            "event-profile",
            ALICE_KEY,
            "Beijing",
            "fact-shared",
            validTime,
            "2024-03-01T00:00:00Z");
        NormalizedMemoryEvent account = add(
            "event-account",
            accountKey,
            "Shanghai",
            "fact-shared",
            validTime,
            "2024-04-01T00:00:00Z");

        integrator.apply(profile);
        integrator.apply(account);
        TemporalState state = integrator.stateSnapshot();

        Assertions.assertEquals(2, state.getVersions().size());
        Assertions.assertEquals(
            Arrays.asList(accountKey, ALICE_KEY),
            new ArrayList<>(
                state.getVersionsByFactKey().keySet()));
        Assertions.assertTrue(state.getRelations().isEmpty());
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () -> state.getVersions().clear());
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () -> state.getRelations().clear());
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () -> state.getVersionsByFactKey().clear());
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () -> state.getVersionsByFactKey()
                .get(ALICE_KEY).clear());
    }

    private NormalizedMemoryEvent add(
        String eventId,
        FactKey key,
        String value,
        TimeInterval validTime,
        String recordedAt) {
        return add(
            eventId,
            key,
            value,
            factId(key),
            validTime,
            recordedAt);
    }

    private NormalizedMemoryEvent add(
        String eventId,
        FactKey key,
        String value,
        String factId,
        TimeInterval validTime,
        String recordedAt) {
        return normalizer.normalize(
            MemoryEvent.add(
                eventId,
                fact(factId, key, value),
                validTime,
                time(recordedAt),
                evidence(eventId)),
            key);
    }

    private NormalizedMemoryEvent correct(
        String eventId,
        FactKey key,
        String value,
        TimeInterval validTime,
        String recordedAt) {
        return normalizer.normalize(
            MemoryEvent.correct(
                eventId,
                fact(factId(key), key, value),
                validTime,
                time(recordedAt),
                evidence(eventId)),
            key);
    }

    private NormalizedMemoryEvent retract(
        String eventId,
        FactKey key,
        TimeInterval validTime,
        String recordedAt) {
        return normalizer.normalize(
            MemoryEvent.retract(
                eventId,
                factId(key),
                validTime,
                time(recordedAt),
                evidence(eventId)),
            key);
    }

    private static MemoryFact fact(
        String factId,
        FactKey key,
        String value) {
        return MemoryFact.attribute(
            factId,
            new MemoryEntity(key.getSubjectId(), "person"),
            key.getPredicate(),
            value);
    }

    private static String factId(FactKey key) {
        return "fact-"
            + key.getSubjectId()
            + "-"
            + key.getScope();
    }

    private static List<Evidence> evidence(String eventId) {
        return Collections.singletonList(new Evidence(
            "evidence-" + eventId,
            new Source("source-1", "registry"),
            "Evidence for " + eventId));
    }

    private static MemoryFactVersion version(
        TemporalState state,
        String versionId) {
        for (MemoryFactVersion version : state.getVersions()) {
            if (version.getId().equals(versionId)) {
                return version;
            }
        }
        throw new AssertionError("Missing version: " + versionId);
    }

    private static TimeInterval interval(
        String start,
        String end) {
        return new TimeInterval(time(start), time(end));
    }

    private static Instant time(String value) {
        return Instant.parse(value);
    }
}
