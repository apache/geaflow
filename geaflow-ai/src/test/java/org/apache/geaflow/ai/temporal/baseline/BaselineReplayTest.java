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

package org.apache.geaflow.ai.temporal.baseline;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
import org.apache.geaflow.ai.temporal.oracle.ReplayMethod;
import org.apache.geaflow.ai.temporal.semantics.CanonicalSnapshot;
import org.apache.geaflow.ai.temporal.semantics.EventNormalizer;
import org.apache.geaflow.ai.temporal.semantics.NormalizedMemoryEvent;
import org.apache.geaflow.ai.temporal.semantics.TemporalState;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Defines deterministic current-state semantics for the two replay baselines.
 */
public class BaselineReplayTest {

    private static final FactKey PROFILE_KEY = new FactKey(
        "person:alice",
        "city",
        "profile");
    private static final FactKey ACCOUNT_KEY = new FactKey(
        "person:alice",
        "city",
        "account");
    private static final TimeInterval ALL_VALID_TIME =
        TimeInterval.unboundedFrom(Instant.ofEpochMilli(Long.MIN_VALUE));

    private final EventNormalizer normalizer = new EventNormalizer();

    @Test
    public void testSameTimeConflictUsesEventIdTieBreakAndIgnoresValidTime() {
        Instant recordedAt = Instant.ofEpochMilli(1000);
        NormalizedMemoryEvent eventA = add(
            "event-a",
            "fact-a",
            PROFILE_KEY,
            "Beijing",
            interval(100, 200),
            recordedAt);
        NormalizedMemoryEvent eventB = add(
            "event-b",
            "fact-b",
            PROFILE_KEY,
            "Rome",
            interval(300, 400),
            recordedAt);
        List<NormalizedMemoryEvent> forward = Arrays.asList(eventA, eventB);
        List<NormalizedMemoryEvent> reversed = Arrays.asList(eventB, eventA);

        ReplayMethod lww = new LwwBaseline();
        CanonicalSnapshot expectedLww = snapshot(
            forward,
            versions(PROFILE_KEY, version(eventB)),
            eventB);
        Assertions.assertEquals(expectedLww, lww.replayToSnapshot(forward));
        Assertions.assertEquals(expectedLww, lww.replayToSnapshot(reversed));

        ReplayMethod singleTimestamp = new SingleTimestampBaseline();
        CanonicalSnapshot expectedSingle = snapshot(
            forward,
            versions(PROFILE_KEY, version(eventA), version(eventB)),
            Collections.singletonList(new VersionRelation(
                VersionRelationType.CONFLICTS_WITH,
                versionId(eventA),
                versionId(eventB))),
            eventA,
            eventB);
        Assertions.assertEquals(
            expectedSingle,
            singleTimestamp.replayToSnapshot(forward));
        Assertions.assertEquals(
            expectedSingle,
            singleTimestamp.replayToSnapshot(reversed));
    }

    @Test
    public void testSingleTimestampReplacesSameValueAndBuildsAllCurrentConflicts() {
        NormalizedMemoryEvent firstBeijing = add(
            "event-a",
            "fact-a",
            PROFILE_KEY,
            "Beijing",
            interval(100, 200),
            Instant.ofEpochMilli(1000));
        NormalizedMemoryEvent latestBeijing = add(
            "event-b",
            "fact-b",
            PROFILE_KEY,
            "Beijing",
            interval(300, 400),
            Instant.ofEpochMilli(2000));
        NormalizedMemoryEvent rome = add(
            "event-c",
            "fact-c",
            PROFILE_KEY,
            "Rome",
            interval(500, 600),
            Instant.ofEpochMilli(3000));
        NormalizedMemoryEvent paris = add(
            "event-d",
            "fact-d",
            PROFILE_KEY,
            "Paris",
            interval(700, 800),
            Instant.ofEpochMilli(4000));
        List<NormalizedMemoryEvent> ordered = Arrays.asList(
            firstBeijing,
            latestBeijing,
            rome,
            paris);
        List<NormalizedMemoryEvent> reversed = Arrays.asList(
            paris,
            rome,
            latestBeijing,
            firstBeijing);

        ReplayMethod method = new SingleTimestampBaseline();
        CanonicalSnapshot snapshot = method.replayToSnapshot(ordered);

        Assertions.assertEquals(snapshot, method.replayToSnapshot(reversed));
        Assertions.assertEquals(
            Arrays.asList(
                versionId(latestBeijing),
                versionId(rome),
                versionId(paris)),
            versionIds(snapshot));
        Assertions.assertEquals(
            latestBeijing.getEventId(),
            snapshot.getGeneratingEventIds().get(
                versionId(latestBeijing)));
        Assertions.assertEquals(
            Arrays.asList(
                conflict(latestBeijing, rome),
                conflict(latestBeijing, paris),
                conflict(rome, paris)),
            snapshot.getState().getRelations());
    }

    @Test
    public void testLatePartialCorrectionReplacesCurrentStateWithoutHistory() {
        NormalizedMemoryEvent first = add(
            "event-add-a",
            "fact-a",
            PROFILE_KEY,
            "Beijing",
            interval(100, 900),
            Instant.ofEpochMilli(1000));
        NormalizedMemoryEvent conflict = add(
            "event-add-b",
            "fact-b",
            PROFILE_KEY,
            "Rome",
            interval(100, 900),
            Instant.ofEpochMilli(2000));
        NormalizedMemoryEvent correction = correct(
            "event-correct",
            "fact-a",
            PROFILE_KEY,
            "Shanghai",
            interval(400, 500),
            Instant.ofEpochMilli(3000));
        List<NormalizedMemoryEvent> chronological =
            Arrays.asList(first, conflict, correction);
        List<NormalizedMemoryEvent> arrivalOrder =
            Arrays.asList(correction, conflict, first);
        CanonicalSnapshot expected = snapshot(
            chronological,
            versions(PROFILE_KEY, version(correction)),
            correction);

        for (ReplayMethod method : methods()) {
            Assertions.assertEquals(
                expected,
                method.replayToSnapshot(chronological));
            Assertions.assertEquals(
                expected,
                method.replayToSnapshot(arrivalOrder));
        }
    }

    @Test
    public void testRetractClearsCurrentStateAndLaterAddRestoresIt() {
        NormalizedMemoryEvent first = add(
            "event-add-a",
            "fact-a",
            PROFILE_KEY,
            "Beijing",
            interval(100, 900),
            Instant.ofEpochMilli(1000));
        NormalizedMemoryEvent second = add(
            "event-add-b",
            "fact-b",
            PROFILE_KEY,
            "Rome",
            interval(100, 900),
            Instant.ofEpochMilli(2000));
        NormalizedMemoryEvent retract = retract(
            "event-retract",
            "fact-b",
            PROFILE_KEY,
            interval(450, 460),
            Instant.ofEpochMilli(3000));
        NormalizedMemoryEvent recovery = add(
            "event-recover",
            "fact-c",
            PROFILE_KEY,
            "Paris",
            interval(700, 800),
            Instant.ofEpochMilli(4000));
        List<NormalizedMemoryEvent> throughRetract =
            Arrays.asList(retract, second, first);
        CanonicalSnapshot expectedRetracted = snapshot(
            throughRetract,
            Collections.emptyMap());
        List<NormalizedMemoryEvent> throughRecovery =
            Arrays.asList(recovery, retract, second, first);
        CanonicalSnapshot expectedRecovered = snapshot(
            throughRecovery,
            versions(PROFILE_KEY, version(recovery)),
            recovery);
        NormalizedMemoryEvent correctionWithoutCurrent = correct(
            "event-orphan-correct",
            "fact-a",
            PROFILE_KEY,
            "London",
            interval(100, 200),
            Instant.ofEpochMilli(1000));
        NormalizedMemoryEvent retractWithoutCurrent = retract(
            "event-orphan-retract",
            "fact-a",
            PROFILE_KEY,
            interval(100, 200),
            Instant.ofEpochMilli(1000));

        for (ReplayMethod method : methods()) {
            Assertions.assertEquals(
                expectedRetracted,
                method.replayToSnapshot(throughRetract));
            Assertions.assertEquals(
                expectedRecovered,
                method.replayToSnapshot(throughRecovery));
            Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> method.replayToSnapshot(
                    Collections.singletonList(correctionWithoutCurrent)));
            Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> method.replayToSnapshot(
                    Collections.singletonList(retractWithoutCurrent)));
            Assertions.assertEquals(
                expectedRetracted,
                method.replayToSnapshot(throughRetract));
        }
    }

    @Test
    public void testDuplicateIsNoopAndEventIdReuseIsAtomic() {
        NormalizedMemoryEvent original = add(
            "event-a",
            "fact-a",
            PROFILE_KEY,
            "Beijing",
            interval(100, 900),
            Instant.ofEpochMilli(1000));
        NormalizedMemoryEvent reused = add(
            "event-a",
            "fact-a",
            PROFILE_KEY,
            "Rome",
            interval(100, 900),
            Instant.ofEpochMilli(1000));
        NormalizedMemoryEvent retract = retract(
            "event-retract",
            "fact-a",
            PROFILE_KEY,
            interval(200, 300),
            Instant.ofEpochMilli(2000));
        CanonicalSnapshot expected = snapshot(
            Collections.singletonList(original),
            versions(PROFILE_KEY, version(original)),
            original);
        CanonicalSnapshot expectedRetracted = snapshot(
            Arrays.asList(original, retract),
            Collections.emptyMap());

        for (ReplayMethod method : methods()) {
            Assertions.assertEquals(
                expected,
                method.replayToSnapshot(
                    Arrays.asList(original, original)));
            Assertions.assertEquals(
                expectedRetracted,
                method.replayToSnapshot(
                    Arrays.asList(original, retract, retract)));
            Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> method.replayToSnapshot(
                    Arrays.asList(original, reused)));
            Assertions.assertEquals(
                expected,
                method.replayToSnapshot(
                    Collections.singletonList(original)));
        }
    }

    @Test
    public void testFactKeyScopeKeepsIndependentCurrentState() {
        Instant recordedAt = Instant.ofEpochMilli(1000);
        NormalizedMemoryEvent profile = add(
            "event-profile",
            "fact-shared",
            PROFILE_KEY,
            "Beijing",
            interval(100, 200),
            recordedAt);
        NormalizedMemoryEvent account = add(
            "event-account",
            "fact-shared",
            ACCOUNT_KEY,
            "Rome",
            interval(300, 400),
            recordedAt);
        Map<FactKey, List<MemoryFactVersion>> current = new LinkedHashMap<>();
        current.put(PROFILE_KEY, Collections.singletonList(version(profile)));
        current.put(ACCOUNT_KEY, Collections.singletonList(version(account)));
        CanonicalSnapshot expected = snapshot(
            Arrays.asList(profile, account),
            current,
            profile,
            account);

        for (ReplayMethod method : methods()) {
            Assertions.assertEquals(
                expected,
                method.replayToSnapshot(Arrays.asList(profile, account)));
        }
    }

    private static ReplayMethod[] methods() {
        return new ReplayMethod[] {
            new LwwBaseline(),
            new SingleTimestampBaseline()
        };
    }

    private NormalizedMemoryEvent add(
        String eventId,
        String factId,
        FactKey key,
        String value,
        TimeInterval validTime,
        Instant recordedAt) {
        return normalizer.normalize(
            MemoryEvent.add(
                eventId,
                fact(factId, key, value),
                validTime,
                recordedAt,
                evidence(eventId)),
            key);
    }

    private NormalizedMemoryEvent correct(
        String eventId,
        String factId,
        FactKey key,
        String value,
        TimeInterval validTime,
        Instant recordedAt) {
        return normalizer.normalize(
            MemoryEvent.correct(
                eventId,
                fact(factId, key, value),
                validTime,
                recordedAt,
                evidence(eventId)),
            key);
    }

    private NormalizedMemoryEvent retract(
        String eventId,
        String factId,
        FactKey key,
        TimeInterval validTime,
        Instant recordedAt) {
        return normalizer.normalize(
            MemoryEvent.retract(
                eventId,
                factId,
                validTime,
                recordedAt,
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

    private static List<Evidence> evidence(String eventId) {
        return Collections.singletonList(new Evidence(
            "evidence-" + eventId,
            new Source("source-registry", "registry"),
            "Evidence for " + eventId));
    }

    private static TimeInterval interval(long start, long end) {
        return new TimeInterval(
            Instant.ofEpochMilli(start),
            Instant.ofEpochMilli(end));
    }

    private static MemoryFactVersion version(
        NormalizedMemoryEvent event) {
        return new MemoryFactVersion(
            versionId(event),
            event.getEvent().getFact().get(),
            MemoryFactVersionStatus.ACTIVE,
            ALL_VALID_TIME,
            TimeInterval.unboundedFrom(event.getRecordedAt()),
            event.getEvidence());
    }

    private static String versionId(NormalizedMemoryEvent event) {
        return event.getEventId() + ":version:0";
    }

    private static List<String> versionIds(CanonicalSnapshot snapshot) {
        List<String> ids = new ArrayList<>();
        for (MemoryFactVersion version : snapshot.getState().getVersions()) {
            ids.add(version.getId());
        }
        return ids;
    }

    private static VersionRelation conflict(
        NormalizedMemoryEvent left,
        NormalizedMemoryEvent right) {
        return new VersionRelation(
            VersionRelationType.CONFLICTS_WITH,
            versionId(left),
            versionId(right));
    }

    private static Map<FactKey, List<MemoryFactVersion>> versions(
        FactKey key,
        MemoryFactVersion... values) {
        return Collections.singletonMap(key, Arrays.asList(values));
    }

    private static CanonicalSnapshot snapshot(
        List<NormalizedMemoryEvent> events,
        Map<FactKey, List<MemoryFactVersion>> versions,
        NormalizedMemoryEvent... generatingEvents) {
        return snapshot(
            events,
            versions,
            Collections.emptyList(),
            generatingEvents);
    }

    private static CanonicalSnapshot snapshot(
        List<NormalizedMemoryEvent> events,
        Map<FactKey, List<MemoryFactVersion>> versions,
        List<VersionRelation> relations,
        NormalizedMemoryEvent... generatingEvents) {
        Map<String, String> generating = new LinkedHashMap<>();
        for (NormalizedMemoryEvent event : generatingEvents) {
            generating.put(versionId(event), event.getEventId());
        }
        return new CanonicalSnapshot(
            new TemporalState(versions, relations),
            events,
            generating);
    }
}
