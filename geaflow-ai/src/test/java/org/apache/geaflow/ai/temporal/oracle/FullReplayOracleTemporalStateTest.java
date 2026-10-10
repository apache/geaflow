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

package org.apache.geaflow.ai.temporal.oracle;

import java.time.Instant;
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
import org.apache.geaflow.ai.temporal.model.VersionRelation;
import org.apache.geaflow.ai.temporal.model.VersionRelationType;
import org.apache.geaflow.ai.temporal.query.BitemporalQuery;
import org.apache.geaflow.ai.temporal.semantics.EventNormalizer;
import org.apache.geaflow.ai.temporal.semantics.NormalizedMemoryEvent;
import org.apache.geaflow.ai.temporal.semantics.TemporalState;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class FullReplayOracleTemporalStateTest {

    private static final FactKey KEY = new FactKey(
        "person:alice",
        "city",
        "profile");

    private final EventNormalizer normalizer = new EventNormalizer();
    private final FullReplayOracle oracle = new FullReplayOracle();
    private final BitemporalQuery query = new BitemporalQuery();

    @Test
    public void testPartialRetractionCreatesTombstoneAndResidues() {
        NormalizedMemoryEvent add = add(
            "event-add",
            KEY,
            "Beijing",
            TimeInterval.unboundedFrom(
                time("2024-01-01T00:00:00Z")),
            "2024-03-01T00:00:00Z");
        NormalizedMemoryEvent retract = retract(
            "event-retract",
            KEY,
            interval(
                "2024-04-01T00:00:00Z",
                "2024-09-01T00:00:00Z"),
            "2024-06-01T00:00:00Z");

        TemporalState state = oracle.replayNormalized(
            Arrays.asList(retract, add));

        Assertions.assertEquals(4, state.getVersions().size());
        Assertions.assertEquals(
            state.getVersions(),
            state.getVersionsByFactKey().get(KEY));

        MemoryFactVersion old = version(
            state,
            "event-add:version:0");
        Assertions.assertEquals(
            MemoryFactVersionStatus.ACTIVE,
            old.getStatus());
        Assertions.assertEquals(
            interval(
                "2024-03-01T00:00:00Z",
                "2024-06-01T00:00:00Z"),
            old.getTransactionTime());

        MemoryFactVersion tombstone = version(
            state,
            "event-retract:version:0");
        Assertions.assertEquals(
            MemoryFactVersionStatus.RETRACTED,
            tombstone.getStatus());
        Assertions.assertEquals(
            retract.getValidTime(),
            tombstone.getValidTime());
        Assertions.assertEquals(
            TimeInterval.unboundedFrom(retract.getRecordedAt()),
            tombstone.getTransactionTime());

        MemoryFactVersion left = version(
            state,
            "event-retract:version:1");
        MemoryFactVersion right = version(
            state,
            "event-retract:version:2");
        Assertions.assertEquals(
            MemoryFactVersionStatus.ACTIVE,
            left.getStatus());
        Assertions.assertEquals(
            interval(
                "2024-01-01T00:00:00Z",
                "2024-04-01T00:00:00Z"),
            left.getValidTime());
        Assertions.assertEquals(
            MemoryFactVersionStatus.ACTIVE,
            right.getStatus());
        Assertions.assertEquals(
            TimeInterval.unboundedFrom(
                time("2024-09-01T00:00:00Z")),
            right.getValidTime());

        Assertions.assertEquals(
            Arrays.asList(
                relation(
                    VersionRelationType.SUPERSEDES,
                    tombstone.getId(),
                    old.getId()),
                relation(
                    VersionRelationType.SUPERSEDES,
                    left.getId(),
                    old.getId()),
                relation(
                    VersionRelationType.SUPERSEDES,
                    right.getId(),
                    old.getId())),
            state.getRelations());
        Assertions.assertTrue(query.query(
            state.getVersions(),
            time("2024-05-01T00:00:00Z"),
            time("2024-07-01T00:00:00Z")).isEmpty());
        Assertions.assertEquals(1, query.query(
            state.getVersions(),
            time("2024-02-01T00:00:00Z"),
            time("2024-07-01T00:00:00Z")).size());
    }

    @Test
    public void testExplicitCorrectionSupersedesSameValue() {
        TimeInterval validTime = interval(
            "2024-01-01T00:00:00Z",
            "2025-01-01T00:00:00Z");
        NormalizedMemoryEvent add = add(
            "event-add",
            KEY,
            "Beijing",
            validTime,
            "2024-03-01T00:00:00Z");
        NormalizedMemoryEvent correction = correct(
            "event-correct",
            KEY,
            "Beijing",
            validTime,
            "2024-06-01T00:00:00Z");

        TemporalState state = oracle.replayNormalized(
            Arrays.asList(correction, add));

        Assertions.assertEquals(2, state.getVersions().size());
        Assertions.assertEquals(
            Collections.singletonList(relation(
                VersionRelationType.SUPERSEDES,
                "event-correct:version:0",
                "event-add:version:0")),
            state.getRelations());
    }

    @Test
    public void testSameValueOverlapCreatesDuplicateVersion() {
        TimeInterval validTime = interval(
            "2024-01-01T00:00:00Z",
            "2025-01-01T00:00:00Z");
        NormalizedMemoryEvent first = add(
            "event-first",
            KEY,
            "Beijing",
            validTime,
            "2024-03-01T00:00:00Z");
        NormalizedMemoryEvent duplicate = add(
            "event-duplicate",
            KEY,
            "Beijing",
            validTime,
            "2024-04-01T00:00:00Z");

        TemporalState state = oracle.replayNormalized(
            Arrays.asList(duplicate, first));

        MemoryFactVersion old = version(
            state,
            "event-first:version:0");
        MemoryFactVersion current = version(
            state,
            "event-duplicate:version:0");
        Assertions.assertEquals(
            time("2024-04-01T00:00:00Z"),
            old.getTransactionTime().getEnd().get());
        Assertions.assertFalse(
            current.getTransactionTime().getEnd().isPresent());
        Assertions.assertEquals(
            Arrays.asList(
                "evidence-event-duplicate",
                "evidence-event-first"),
            Arrays.asList(
                current.getEvidence().get(0).getId(),
                current.getEvidence().get(1).getId()));
        Assertions.assertEquals(
            Collections.singletonList(relation(
                VersionRelationType.DUPLICATE_OF,
                current.getId(),
                old.getId())),
            state.getRelations());
    }

    @Test
    public void testDifferentValueOverlapConflictsButBoundaryDoesNot() {
        NormalizedMemoryEvent first = add(
            "event-first",
            KEY,
            "Beijing",
            interval(
                "2024-01-01T00:00:00Z",
                "2024-06-01T00:00:00Z"),
            "2024-03-01T00:00:00Z");
        NormalizedMemoryEvent conflict = add(
            "event-conflict",
            KEY,
            "Shanghai",
            interval(
                "2024-04-01T00:00:00Z",
                "2024-09-01T00:00:00Z"),
            "2024-04-01T00:00:00Z");
        NormalizedMemoryEvent boundary = add(
            "event-boundary",
            KEY,
            "Rome",
            interval(
                "2024-09-01T00:00:00Z",
                "2024-12-01T00:00:00Z"),
            "2024-05-01T00:00:00Z");

        TemporalState shuffled = oracle.replayNormalized(
            Arrays.asList(boundary, conflict, first));
        TemporalState ordered = oracle.replayNormalized(
            Arrays.asList(first, conflict, boundary));

        Assertions.assertEquals(ordered, shuffled);
        Assertions.assertEquals(3, shuffled.getVersions().size());
        for (MemoryFactVersion version : shuffled.getVersions()) {
            Assertions.assertEquals(
                MemoryFactVersionStatus.ACTIVE,
                version.getStatus());
            Assertions.assertFalse(
                version.getTransactionTime().getEnd().isPresent());
        }
        Assertions.assertEquals(
            Collections.singletonList(relation(
                VersionRelationType.CONFLICTS_WITH,
                "event-conflict:version:0",
                "event-first:version:0")),
            shuffled.getRelations());
    }

    @Test
    public void testPartialChangesAddConflictsForResidualVersions() {
        NormalizedMemoryEvent first = add(
            "event-first",
            KEY,
            "Beijing",
            interval(
                "2024-01-01T00:00:00Z",
                "2024-10-01T00:00:00Z"),
            "2024-03-01T00:00:00Z");
        NormalizedMemoryEvent conflict = add(
            "event-conflict",
            KEY,
            "Shanghai",
            interval(
                "2024-05-01T00:00:00Z",
                "2025-01-01T00:00:00Z"),
            "2024-04-01T00:00:00Z");
        NormalizedMemoryEvent retraction = retract(
            "event-retract",
            KEY,
            interval(
                "2024-01-01T00:00:00Z",
                "2024-05-01T00:00:00Z"),
            "2024-06-01T00:00:00Z");
        NormalizedMemoryEvent correction = correct(
            "event-correct",
            KEY,
            "Rome",
            retraction.getValidTime(),
            "2024-06-01T00:00:00Z");

        TemporalState state = oracle.replayNormalized(
            Arrays.asList(retraction, conflict, first));

        Assertions.assertTrue(state.getRelations().contains(relation(
            VersionRelationType.CONFLICTS_WITH,
            "event-conflict:version:0",
            "event-first:version:0")));
        Assertions.assertTrue(state.getRelations().contains(relation(
            VersionRelationType.CONFLICTS_WITH,
            "event-conflict:version:0",
            "event-retract:version:1")));

        TemporalState correctedState = oracle.replayNormalized(
            Arrays.asList(correction, conflict, first));
        Assertions.assertTrue(correctedState.getRelations().contains(
            relation(
                VersionRelationType.CONFLICTS_WITH,
                "event-conflict:version:0",
                "event-correct:version:1")));
    }

    @Test
    public void testLedgerNoopAndEventIdReuse() {
        NormalizedMemoryEvent original = add(
            "event-1",
            KEY,
            "Beijing",
            interval(
                "2024-01-01T00:00:00Z",
                "2025-01-01T00:00:00Z"),
            "2024-03-01T00:00:00Z");
        NormalizedMemoryEvent reused = add(
            "event-1",
            KEY,
            "Shanghai",
            interval(
                "2024-01-01T00:00:00Z",
                "2025-01-01T00:00:00Z"),
            "2024-03-01T00:00:00Z");

        Assertions.assertEquals(
            oracle.replayNormalized(
                Collections.singletonList(original)),
            oracle.replayNormalized(
                Arrays.asList(original, original)));
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> oracle.replayNormalized(
                Arrays.asList(original, reused)));
    }

    @Test
    public void testSameRecordedAtDoesNotLeaveZeroLengthVersion() {
        TimeInterval validTime = interval(
            "2024-01-01T00:00:00Z",
            "2025-01-01T00:00:00Z");
        NormalizedMemoryEvent add = add(
            "event-a-add",
            KEY,
            "Beijing",
            validTime,
            "2024-03-01T00:00:00Z");
        NormalizedMemoryEvent correction = correct(
            "event-b-correct",
            KEY,
            "Shanghai",
            validTime,
            "2024-03-01T00:00:00Z");

        TemporalState state = oracle.replayNormalized(
            Arrays.asList(correction, add));

        Assertions.assertEquals(1, state.getVersions().size());
        Assertions.assertEquals(
            "event-b-correct:version:0",
            state.getVersions().get(0).getId());
        Assertions.assertTrue(state.getRelations().isEmpty());
    }

    private NormalizedMemoryEvent add(
        String eventId,
        FactKey key,
        String value,
        TimeInterval validTime,
        String recordedAt) {
        return normalizer.normalize(
            MemoryEvent.add(
                eventId,
                fact(key, value),
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
                fact(key, value),
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
        FactKey key,
        String value) {
        return MemoryFact.attribute(
            factId(key),
            new MemoryEntity(key.getSubjectId(), "person"),
            key.getPredicate(),
            value);
    }

    private static String factId(FactKey key) {
        return "fact-city-" + key.getScope();
    }

    private static List<Evidence> evidence(String eventId) {
        return Collections.singletonList(new Evidence(
            "evidence-" + eventId,
            new Source("source-1", "registry"),
            "Evidence for " + eventId));
    }

    private static VersionRelation relation(
        VersionRelationType type,
        String from,
        String to) {
        return new VersionRelation(type, from, to);
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
