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
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.geaflow.ai.temporal.model.Evidence;
import org.apache.geaflow.ai.temporal.model.FactKey;
import org.apache.geaflow.ai.temporal.model.MemoryEntity;
import org.apache.geaflow.ai.temporal.model.MemoryEvent;
import org.apache.geaflow.ai.temporal.model.MemoryFact;
import org.apache.geaflow.ai.temporal.model.MemoryFactVersion;
import org.apache.geaflow.ai.temporal.model.MemoryFactVersionStatus;
import org.apache.geaflow.ai.temporal.model.Source;
import org.apache.geaflow.ai.temporal.model.TimeInterval;
import org.apache.geaflow.ai.temporal.model.VersionRelationType;
import org.apache.geaflow.ai.temporal.query.BitemporalQuery;
import org.apache.geaflow.ai.temporal.semantics.EventNormalizer;
import org.apache.geaflow.ai.temporal.semantics.NormalizedMemoryEvent;
import org.apache.geaflow.ai.temporal.semantics.TemporalErrorCode;
import org.apache.geaflow.ai.temporal.semantics.TemporalState;
import org.apache.geaflow.ai.temporal.semantics.TemporalValidationException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

public class TargetedReplayOracleTest {

    private static final FactKey KEY = new FactKey("alice", "city", "profile");
    private static final TimeInterval YEAR = interval("2024-01-01", "2025-01-01");
    private final EventNormalizer normalizer = new EventNormalizer();
    private final FullReplayOracle oracle = new FullReplayOracle();

    @Test
    public void testCorrectionClipsToTargetAndKeepsConflictingValue() {
        NormalizedMemoryEvent first = add("first", "Beijing",
            interval("2024-01-01", "2024-07-01"), "2024-02-01");
        NormalizedMemoryEvent conflict = add("conflict", "Shanghai",
            interval("2024-04-01", "2024-12-01"), "2024-03-01");
        MemoryFactVersion target = addedVersion(first);
        MemoryFactVersion untouched = addedVersion(conflict);
        NormalizedMemoryEvent change = correct("change", "Rome",
            interval("2024-05-01", "2024-09-01"), "2024-06-01", target.getId());

        TemporalState state = replay(first, conflict, change);

        Assertions.assertTrue(state.getVersions().contains(untouched));
        assertValues(state, "2024-06-15", "Rome", "Shanghai");
        assertValues(state, "2024-08-01", "Shanghai");
        MemoryFactVersion changed = currentValue(state, "Rome");
        Assertions.assertEquals(interval("2024-05-01", "2024-07-01"),
            changed.getValidTime());
        Assertions.assertEquals(interval("2024-01-01", "2024-05-01"),
            currentValue(state, "Beijing").getValidTime());
        Assertions.assertEquals(2, state.getRelations().stream()
            .filter(relation -> relation.getType() == VersionRelationType.SUPERSEDES)
            .count());
        Assertions.assertTrue(state.getRelations().stream()
            .filter(relation -> relation.getType() == VersionRelationType.SUPERSEDES)
            .allMatch(relation -> relation.getToVersionId().equals(target.getId())));
    }

    @Test
    public void testRetractionOnlyWithdrawsItsSelectedConflictingVersion() {
        NormalizedMemoryEvent first = add("first", "Beijing", YEAR, "2024-02-01");
        NormalizedMemoryEvent conflict = add("conflict", "Shanghai", YEAR, "2024-03-01");
        NormalizedMemoryEvent retract = retract("withdraw",
            interval("2024-04-01", "2024-07-01"), "2024-06-01",
            addedVersion(conflict).getId());

        TemporalState state = replay(first, conflict, retract);

        Assertions.assertTrue(state.getVersions().contains(addedVersion(first)));
        assertValues(state, "2024-06-15", "Beijing");
        assertValues(state, "2024-08-01", "Beijing", "Shanghai");
        MemoryFactVersion tombstone = state.getVersions().stream()
            .filter(version -> version.getStatus() == MemoryFactVersionStatus.RETRACTED)
            .findFirst().get();
        Assertions.assertEquals("Shanghai", tombstone.getFact().getLiteralValue().get());
        Assertions.assertEquals(retract.getValidTime(), tombstone.getValidTime());
    }

    @Test
    public void testTargetValidationUsesStableCodesAndFactKeyIsolation() {
        NormalizedMemoryEvent first = add("first", "Beijing", YEAR, "2024-02-01");
        String targetId = addedVersion(first).getId();
        assertCode(TemporalErrorCode.REJECT_TARGET_REQUIRED,
            () -> normalizer.normalize(MemoryEvent.retract("missing-target", "fact-city",
                YEAR, time("2024-03-01"), evidence("missing-target")), KEY));
        assertCode(TemporalErrorCode.REJECT_TARGET_NOT_FOUND,
            () -> replay(first, retract("unknown", YEAR, "2024-03-01", "unknown-version")));
        NormalizedMemoryEvent otherScope = normalizer.normalize(
            MemoryEvent.retract("scope", "fact-city", YEAR, time("2024-03-01"),
                evidence("scope")).targeting(targetId),
            new FactKey("alice", "city", "account"));
        assertCode(TemporalErrorCode.REJECT_TARGET_NOT_FOUND, () -> replay(first, otherScope));
        assertCode(TemporalErrorCode.REJECT_TARGET_NO_OVERLAP,
            () -> replay(first, retract("adjacent", interval("2025-01-01", "2026-01-01"),
                "2024-03-01", targetId)));

        NormalizedMemoryEvent correction = correct("change", "Rome", YEAR,
            "2024-03-01", targetId);
        assertCode(TemporalErrorCode.REJECT_TARGET_INACTIVE,
            () -> replay(first, correction, retract("closed", YEAR, "2024-04-01", targetId)));
        NormalizedMemoryEvent withdrawn = retract("withdraw", YEAR, "2024-03-01", targetId);
        String tombstoneId = replay(first, withdrawn).getVersions().stream()
            .filter(version -> version.getStatus() == MemoryFactVersionStatus.RETRACTED)
            .findFirst().get().getId();
        assertCode(TemporalErrorCode.REJECT_TARGET_INACTIVE,
            () -> replay(first, withdrawn,
                retract("tombstone", YEAR, "2024-04-01", tombstoneId)));
        assertCode(TemporalErrorCode.REJECT_EVENT_ID_REUSE,
            () -> replay(first, add("first", "Rome", YEAR, "2024-02-01")));
    }

    @Test
    public void testSameTimestampChainDoesNotLeaveDanglingRelations() {
        NormalizedMemoryEvent first = add("event-a", "Beijing", YEAR, "2024-02-01");
        NormalizedMemoryEvent conflict = add("event-b", "Shanghai", YEAR, "2024-02-01");
        String firstId = addedVersion(first).getId();
        NormalizedMemoryEvent correction = correct("event-c", "Rome",
            interval("2024-04-01", "2024-10-01"), "2024-02-01", firstId);
        String changedId = currentValue(replay(first, conflict, correction), "Rome").getId();
        NormalizedMemoryEvent withdrawn = retract("event-d",
            interval("2024-05-01", "2024-06-01"), "2024-02-01", changedId);

        TemporalState state = replay(withdrawn, correction, conflict, first);

        Assertions.assertEquals(6, state.getVersions().size());
        Set<String> ids = state.getVersions().stream()
            .map(MemoryFactVersion::getId).collect(Collectors.toSet());
        Assertions.assertFalse(ids.contains(firstId));
        Assertions.assertFalse(ids.contains(changedId));
        Assertions.assertTrue(state.getVersions().stream()
            .noneMatch(version -> version.getTransactionTime().getEnd().isPresent()));
        Assertions.assertTrue(state.getRelations().stream().allMatch(relation ->
            ids.contains(relation.getFromVersionId()) && ids.contains(relation.getToVersionId())));
        assertValues(state, "2024-05-15", "Shanghai");
        assertValues(state, "2024-07-01", "Rome", "Shanghai");
    }

    @Test
    public void testResidualTargetSurvivesUnrelatedEarlierConflictAndReplayOrder() {
        NormalizedMemoryEvent first = add("first", "Beijing", YEAR, "2024-02-01");
        NormalizedMemoryEvent correction = correct("change", "Rome",
            interval("2024-06-01", "2024-07-01"), "2024-06-01",
            addedVersion(first).getId());
        TemporalState beforeLate = replay(first, correction);
        MemoryFactVersion residual = beforeLate.getVersions().stream()
            .filter(version -> !version.getTransactionTime().getEnd().isPresent())
            .filter(version -> version.getValidTime().equals(
                interval("2024-01-01", "2024-06-01")))
            .findFirst().get();
        NormalizedMemoryEvent earlierConflict = add("late", "Shanghai",
            interval("2024-01-01", "2024-03-01"), "2024-04-01");
        NormalizedMemoryEvent retract = retract("withdraw",
            interval("2024-02-01", "2024-03-01"), "2024-08-01", residual.getId());

        TemporalState state = replay(retract, correction, earlierConflict, first);

        Assertions.assertEquals(replay(first, earlierConflict, correction, retract), state);
        Assertions.assertTrue(state.getVersions().stream()
            .anyMatch(version -> version.getId().equals(residual.getId())));
        assertValues(state, "2024-02-15", "Shanghai");
        assertValues(state, "2024-05-01", "Beijing");
        assertValues(state, "2024-06-15", "Rome");
    }

    @Test
    public void testMergedAddIdentityUsesItsExpandedInterval() {
        NormalizedMemoryEvent first = add("first", "Beijing",
            interval("2024-01-01", "2024-04-01"), "2024-02-01");
        NormalizedMemoryEvent duplicate = add("duplicate", "Beijing",
            interval("2024-03-01", "2024-09-01"), "2024-03-01");

        TemporalState state = replay(duplicate, first);
        MemoryFactVersion merged = currentValue(state, "Beijing");

        Assertions.assertEquals(replay(first, duplicate), state);
        Assertions.assertEquals(interval("2024-01-01", "2024-09-01"),
            merged.getValidTime());
        Assertions.assertNotEquals(addedVersion(duplicate).getId(), merged.getId());
        NormalizedMemoryEvent retract = retract("withdraw",
            interval("2023-01-01", "2026-01-01"), "2024-05-01", merged.getId());
        TemporalState withdrawn = replay(first, duplicate, retract);
        MemoryFactVersion tombstone = withdrawn.getVersions().stream()
            .filter(version -> version.getStatus() == MemoryFactVersionStatus.RETRACTED)
            .findFirst().get();
        Assertions.assertEquals(merged.getValidTime(), tombstone.getValidTime());
        assertValues(withdrawn, "2024-06-15");
    }

    @Test
    public void testAddMergesTransitiveSameValueOverlapAfterExplicitCorrection() {
        NormalizedMemoryEvent first = add("first", "Beijing", YEAR, "2024-02-01");
        NormalizedMemoryEvent conflict = add("conflict", "Shanghai",
            interval("2024-04-01", "2024-07-01"), "2024-03-01");
        NormalizedMemoryEvent correction = correct("change", "Shanghai",
            interval("2024-05-01", "2024-08-01"), "2024-04-01",
            addedVersion(first).getId());
        NormalizedMemoryEvent duplicate = add("duplicate", "Shanghai",
            interval("2024-07-01", "2024-09-01"), "2024-05-01");

        TemporalState state = replay(first, conflict, correction, duplicate);

        MemoryFactVersion merged = currentValue(state, "Shanghai");
        Assertions.assertEquals(interval("2024-04-01", "2024-09-01"),
            merged.getValidTime());
        Assertions.assertEquals(3, merged.getEvidence().size());
        Assertions.assertEquals(1, state.getVersions().stream()
            .filter(version -> !version.getTransactionTime().getEnd().isPresent())
            .filter(version -> version.getFact().getLiteralValue().get().equals("Shanghai"))
            .count());
        Assertions.assertEquals(2, state.getRelations().stream()
            .filter(relation -> relation.getType() == VersionRelationType.DUPLICATE_OF)
            .filter(relation -> relation.getFromVersionId().equals(merged.getId()))
            .count());
    }

    private TemporalState replay(NormalizedMemoryEvent... events) {
        return oracle.replayNormalized(Arrays.asList(events));
    }

    private MemoryFactVersion addedVersion(NormalizedMemoryEvent event) {
        return replay(event).getVersions().get(0);
    }

    private NormalizedMemoryEvent add(
        String id,
        String value,
        TimeInterval validTime,
        String recordedAt) {
        return normalizer.normalize(MemoryEvent.add(id, fact(value), validTime,
            time(recordedAt), evidence(id)), KEY);
    }

    private NormalizedMemoryEvent correct(
        String id,
        String value,
        TimeInterval validTime,
        String recordedAt,
        String targetId) {
        return normalizer.normalize(MemoryEvent.correct(id, fact(value), validTime,
            time(recordedAt), evidence(id)).targeting(targetId), KEY);
    }

    private NormalizedMemoryEvent retract(
        String id,
        TimeInterval validTime,
        String recordedAt,
        String targetId) {
        return normalizer.normalize(MemoryEvent.retract(id, "fact-city", validTime,
            time(recordedAt), evidence(id)).targeting(targetId), KEY);
    }

    private static MemoryFact fact(String value) {
        return MemoryFact.attribute("fact-city", new MemoryEntity("alice", "person"),
            "city", value);
    }

    private static List<Evidence> evidence(String id) {
        return Collections.singletonList(new Evidence("evidence-" + id,
            new Source("registry", "registry"), "Evidence for " + id));
    }

    private static MemoryFactVersion currentValue(TemporalState state, String value) {
        return state.getVersions().stream()
            .filter(version -> version.getStatus() == MemoryFactVersionStatus.ACTIVE)
            .filter(version -> !version.getTransactionTime().getEnd().isPresent())
            .filter(version -> version.getFact().getLiteralValue().get().equals(value))
            .findFirst().get();
    }

    private static void assertValues(TemporalState state, String validAt, String... expected) {
        List<String> values = new BitemporalQuery().query(state.getVersions(),
            time(validAt), time("2025-01-01")).stream()
            .map(version -> version.getFact().getLiteralValue().get())
            .sorted().collect(Collectors.toList());
        Assertions.assertEquals(Arrays.asList(expected), values);
    }

    private static void assertCode(TemporalErrorCode code, Executable action) {
        Assertions.assertEquals(code, Assertions.assertThrows(
            TemporalValidationException.class, action).getCode());
    }

    private static TimeInterval interval(String start, String end) {
        return new TimeInterval(time(start), time(end));
    }

    private static Instant time(String date) {
        return Instant.parse(date + "T00:00:00Z");
    }
}
