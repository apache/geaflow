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
import org.apache.geaflow.ai.temporal.model.VersionRelation;
import org.apache.geaflow.ai.temporal.model.VersionRelationType;
import org.apache.geaflow.ai.temporal.oracle.FullReplayOracle;
import org.apache.geaflow.ai.temporal.semantics.EventLedgerDecision;
import org.apache.geaflow.ai.temporal.semantics.EventNormalizer;
import org.apache.geaflow.ai.temporal.semantics.NormalizedMemoryEvent;
import org.apache.geaflow.ai.temporal.semantics.TemporalErrorCode;
import org.apache.geaflow.ai.temporal.semantics.TemporalState;
import org.apache.geaflow.ai.temporal.semantics.TemporalValidationException;
import org.apache.geaflow.ai.temporal.semantics.VersionIdentity;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class IncrementalTargetTest {

    private static final Instant BASE = Instant.parse("2024-01-01T00:00:00Z");
    private static final FactKey PROFILE = new FactKey("person:alice", "city", "profile");
    private static final FactKey ACCOUNT = new FactKey("person:alice", "city", "account");
    private static final TimeInterval VALID = interval(0, 100);

    private final EventNormalizer normalizer = new EventNormalizer();
    private final FullReplayOracle oracle = new FullReplayOracle();

    @Test
    public void testBatchSortsTargetDependencyAndClipsToExactTarget() {
        IncrementalTemporalIntegrator integrator = new IncrementalTemporalIntegrator();
        NormalizedMemoryEvent source = add("source", PROFILE, "Beijing", VALID, 10);
        NormalizedMemoryEvent account = add("account", ACCOUNT, "Paris", VALID, 15);
        NormalizedMemoryEvent change = correct(
            "change", PROFILE, "Shanghai", interval(-10, 110), 20, addId(source));

        List<EventLedgerDecision> decisions = integrator.applyBatch(
            Arrays.asList(change, account, source));

        Assertions.assertEquals(Arrays.asList(EventLedgerDecision.ACCEPTED,
            EventLedgerDecision.ACCEPTED, EventLedgerDecision.ACCEPTED), decisions);
        Assertions.assertEquals(Arrays.asList(source, account, change),
            integrator.normalizedEventSnapshot());
        TemporalState state = integrator.stateSnapshot();
        Assertions.assertEquals(3, state.getVersions().size());
        MemoryFactVersion original = version(state, addId(source));
        Assertions.assertEquals(VALID, original.getValidTime());
        Assertions.assertEquals(interval(10, 20), original.getTransactionTime());
        MemoryFactVersion replacement = version(state, changeId(change,
            change.getEvent().getFact().get(), VALID));
        Assertions.assertEquals(VALID, replacement.getValidTime());
        Assertions.assertEquals(TimeInterval.unboundedFrom(at(20)),
            replacement.getTransactionTime());
        Assertions.assertEquals("Shanghai", replacement.getFact().getLiteralValue().get());
        Assertions.assertEquals(change.getEvidence(), replacement.getEvidence());
        Assertions.assertEquals(Collections.singletonList(new VersionRelation(
            VersionRelationType.SUPERSEDES, replacement.getId(), original.getId())),
            state.getRelations());
        Assertions.assertEquals(Collections.singletonList(version(state, addId(account))),
            state.getVersionsByFactKey().get(ACCOUNT));
        Assertions.assertEquals(oracle.replayNormalized(Arrays.asList(source, account, change)),
            state);
    }

    @Test
    public void testAtomicRollbackLeavesEveryEventIdAvailableForRetry() {
        IncrementalTemporalIntegrator integrator = new IncrementalTemporalIntegrator();
        NormalizedMemoryEvent source = add("source", PROFILE, "Beijing", VALID, 10);
        NormalizedMemoryEvent staged = add("staged", ACCOUNT, "Paris", VALID, 15);
        NormalizedMemoryEvent invalid = correct(
            "retry", PROFILE, "Shanghai", interval(100, 200), 20, addId(source));
        integrator.apply(source);
        TemporalState before = integrator.stateSnapshot();
        List<NormalizedMemoryEvent> eventsBefore = integrator.normalizedEventSnapshot();

        TemporalValidationException failure = Assertions.assertThrows(
            TemporalValidationException.class,
            () -> integrator.applyBatch(Arrays.asList(invalid, staged)));

        Assertions.assertEquals(TemporalErrorCode.REJECT_TARGET_NO_OVERLAP, failure.getCode());
        Assertions.assertEquals(before, integrator.stateSnapshot());
        Assertions.assertEquals(eventsBefore, integrator.normalizedEventSnapshot());
        NormalizedMemoryEvent retry = correct(
            "retry", PROFILE, "Shanghai", interval(20, 40), 20, addId(source));
        NormalizedMemoryEvent stagedRetry = add("staged", ACCOUNT, "Munich", VALID, 15);
        Assertions.assertEquals(Arrays.asList(EventLedgerDecision.ACCEPTED,
            EventLedgerDecision.ACCEPTED),
            integrator.applyBatch(Arrays.asList(retry, stagedRetry)));
        TemporalState after = integrator.stateSnapshot();
        Assertions.assertEquals(5, after.getVersions().size());
        Assertions.assertEquals(Arrays.asList(source, stagedRetry, retry),
            integrator.normalizedEventSnapshot());
        Assertions.assertEquals(interval(0, 20), version(after, changeId(retry,
            source.getEvent().getFact().get(), interval(0, 20))).getValidTime());
        Assertions.assertEquals(interval(20, 40), version(after, changeId(retry,
            retry.getEvent().getFact().get(), interval(20, 40))).getValidTime());
        Assertions.assertEquals(interval(40, 100), version(after, changeId(retry,
            source.getEvent().getFact().get(), interval(40, 100))).getValidTime());
        Assertions.assertEquals("Munich",
            version(after, addId(stagedRetry)).getFact().getLiteralValue().get());
        Assertions.assertEquals(oracle.replayNormalized(Arrays.asList(source, stagedRetry, retry)),
            after);
    }

    @Test
    public void testBatchIdempotencyDecisionsAndEventIdReuseRollback() {
        IncrementalTemporalIntegrator integrator = new IncrementalTemporalIntegrator();
        NormalizedMemoryEvent source = add("source", PROFILE, "Beijing", VALID, 10);
        List<EventLedgerDecision> decisions = integrator.applyBatch(Arrays.asList(source, source));
        Assertions.assertEquals(Arrays.asList(EventLedgerDecision.ACCEPTED,
            EventLedgerDecision.DUPLICATE_NOOP), decisions);
        Assertions.assertThrows(UnsupportedOperationException.class, () -> decisions.clear());
        TemporalState before = integrator.stateSnapshot();
        NormalizedMemoryEvent staged = add("staged", ACCOUNT, "Paris", VALID, 5);
        NormalizedMemoryEvent reused = add("source", PROFILE, "Rome", VALID, 10);

        TemporalValidationException failure = Assertions.assertThrows(
            TemporalValidationException.class,
            () -> integrator.applyBatch(Arrays.asList(reused, staged)));

        Assertions.assertEquals(TemporalErrorCode.REJECT_EVENT_ID_REUSE, failure.getCode());
        Assertions.assertEquals(before, integrator.stateSnapshot());
        Assertions.assertEquals(Collections.singletonList(source),
            integrator.normalizedEventSnapshot());
        Assertions.assertEquals(Collections.singletonList(EventLedgerDecision.ACCEPTED),
            integrator.applyBatch(Collections.singletonList(staged)));
        Assertions.assertEquals(Arrays.asList(EventLedgerDecision.DUPLICATE_NOOP,
            EventLedgerDecision.DUPLICATE_NOOP),
            integrator.applyBatch(Arrays.asList(source, staged)));
        Assertions.assertEquals(oracle.replayNormalized(Arrays.asList(staged, source)),
            integrator.stateSnapshot());
    }

    @Test
    public void testSameTimestampCrossBatchReplayKeepsOnlyMaterializedEndpoints() {
        IncrementalTemporalIntegrator integrator = new IncrementalTemporalIntegrator();
        NormalizedMemoryEvent source = add("a-source", PROFILE, "Beijing", VALID, 10);
        NormalizedMemoryEvent conflict = add("b-conflict", PROFILE, "Paris", VALID, 10);
        NormalizedMemoryEvent change = correct(
            "c-change", PROFILE, "Shanghai", VALID, 10, addId(source));
        integrator.applyBatch(Collections.singletonList(source));
        integrator.applyBatch(Collections.singletonList(change));
        Assertions.assertEquals(1, integrator.stateSnapshot().getVersions().size());
        Assertions.assertTrue(integrator.stateSnapshot().getRelations().isEmpty());

        integrator.applyBatch(Collections.singletonList(conflict));

        TemporalState state = integrator.stateSnapshot();
        String changedId = changeId(change, change.getEvent().getFact().get(), VALID);
        Assertions.assertEquals(2, state.getVersions().size());
        Assertions.assertEquals(Collections.singletonList(new VersionRelation(
            VersionRelationType.CONFLICTS_WITH, addId(conflict), changedId)), state.getRelations());
        Set<String> ids = state.getVersions().stream().map(MemoryFactVersion::getId)
            .collect(Collectors.toSet());
        Assertions.assertFalse(ids.contains(addId(source)));
        for (MemoryFactVersion version : state.getVersions()) {
            Assertions.assertEquals(MemoryFactVersionStatus.ACTIVE, version.getStatus());
            Assertions.assertEquals(TimeInterval.unboundedFrom(at(10)),
                version.getTransactionTime());
        }
        for (VersionRelation relation : state.getRelations()) {
            Assertions.assertTrue(ids.contains(relation.getFromVersionId()));
            Assertions.assertTrue(ids.contains(relation.getToVersionId()));
        }
        Assertions.assertEquals(Arrays.asList(source, conflict, change),
            integrator.normalizedEventSnapshot());
        Assertions.assertEquals(oracle.replayNormalized(Arrays.asList(change, source, conflict)),
            state);
        IncrementalTemporalIntegrator singleBatch = new IncrementalTemporalIntegrator();
        singleBatch.applyBatch(Arrays.asList(change, conflict, source));
        Assertions.assertEquals(state, singleBatch.stateSnapshot());
    }

    @Test
    public void testLateAddCannotRetargetCorrectionOrRenumberItsResidual() {
        IncrementalTemporalIntegrator integrator = new IncrementalTemporalIntegrator();
        NormalizedMemoryEvent source = add("source", PROFILE, "Beijing", VALID, 10);
        NormalizedMemoryEvent change = correct(
            "change", PROFILE, "Shanghai", interval(30, 180), 20, addId(source));
        integrator.applyBatch(Arrays.asList(change, source));
        TemporalState before = integrator.stateSnapshot();
        String residualId = changeId(change, source.getEvent().getFact().get(), interval(0, 30));
        String replacementId = changeId(change,
            change.getEvent().getFact().get(), interval(30, 100));
        Assertions.assertEquals(interval(0, 30), version(before, residualId).getValidTime());
        Assertions.assertEquals(interval(30, 100), version(before, replacementId).getValidTime());
        NormalizedMemoryEvent lateAdd = add(
            "late-add", PROFILE, "Rome", interval(150, 200), 15);
        NormalizedMemoryEvent account = add("account", ACCOUNT, "Tokyo", VALID, 14);

        integrator.applyBatch(Arrays.asList(lateAdd, account));

        TemporalState after = integrator.stateSnapshot();
        Assertions.assertEquals(5, after.getVersions().size());
        for (MemoryFactVersion version : before.getVersions()) {
            Assertions.assertEquals(version, version(after, version.getId()));
        }
        Assertions.assertEquals(interval(150, 200), version(after, addId(lateAdd)).getValidTime());
        Assertions.assertFalse(version(after, addId(lateAdd))
            .getTransactionTime().getEnd().isPresent());
        Assertions.assertEquals("Rome",
            version(after, addId(lateAdd)).getFact().getLiteralValue().get());
        Assertions.assertEquals(
            oracle.replayNormalized(Arrays.asList(source, account, lateAdd, change)), after);
    }

    @Test
    public void testAddMergesCompleteSameValueChainAfterTargetedCorrection() {
        IncrementalTemporalIntegrator integrator = new IncrementalTemporalIntegrator();
        NormalizedMemoryEvent source = add("source", PROFILE, "Beijing", VALID, 10);
        NormalizedMemoryEvent conflict = add(
            "conflict", PROFILE, "Shanghai", interval(30, 60), 20);
        NormalizedMemoryEvent change = correct(
            "change", PROFILE, "Shanghai", interval(40, 70), 30, addId(source));
        NormalizedMemoryEvent duplicate = add(
            "duplicate", PROFILE, "Shanghai", interval(60, 80), 40);
        integrator.applyBatch(Arrays.asList(change, conflict, source));
        integrator.applyBatch(Collections.singletonList(duplicate));

        TemporalState state = integrator.stateSnapshot();
        String mergedId = VersionIdentity.create(duplicate, duplicate.getEvent().getFact().get(),
            MemoryFactVersionStatus.ACTIVE, interval(30, 80), null);
        MemoryFactVersion merged = version(state, mergedId);
        Assertions.assertEquals(interval(30, 80), merged.getValidTime());
        Assertions.assertEquals(Arrays.asList(evidence("change").get(0),
            evidence("conflict").get(0), evidence("duplicate").get(0)), merged.getEvidence());
        Assertions.assertEquals(1, state.getVersions().stream()
            .filter(version -> !version.getTransactionTime().getEnd().isPresent())
            .filter(version -> version.getFact().getLiteralValue().get().equals("Shanghai"))
            .count());
        Assertions.assertTrue(state.getRelations().contains(new VersionRelation(
            VersionRelationType.DUPLICATE_OF, mergedId, addId(conflict))));
        Assertions.assertTrue(state.getRelations().contains(new VersionRelation(
            VersionRelationType.DUPLICATE_OF, mergedId,
            changeId(change, change.getEvent().getFact().get(), interval(40, 70)))));
        Assertions.assertEquals(oracle.replayNormalized(
            Arrays.asList(source, conflict, change, duplicate)), state);
    }

    private NormalizedMemoryEvent add(
        String id, FactKey key, String value, TimeInterval valid, long recordedAt) {
        return normalizer.normalize(MemoryEvent.add(id, fact(key, value), valid, at(recordedAt),
            evidence(id)), key);
    }

    private NormalizedMemoryEvent correct(
        String id,
        FactKey key,
        String value,
        TimeInterval valid,
        long recordedAt,
        String targetId) {
        return normalizer.normalize(MemoryEvent.correct(id, fact(key, value), valid, at(recordedAt),
            evidence(id)).targeting(targetId), key);
    }

    private static MemoryFact fact(FactKey key, String value) {
        return MemoryFact.attribute("fact-shared", new MemoryEntity(key.getSubjectId(), "person"),
            key.getPredicate(), value);
    }

    private static List<Evidence> evidence(String id) {
        return Collections.singletonList(new Evidence("evidence-" + id,
            new Source("source", "fixture"), "Evidence for " + id));
    }

    private static String addId(NormalizedMemoryEvent event) {
        return VersionIdentity.create(event, event.getEvent().getFact().get(),
            MemoryFactVersionStatus.ACTIVE, event.getValidTime(), null);
    }

    private static String changeId(
        NormalizedMemoryEvent event, MemoryFact fact, TimeInterval valid) {
        return VersionIdentity.create(event, fact, MemoryFactVersionStatus.ACTIVE, valid,
            event.getTargetVersionId().get());
    }

    private static MemoryFactVersion version(TemporalState state, String id) {
        return state.getVersions().stream().filter(version -> version.getId().equals(id))
            .findFirst().orElseThrow(() -> new AssertionError("Missing version: " + id));
    }

    private static TimeInterval interval(long start, long end) {
        return new TimeInterval(at(start), at(end));
    }

    private static Instant at(long nanos) {
        return BASE.plusNanos(nanos);
    }
}
