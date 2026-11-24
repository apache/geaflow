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
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.apache.geaflow.ai.temporal.adapter.MemoryGraphAdapter;
import org.apache.geaflow.ai.temporal.model.Evidence;
import org.apache.geaflow.ai.temporal.model.FactKey;
import org.apache.geaflow.ai.temporal.model.MemoryEntity;
import org.apache.geaflow.ai.temporal.model.MemoryEvent;
import org.apache.geaflow.ai.temporal.model.MemoryFact;
import org.apache.geaflow.ai.temporal.model.MemoryFactVersionStatus;
import org.apache.geaflow.ai.temporal.model.Source;
import org.apache.geaflow.ai.temporal.model.TimeInterval;
import org.apache.geaflow.ai.temporal.oracle.FullReplayOracle;
import org.apache.geaflow.ai.temporal.oracle.ReplayMethod;
import org.apache.geaflow.ai.temporal.semantics.CanonicalSnapshot;
import org.apache.geaflow.ai.temporal.semantics.EventLedgerDecision;
import org.apache.geaflow.ai.temporal.semantics.EventNormalizer;
import org.apache.geaflow.ai.temporal.semantics.NormalizedMemoryEvent;
import org.apache.geaflow.ai.temporal.semantics.TemporalErrorCode;
import org.apache.geaflow.ai.temporal.semantics.TemporalValidationException;
import org.apache.geaflow.ai.temporal.semantics.VersionIdentity;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CanonicalReplaySnapshotTest {

    private static final Instant BASE = Instant.parse("2024-01-01T00:00:00Z");
    private static final FactKey KEY = new FactKey("alice", "city", "profile");
    private final EventNormalizer normalizer = new EventNormalizer();
    private final FullReplayOracle oracle = new FullReplayOracle();

    @Test
    public void testEmptySnapshotsAndReplayMethodContract() {
        ReplayMethod replay = oracle;
        CanonicalSnapshot empty = replay.replayToSnapshot(Collections.emptyList());
        Assertions.assertEquals(new IncrementalTemporalIntegrator().canonicalSnapshot(), empty);
        Assertions.assertTrue(empty.getGeneratingEventIds().isEmpty());
        Assertions.assertThrows(NullPointerException.class, () -> replay.replayToSnapshot(null));
    }

    @Test
    public void testGraphRoundTripPreservesLifecycleAndGeneratingEvents() {
        NormalizedMemoryEvent first = add("first", "Beijing", interval(0, 100), 10);
        NormalizedMemoryEvent conflict = add("conflict", "Shanghai", interval(20, 80), 20);
        NormalizedMemoryEvent duplicate = add("duplicate", "Beijing", interval(10, 90), 30);
        String firstId = id(first, interval(0, 100), null);
        String duplicateId = id(duplicate, interval(0, 100), null);
        NormalizedMemoryEvent correction = correct("correction", "Chengdu",
            interval(30, 70), 40, duplicateId);
        String correctedId = id(correction, interval(30, 70), duplicateId);
        NormalizedMemoryEvent retraction = retract("retraction", interval(40, 50), 50,
            correctedId);
        List<NormalizedMemoryEvent> events = Arrays.asList(
            retraction, correction, duplicate, first, conflict, first);
        IncrementalTemporalIntegrator integrator = new IncrementalTemporalIntegrator();
        integrator.applyBatch(events);

        CanonicalSnapshot snapshot = oracle.replayToSnapshot(events);
        Map<String, String> expectedGenerating = new TreeMap<>();
        expectedGenerating.put(firstId, first.getEventId());
        expectedGenerating.put(id(conflict, interval(20, 80), null), conflict.getEventId());
        expectedGenerating.put(duplicateId, duplicate.getEventId());
        expectedGenerating.put(correctedId, correction.getEventId());
        expectedGenerating.put(VersionIdentity.create(correction, fact("Beijing"),
            MemoryFactVersionStatus.ACTIVE, interval(0, 30), duplicateId), correction.getEventId());
        expectedGenerating.put(VersionIdentity.create(correction, fact("Beijing"),
            MemoryFactVersionStatus.ACTIVE, interval(70, 100), duplicateId), correction.getEventId());
        expectedGenerating.put(VersionIdentity.create(retraction, fact("Chengdu"),
            MemoryFactVersionStatus.RETRACTED, interval(40, 50), correctedId), retraction.getEventId());
        expectedGenerating.put(VersionIdentity.create(retraction, fact("Chengdu"),
            MemoryFactVersionStatus.ACTIVE, interval(30, 40), correctedId), retraction.getEventId());
        expectedGenerating.put(VersionIdentity.create(retraction, fact("Chengdu"),
            MemoryFactVersionStatus.ACTIVE, interval(50, 70), correctedId), retraction.getEventId());

        Assertions.assertEquals(expectedGenerating, snapshot.getGeneratingEventIds());
        Assertions.assertEquals(snapshot, integrator.canonicalSnapshot());
        Assertions.assertEquals(oracle.replayNormalized(events), snapshot.getState());
        Assertions.assertEquals(5, snapshot.getEvents().size());
        MemoryGraphAdapter adapter = new MemoryGraphAdapter();
        Assertions.assertEquals(snapshot, adapter.fromGraph(adapter.toGraph(snapshot)));
        Assertions.assertThrows(UnsupportedOperationException.class,
            () -> snapshot.getGeneratingEventIds().clear());
        Assertions.assertThrows(UnsupportedOperationException.class,
            () -> integrator.normalizedEventSnapshot().clear());
    }

    @Test
    public void testLateReplayReplacesObsoleteIdentityAndPreservesOtherKeys() {
        NormalizedMemoryEvent later = add("later", "Beijing", interval(10, 20), 30);
        NormalizedMemoryEvent earlier = add("earlier", "Beijing", interval(0, 15), 10);
        FactKey account = new FactKey("alice", "city", "account");
        NormalizedMemoryEvent unrelated = normalizer.normalize(MemoryEvent.add("account",
            fact("Rome"), interval(0, 100), at(20), evidence("account")), account);
        IncrementalTemporalIntegrator integrator = new IncrementalTemporalIntegrator();
        integrator.applyBatch(Arrays.asList(later, unrelated));
        CanonicalSnapshot before = integrator.canonicalSnapshot();
        integrator.apply(earlier);

        CanonicalSnapshot snapshot = integrator.canonicalSnapshot();
        Assertions.assertEquals(oracle.replayToSnapshot(Arrays.asList(later, unrelated, earlier)),
            snapshot);
        Assertions.assertFalse(snapshot.getGeneratingEventIds().containsKey(
            id(later, interval(10, 20), null)));
        Assertions.assertEquals(later.getEventId(), snapshot.getGeneratingEventIds().get(
            id(later, interval(0, 20), null)));
        Assertions.assertEquals(earlier.getEventId(), snapshot.getGeneratingEventIds().get(
            id(earlier, interval(0, 15), null)));
        Assertions.assertEquals(before.getState().getVersionsByFactKey().get(account),
            snapshot.getState().getVersionsByFactKey().get(account));
        Assertions.assertEquals(2, before.getGeneratingEventIds().size());
    }

    @Test
    public void testSameTimeOmittedVersionsLoseMappingButKeepTheirEvents() {
        NormalizedMemoryEvent first = add("a", "Beijing", interval(0, 100), 10);
        NormalizedMemoryEvent duplicate = add("b", "Beijing", interval(10, 90), 10);
        String duplicateId = id(duplicate, interval(0, 100), null);
        NormalizedMemoryEvent correction = correct("c", "Rome", interval(30, 70), 10,
            duplicateId);
        String correctedId = id(correction, interval(30, 70), duplicateId);
        NormalizedMemoryEvent retraction = retract("d", interval(40, 50), 10, correctedId);
        IncrementalTemporalIntegrator integrator = new IncrementalTemporalIntegrator();
        integrator.apply(first);
        integrator.applyBatch(Arrays.asList(correction, duplicate));
        integrator.apply(retraction);

        CanonicalSnapshot snapshot = integrator.canonicalSnapshot();
        Assertions.assertEquals(oracle.replayToSnapshot(
            Arrays.asList(retraction, duplicate, first, correction)), snapshot);
        Assertions.assertEquals(4, snapshot.getEvents().size());
        Assertions.assertEquals(5, snapshot.getGeneratingEventIds().size());
        Assertions.assertFalse(snapshot.getGeneratingEventIds().containsKey(
            id(first, interval(0, 100), null)));
        Assertions.assertFalse(snapshot.getGeneratingEventIds().containsKey(duplicateId));
        Assertions.assertFalse(snapshot.getGeneratingEventIds().containsKey(correctedId));
        Assertions.assertEquals(2, snapshot.getGeneratingEventIds().values().stream()
            .filter(correction.getEventId()::equals).count());
        Assertions.assertEquals(3, snapshot.getGeneratingEventIds().values().stream()
            .filter(retraction.getEventId()::equals).count());
        Assertions.assertTrue(snapshot.getState().getRelations().isEmpty());
        Assertions.assertEquals(snapshot.getGeneratingEventIds().keySet(), snapshot.getState()
            .getVersions().stream().map(version -> version.getId()).collect(Collectors.toSet()));
    }

    @Test
    public void testRejectedBatchRollsBackGeneratingEventsAndAllowsRetry() {
        NormalizedMemoryEvent first = add("a", "Beijing", interval(0, 100), 10);
        NormalizedMemoryEvent duplicate = add("b", "Beijing", interval(10, 90), 10);
        NormalizedMemoryEvent invalid = retract("invalid", interval(30, 40), 20, "missing");
        IncrementalTemporalIntegrator integrator = new IncrementalTemporalIntegrator();
        integrator.apply(first);
        CanonicalSnapshot before = integrator.canonicalSnapshot();

        Assertions.assertEquals(TemporalErrorCode.REJECT_TARGET_NOT_FOUND,
            Assertions.assertThrows(TemporalValidationException.class,
                () -> integrator.applyBatch(Arrays.asList(duplicate, invalid))).getCode());
        Assertions.assertEquals(before, integrator.canonicalSnapshot());
        Assertions.assertEquals(Arrays.asList(EventLedgerDecision.ACCEPTED,
            EventLedgerDecision.DUPLICATE_NOOP), integrator.applyBatch(
                Arrays.asList(duplicate, duplicate)));
        Assertions.assertEquals(oracle.replayToSnapshot(Arrays.asList(first, duplicate)),
            integrator.canonicalSnapshot());
        Assertions.assertEquals(before.getGeneratingEventIds(),
            Collections.singletonMap(id(first, interval(0, 100), null), first.getEventId()));

        CanonicalSnapshot committed = integrator.canonicalSnapshot();
        NormalizedMemoryEvent reused = add("b", "Shanghai", interval(10, 90), 10);
        Assertions.assertEquals(TemporalErrorCode.REJECT_EVENT_ID_REUSE,
            Assertions.assertThrows(TemporalValidationException.class,
                () -> integrator.applyBatch(Collections.singletonList(reused))).getCode());
        Assertions.assertEquals(committed, integrator.canonicalSnapshot());
    }

    @Test
    public void testLateBatchReplaysCompleteTargetDependenciesAndProvenanceAtomically() {
        NormalizedMemoryEvent first = add("a", "Beijing", interval(0, 10), 20);
        NormalizedMemoryEvent correction = correct("c", "Rome", interval(2, 8), 30,
            id(first, interval(0, 10), null));
        NormalizedMemoryEvent earlier = add("b", "Beijing", interval(5, 15), 10);
        String earlierId = id(earlier, interval(5, 15), null);
        NormalizedMemoryEvent invalid = retract("r", interval(15, 20), 15, earlierId);
        IncrementalTemporalIntegrator integrator = new IncrementalTemporalIntegrator();
        integrator.applyBatch(Arrays.asList(correction, first));
        CanonicalSnapshot before = integrator.canonicalSnapshot();

        Assertions.assertThrows(TemporalValidationException.class, () -> integrator.apply(earlier));
        Assertions.assertEquals(before, integrator.canonicalSnapshot());
        Assertions.assertEquals(TemporalErrorCode.REJECT_TARGET_NO_OVERLAP,
            Assertions.assertThrows(TemporalValidationException.class,
                () -> integrator.applyBatch(Arrays.asList(invalid, earlier))).getCode());
        Assertions.assertEquals(before, integrator.canonicalSnapshot());

        NormalizedMemoryEvent retry = retract("r", interval(10, 15), 15, earlierId);
        integrator.applyBatch(Arrays.asList(retry, earlier));
        CanonicalSnapshot snapshot = integrator.canonicalSnapshot();
        Assertions.assertEquals(oracle.replayToSnapshot(
            Arrays.asList(correction, earlier, retry, first)), snapshot);
        Assertions.assertEquals(first.getEventId(), snapshot.getGeneratingEventIds().get(
            id(first, interval(0, 10), null)));
        Assertions.assertEquals(earlier.getEventId(), snapshot.getGeneratingEventIds().get(earlierId));
    }

    private NormalizedMemoryEvent add(String id, String value, TimeInterval valid, long recorded) {
        return normalizer.normalize(MemoryEvent.add(id, fact(value), valid, at(recorded),
            evidence(id)), KEY);
    }

    private NormalizedMemoryEvent correct(
        String id, String value, TimeInterval valid, long recorded, String target) {
        return normalizer.normalize(MemoryEvent.correct(id, fact(value), valid, at(recorded),
            evidence(id)).targeting(target), KEY);
    }

    private NormalizedMemoryEvent retract(
        String id, TimeInterval valid, long recorded, String target) {
        return normalizer.normalize(MemoryEvent.retract(id, "fact-city", valid, at(recorded),
            evidence(id)).targeting(target), KEY);
    }

    private static String id(NormalizedMemoryEvent event, TimeInterval valid, String target) {
        return VersionIdentity.create(event, event.getEvent().getFact().get(),
            MemoryFactVersionStatus.ACTIVE, valid, target);
    }

    private static MemoryFact fact(String value) {
        return MemoryFact.attribute("fact-city", new MemoryEntity("alice", "person"),
            "city", value);
    }

    private static List<Evidence> evidence(String id) {
        return Collections.singletonList(new Evidence("evidence-" + id,
            new Source("registry", "Registry"), "Evidence for " + id));
    }

    private static TimeInterval interval(long start, long end) {
        return new TimeInterval(at(start), at(end));
    }

    private static Instant at(long nanos) {
        return BASE.plusNanos(nanos);
    }
}
