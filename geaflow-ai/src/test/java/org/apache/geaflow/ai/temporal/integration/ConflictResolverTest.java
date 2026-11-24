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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.apache.geaflow.ai.temporal.semantics.EventNormalizer;
import org.apache.geaflow.ai.temporal.semantics.NormalizedMemoryEvent;
import org.apache.geaflow.ai.temporal.semantics.TemporalErrorCode;
import org.apache.geaflow.ai.temporal.semantics.TemporalState;
import org.apache.geaflow.ai.temporal.semantics.TemporalValidationException;
import org.apache.geaflow.ai.temporal.semantics.VersionIdentity;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ConflictResolverTest {

    private static final Instant BASE = Instant.parse("2024-01-01T00:00:00Z");
    private static final FactKey KEY = new FactKey("alice", "city", "profile");

    private final EventNormalizer normalizer = new EventNormalizer();
    private final ConflictResolver resolver = new ConflictResolver();

    @Test
    public void testDuplicateClosureIncludesConflictBeyondOriginalEventInterval() {
        final MemoryFactVersion first = current("first", "Paris", interval(0, 50));
        final MemoryFactVersion bridge = current("bridge", "Paris", interval(40, 70));
        final MemoryFactVersion conflict = current("conflict", "Rome", interval(10, 20));
        final MemoryFactVersion historical = new MemoryFactVersion("historical", fact("Paris"),
            interval(-100, 100), interval(1, 2), evidence("historical"));
        final MemoryFactVersion tombstone = new MemoryFactVersion("tombstone", fact("Paris"),
            MemoryFactVersionStatus.RETRACTED, interval(-100, 100),
            TimeInterval.unboundedFrom(at(2)), evidence("tombstone"));
        final List<MemoryFactVersion> versions = new ArrayList<>(
            Arrays.asList(first, bridge, conflict, historical, tombstone));
        final List<MemoryFactVersion> before = new ArrayList<>(versions);
        final NormalizedMemoryEvent event = add("merge", KEY, "Paris", interval(60, 80), 40);

        final ResolutionDecision decision = resolver.resolve(event, versions);

        Assertions.assertEquals(interval(0, 80), decision.getEffectiveValidTime());
        Assertions.assertEquals(Arrays.asList("bridge", "conflict", "first"),
            decision.getMandatoryVersionIds());
        Assertions.assertEquals(decision.getMandatoryVersionIds(), decision.getReadVersionIds());
        Assertions.assertEquals(Arrays.asList("MERGE_EQUAL_VALUE", "COEXIST_CONFLICT"),
            decision.getRules());
        Assertions.assertNull(decision.getTargetVersionId());
        Assertions.assertEquals(before, versions);
        Assertions.assertThrows(UnsupportedOperationException.class,
            () -> decision.getMandatoryVersionIds().clear());
        Assertions.assertThrows(UnsupportedOperationException.class,
            () -> decision.getReadVersionIds().clear());
        Assertions.assertThrows(UnsupportedOperationException.class,
            () -> decision.getRules().clear());
        Collections.reverse(versions);
        final ResolutionDecision reversed = resolver.resolve(event, versions);
        Assertions.assertEquals(decision.getEffectiveValidTime(), reversed.getEffectiveValidTime());
        Assertions.assertEquals(decision.getMandatoryVersionIds(),
            reversed.getMandatoryVersionIds());
        Assertions.assertEquals(decision.getRules(), reversed.getRules());
    }

    @Test
    public void testAdjacentSameValueIsNewAssertionWithoutAnOverlap() {
        final NormalizedMemoryEvent event = add("adjacent", KEY, "Paris", interval(20, 30), 40);
        final ResolutionDecision decision = resolver.resolve(event,
            Collections.singletonList(current("existing", "Paris", interval(0, 20))));

        Assertions.assertEquals(event.getValidTime(), decision.getEffectiveValidTime());
        Assertions.assertTrue(decision.getMandatoryVersionIds().isEmpty());
        Assertions.assertTrue(decision.getReadVersionIds().isEmpty());
        Assertions.assertEquals(Collections.singletonList("ASSERT_NEW"), decision.getRules());
    }

    @Test
    public void testApplyUsesClosureAndPreservesEvidenceAndCanonicalRelations() {
        final MemoryFactVersion first = current("first", "Paris", interval(0, 50));
        final MemoryFactVersion bridge = current("bridge", "Paris", interval(40, 70));
        final MemoryFactVersion conflict = current("conflict", "Rome", interval(10, 20));
        final List<MemoryFactVersion> versions =
            new ArrayList<>(Arrays.asList(first, bridge, conflict));
        final List<VersionRelation> relations = new ArrayList<>();
        final Map<String, String> provenance = new HashMap<>();
        final NormalizedMemoryEvent event = add("merge", KEY, "Paris", interval(60, 80), 40);

        resolver.apply(event, versions, relations, provenance);

        final String mergedId = id(event, event.getEvent().getFact().get(), interval(0, 80),
            MemoryFactVersionStatus.ACTIVE);
        final MemoryFactVersion merged = versions.stream().filter(v -> v.getId().equals(mergedId))
            .findFirst().orElseThrow(() -> new AssertionError("Merged version missing"));
        Assertions.assertEquals(3, merged.getEvidence().size());
        Assertions.assertTrue(merged.getEvidence().containsAll(event.getEvidence()));
        Assertions.assertTrue(merged.getEvidence().containsAll(first.getEvidence()));
        Assertions.assertTrue(merged.getEvidence().containsAll(bridge.getEvidence()));
        Assertions.assertEquals("merge", provenance.get(mergedId));
        Assertions.assertEquals(3, relations.size());
        Assertions.assertTrue(relations.contains(new VersionRelation(
            VersionRelationType.DUPLICATE_OF, mergedId, first.getId())));
        Assertions.assertTrue(relations.contains(new VersionRelation(
            VersionRelationType.DUPLICATE_OF, mergedId, bridge.getId())));
        Assertions.assertTrue(relations.contains(new VersionRelation(
            VersionRelationType.CONFLICTS_WITH, mergedId, conflict.getId())));
        Assertions.assertEquals(2, versions.stream()
            .filter(v -> !v.getTransactionTime().getEnd().isPresent()).count());
    }

    @Test
    public void testCorrectionSelectsExactTargetAndKeepsBothResiduals() {
        final MemoryFactVersion target = current("target", "Paris", interval(0, 100));
        final MemoryFactVersion conflict = current("conflict", "Rome", interval(10, 90));
        final List<MemoryFactVersion> versions = new ArrayList<>(Arrays.asList(conflict, target));
        final NormalizedMemoryEvent event =
            change("correct", false, interval(20, 80), 40, "target");
        final ResolutionDecision decision = resolver.resolve(event, versions);
        Assertions.assertEquals("target", decision.getTargetVersionId());
        Assertions.assertEquals(Collections.singletonList("target"), decision.getReadVersionIds());
        Assertions.assertEquals(Arrays.asList("conflict", "target"),
            decision.getMandatoryVersionIds());
        Assertions.assertEquals(Collections.singletonList("CORRECT_TARGET"), decision.getRules());
        final List<VersionRelation> relations = new ArrayList<>();

        resolver.apply(event, versions, relations, new HashMap<>());

        Assertions.assertTrue(versions.contains(conflict));
        Assertions.assertEquals(5, versions.size());
        Assertions.assertEquals(3, relations.stream()
            .filter(r -> r.getType() == VersionRelationType.SUPERSEDES).count());
        for (TimeInterval remainder : Arrays.asList(interval(0, 20), interval(80, 100))) {
            MemoryFactVersion residual = versions.stream()
                .filter(v -> v.getId().equals(id(event, target.getFact(), remainder,
                    MemoryFactVersionStatus.ACTIVE)))
                .findFirst().orElseThrow(() -> new AssertionError("Residual missing"));
            Assertions.assertEquals(target.getEvidence(), residual.getEvidence());
            Assertions.assertTrue(relations.contains(new VersionRelation(
                VersionRelationType.CONFLICTS_WITH, residual.getId(), conflict.getId())));
        }
    }

    @Test
    public void testRetractionClipsTargetAndPreservesUncoveredTail() {
        final MemoryFactVersion target = current("target", "Paris", interval(0, 100));
        final List<MemoryFactVersion> versions = new ArrayList<>(Collections.singletonList(target));
        final NormalizedMemoryEvent event =
            change("retract", true, interval(-20, 30), 40, "target");
        final ResolutionDecision decision = resolver.resolve(event, versions);
        Assertions.assertEquals(interval(0, 30), decision.getEffectiveValidTime());
        Assertions.assertEquals(Collections.singletonList("RETRACT_TARGET"), decision.getRules());
        final List<VersionRelation> relations = new ArrayList<>();

        resolver.apply(event, versions, relations, new HashMap<>());

        Assertions.assertEquals(3, versions.size());
        final MemoryFactVersion tombstone = versions.stream()
            .filter(v -> v.getStatus() == MemoryFactVersionStatus.RETRACTED)
            .findFirst().orElseThrow(() -> new AssertionError("Tombstone missing"));
        Assertions.assertEquals(interval(0, 30), tombstone.getValidTime());
        Assertions.assertEquals(event.getEvidence(), tombstone.getEvidence());
        Assertions.assertEquals(id(event, target.getFact(), interval(0, 30),
            MemoryFactVersionStatus.RETRACTED), tombstone.getId());
        Assertions.assertTrue(versions.stream().anyMatch(v ->
            v.getValidTime().equals(interval(30, 100))
                && v.getEvidence().equals(target.getEvidence())));
        Assertions.assertEquals(2, relations.size());
    }

    @Test
    public void testMissingInactiveAndNonOverlappingTargetsNeverUseAnotherCandidate() {
        final MemoryFactVersion active = current("active", "Paris", interval(0, 100));
        final MemoryFactVersion closed = new MemoryFactVersion("closed", fact("Paris"),
            interval(0, 100), interval(1, 2), evidence("closed"));
        final MemoryFactVersion tombstone = new MemoryFactVersion("tombstone", fact("Paris"),
            MemoryFactVersionStatus.RETRACTED, interval(0, 100),
            TimeInterval.unboundedFrom(at(2)), evidence("tombstone"));
        final List<MemoryFactVersion> versions =
            new ArrayList<>(Arrays.asList(active, closed, tombstone));
        final List<MemoryFactVersion> before = new ArrayList<>(versions);
        assertRejected(change("missing", false, interval(20, 30), 40, "missing"), versions,
            TemporalErrorCode.REJECT_TARGET_NOT_FOUND);
        assertRejected(change("closed", false, interval(20, 30), 40, "closed"), versions,
            TemporalErrorCode.REJECT_TARGET_INACTIVE);
        assertRejected(change("tombstone", true, interval(20, 30), 40, "tombstone"), versions,
            TemporalErrorCode.REJECT_TARGET_INACTIVE);
        assertRejected(change("no-overlap", true, interval(100, 110), 40, "active"), versions,
            TemporalErrorCode.REJECT_TARGET_NO_OVERLAP);
        Assertions.assertEquals(before, versions);
    }

    @Test
    public void testWrongScopeTargetRejectedWithoutPublishingStateOrEvent() {
        final FactKey otherScope = new FactKey(KEY.getSubjectId(), KEY.getPredicate(), "account");
        final NormalizedMemoryEvent source =
            add("account", otherScope, "Paris", interval(0, 100), 10);
        final IncrementalTemporalIntegrator integrator = new IncrementalTemporalIntegrator();
        integrator.apply(source);
        final TemporalState before = integrator.stateSnapshot();
        final String targetId = id(source, source.getEvent().getFact().get(), source.getValidTime(),
            MemoryFactVersionStatus.ACTIVE);
        final NormalizedMemoryEvent event =
            change("wrong-scope", false, interval(20, 30), 40, targetId);

        final TemporalValidationException failure = Assertions.assertThrows(
            TemporalValidationException.class, () -> integrator.apply(event));

        Assertions.assertEquals(TemporalErrorCode.REJECT_TARGET_NOT_FOUND, failure.getCode());
        Assertions.assertEquals(before, integrator.stateSnapshot());
        Assertions.assertEquals(Collections.singletonList(source),
            integrator.normalizedEventSnapshot());
    }

    @Test
    public void testSameTimestampAndLateArrivalMatchIndependentOracleWithoutDanglingRelations() {
        final NormalizedMemoryEvent source = add("a-source", KEY, "Paris", interval(0, 100), 10);
        final NormalizedMemoryEvent conflict = add("b-conflict", KEY, "Rome", interval(10, 90), 10);
        final NormalizedMemoryEvent change = change("c-correct", false, interval(20, 80), 10,
            id(source, source.getEvent().getFact().get(), source.getValidTime(),
                MemoryFactVersionStatus.ACTIVE));
        final NormalizedMemoryEvent duplicate = add("d-merge", KEY, "London", interval(30, 70), 20);
        final List<NormalizedMemoryEvent> events =
            Arrays.asList(source, conflict, change, duplicate);
        final IncrementalTemporalIntegrator sequential = new IncrementalTemporalIntegrator();
        sequential.apply(source);
        sequential.apply(change);
        sequential.apply(duplicate);
        sequential.apply(conflict);
        final IncrementalTemporalIntegrator batched = new IncrementalTemporalIntegrator();
        batched.applyBatch(Arrays.asList(duplicate, change, source, conflict));
        final TemporalState expected = new FullReplayOracle().replayNormalized(events);

        Assertions.assertEquals(expected, sequential.stateSnapshot());
        Assertions.assertEquals(expected, batched.stateSnapshot());
        final Set<String> versionIds = expected.getVersions().stream().map(MemoryFactVersion::getId)
            .collect(Collectors.toSet());
        Assertions.assertFalse(versionIds.contains(id(source, source.getEvent().getFact().get(),
            source.getValidTime(), MemoryFactVersionStatus.ACTIVE)));
        for (VersionRelation relation : expected.getRelations()) {
            Assertions.assertTrue(versionIds.contains(relation.getFromVersionId()));
            Assertions.assertTrue(versionIds.contains(relation.getToVersionId()));
        }
    }

    private void assertRejected(NormalizedMemoryEvent event, List<MemoryFactVersion> versions,
                                TemporalErrorCode code) {
        final List<VersionRelation> relations = new ArrayList<>();
        final Map<String, String> provenance = new HashMap<>();
        final TemporalValidationException failure = Assertions.assertThrows(
            TemporalValidationException.class, () -> resolver.apply(event, versions,
                relations, provenance));
        Assertions.assertEquals(code, failure.getCode());
        Assertions.assertTrue(relations.isEmpty());
        Assertions.assertTrue(provenance.isEmpty());
    }

    private NormalizedMemoryEvent add(String eventId, FactKey key, String value,
                                      TimeInterval valid, long recordedAt) {
        return normalizer.normalize(MemoryEvent.add(eventId, fact(value), valid,
            at(recordedAt), evidence(eventId)), key);
    }

    private NormalizedMemoryEvent change(String eventId, boolean retract, TimeInterval valid,
                                         long recordedAt, String targetId) {
        final MemoryEvent event = retract
            ? MemoryEvent.retract(eventId, "fact-city", valid, at(recordedAt), evidence(eventId))
            : MemoryEvent.correct(eventId, fact("London"), valid, at(recordedAt),
                evidence(eventId));
        return normalizer.normalize(event.targeting(targetId), KEY);
    }

    private static MemoryFactVersion current(String versionId, String value, TimeInterval valid) {
        return new MemoryFactVersion(versionId, fact(value), valid,
            TimeInterval.unboundedFrom(at(5)), evidence(versionId));
    }

    private static MemoryFact fact(String value) {
        return MemoryFact.attribute("fact-city", new MemoryEntity(KEY.getSubjectId(), "person"),
            KEY.getPredicate(), value);
    }

    private static List<Evidence> evidence(String eventId) {
        return Collections.singletonList(new Evidence("evidence-" + eventId,
            new Source("source", "fixture"), "Evidence for " + eventId));
    }

    private static String id(NormalizedMemoryEvent event, MemoryFact fact, TimeInterval valid,
                              MemoryFactVersionStatus status) {
        return VersionIdentity.create(event, fact, status, valid,
            event.getTargetVersionId().orElse(null));
    }

    private static TimeInterval interval(long start, long end) {
        return new TimeInterval(at(start), at(end));
    }

    private static Instant at(long nanos) {
        return BASE.plusNanos(nanos);
    }
}
