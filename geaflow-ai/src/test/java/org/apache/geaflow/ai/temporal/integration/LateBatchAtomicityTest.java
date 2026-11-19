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
import org.apache.geaflow.ai.temporal.semantics.EventLedgerDecision;
import org.apache.geaflow.ai.temporal.semantics.EventNormalizer;
import org.apache.geaflow.ai.temporal.semantics.NormalizedMemoryEvent;
import org.apache.geaflow.ai.temporal.semantics.TemporalErrorCode;
import org.apache.geaflow.ai.temporal.semantics.TemporalState;
import org.apache.geaflow.ai.temporal.semantics.TemporalValidationException;
import org.apache.geaflow.ai.temporal.semantics.VersionIdentity;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class LateBatchAtomicityTest {

    private static final Instant BASE = Instant.parse("2024-01-01T00:00:00Z");
    private static final FactKey KEY = new FactKey("alice", "city", "profile");
    private final EventNormalizer normalizer = new EventNormalizer();
    private final FullReplayOracle oracle = new FullReplayOracle();

    @Test
    public void testLateBatchValidatesCompleteTimelineBeforeExistingFutureTargets() {
        NormalizedMemoryEvent assertion = add("a", interval(0, 10), 20);
        NormalizedMemoryEvent correction = correct("c", interval(2, 8), 30,
            addId(assertion));
        IncrementalTemporalIntegrator integrator = initialized(assertion, correction);
        TemporalState before = integrator.stateSnapshot();
        NormalizedMemoryEvent earlier = add("b", interval(5, 15), 10);
        NormalizedMemoryEvent retract = retract("r", interval(10, 15), 15, addId(earlier));

        // The incomplete prefix expands a's interval and temporarily changes its identity.
        TemporalValidationException prefixFailure = Assertions.assertThrows(
            TemporalValidationException.class, () -> integrator.apply(earlier));
        Assertions.assertEquals(TemporalErrorCode.REJECT_TARGET_NOT_FOUND, prefixFailure.getCode());
        Assertions.assertEquals(before, integrator.stateSnapshot());

        List<EventLedgerDecision> decisions = integrator.applyBatch(
            Arrays.asList(retract, earlier));

        Assertions.assertEquals(Arrays.asList(EventLedgerDecision.ACCEPTED,
            EventLedgerDecision.ACCEPTED), decisions);
        assertFinalTimeline(integrator, before, assertion, correction, earlier, retract);
    }

    @Test
    public void testRejectedLateBatchRollsBackTimelineAndAllowsChangedPayloadRetry() {
        NormalizedMemoryEvent assertion = add("a", interval(0, 10), 20);
        NormalizedMemoryEvent correction = correct("c", interval(2, 8), 30,
            addId(assertion));
        IncrementalTemporalIntegrator integrator = initialized(assertion, correction);
        TemporalState before = integrator.stateSnapshot();
        List<NormalizedMemoryEvent> eventsBefore = integrator.normalizedEventSnapshot();
        NormalizedMemoryEvent earlier = add("b", interval(5, 15), 10);
        NormalizedMemoryEvent invalid = retract("r", interval(15, 20), 15, addId(earlier));

        TemporalValidationException failure = Assertions.assertThrows(
            TemporalValidationException.class,
            () -> integrator.applyBatch(Arrays.asList(invalid, earlier)));

        Assertions.assertEquals(TemporalErrorCode.REJECT_TARGET_NO_OVERLAP, failure.getCode());
        Assertions.assertEquals(before, integrator.stateSnapshot());
        Assertions.assertEquals(eventsBefore, integrator.normalizedEventSnapshot());
        NormalizedMemoryEvent retry = retract("r", interval(10, 15), 15, addId(earlier));
        Assertions.assertEquals(Arrays.asList(EventLedgerDecision.ACCEPTED,
            EventLedgerDecision.ACCEPTED), integrator.applyBatch(Arrays.asList(retry, earlier)));
        assertFinalTimeline(integrator, before, assertion, correction, earlier, retry);
    }

    @Test
    public void testCrossKeyFailuresUseGlobalEventOrder() {
        FactKey account = new FactKey("alice", "city", "account");
        NormalizedMemoryEvent source = normalizer.normalize(
            MemoryEvent.add("account-source", fact("Beijing"), interval(0, 10), at(0),
                evidence("account-source")), account);
        NormalizedMemoryEvent earlierProfileError = correct(
            "profile-invalid", interval(0, 10), 10, "unknown-profile-version");
        NormalizedMemoryEvent laterAccountError = normalizer.normalize(
            MemoryEvent.correct("account-invalid", fact("Shanghai"), interval(10, 20), at(20),
                evidence("account-invalid")).targeting(addId(source)), account);
        List<NormalizedMemoryEvent> events = Arrays.asList(
            laterAccountError, earlierProfileError, source);
        IncrementalTemporalIntegrator integrator = new IncrementalTemporalIntegrator();

        TemporalValidationException oracleFailure = Assertions.assertThrows(
            TemporalValidationException.class, () -> oracle.replayNormalized(events));
        TemporalValidationException incrementalFailure = Assertions.assertThrows(
            TemporalValidationException.class, () -> integrator.applyBatch(events));

        Assertions.assertEquals(TemporalErrorCode.REJECT_TARGET_NOT_FOUND, oracleFailure.getCode());
        Assertions.assertEquals(oracleFailure.getCode(), incrementalFailure.getCode());
        Assertions.assertTrue(integrator.normalizedEventSnapshot().isEmpty());
        Assertions.assertTrue(integrator.stateSnapshot().getVersions().isEmpty());
        Assertions.assertEquals(Collections.singletonList(EventLedgerDecision.ACCEPTED),
            integrator.applyBatch(Collections.singletonList(source)));
    }

    @Test
    public void testEventIdReusePrecheckPrecedesEarlierInvalidTarget() {
        NormalizedMemoryEvent invalidTarget = correct(
            "invalid-target", interval(0, 10), 10, "unknown-version");
        NormalizedMemoryEvent original = add("reused", interval(0, 10), 20);
        NormalizedMemoryEvent reused = normalizer.normalize(
            MemoryEvent.add("reused", fact("Shanghai"), interval(0, 10), at(30),
                evidence("reused")), KEY);
        List<NormalizedMemoryEvent> events = Arrays.asList(reused, invalidTarget, original);
        IncrementalTemporalIntegrator integrator = new IncrementalTemporalIntegrator();

        TemporalValidationException oracleFailure = Assertions.assertThrows(
            TemporalValidationException.class, () -> oracle.replayNormalized(events));
        TemporalValidationException incrementalFailure = Assertions.assertThrows(
            TemporalValidationException.class, () -> integrator.applyBatch(events));

        Assertions.assertEquals(TemporalErrorCode.REJECT_EVENT_ID_REUSE, oracleFailure.getCode());
        Assertions.assertEquals(oracleFailure.getCode(), incrementalFailure.getCode());
        Assertions.assertTrue(integrator.normalizedEventSnapshot().isEmpty());
        Assertions.assertTrue(integrator.stateSnapshot().getVersions().isEmpty());
    }

    private IncrementalTemporalIntegrator initialized(
        NormalizedMemoryEvent assertion, NormalizedMemoryEvent correction) {
        IncrementalTemporalIntegrator integrator = new IncrementalTemporalIntegrator();
        integrator.applyBatch(Arrays.asList(correction, assertion));
        return integrator;
    }

    private void assertFinalTimeline(
        IncrementalTemporalIntegrator integrator,
        TemporalState before,
        NormalizedMemoryEvent assertion,
        NormalizedMemoryEvent correction,
        NormalizedMemoryEvent earlier,
        NormalizedMemoryEvent retract) {
        TemporalState actual = integrator.stateSnapshot();
        List<NormalizedMemoryEvent> ordered = Arrays.asList(
            earlier, retract, assertion, correction);
        Assertions.assertEquals(ordered, integrator.normalizedEventSnapshot());
        Assertions.assertEquals(oracle.replayNormalized(ordered), actual);
        String originalId = addId(assertion);
        Assertions.assertEquals(originalId, correction.getTargetVersionId().get());
        Assertions.assertEquals(interval(0, 10), version(actual, originalId).getValidTime());
        Assertions.assertEquals(interval(20, 30),
            version(actual, originalId).getTransactionTime());
        String replacementId = VersionIdentity.create(correction,
            correction.getEvent().getFact().get(), MemoryFactVersionStatus.ACTIVE,
            correction.getValidTime(), originalId);
        Assertions.assertEquals(version(before, replacementId), version(actual, replacementId));
        Assertions.assertEquals("Shanghai",
            version(actual, replacementId).getFact().getLiteralValue().get());
    }

    private NormalizedMemoryEvent add(String id, TimeInterval valid, long recordedAt) {
        return normalizer.normalize(MemoryEvent.add(id, fact("Beijing"), valid, at(recordedAt),
            evidence(id)), KEY);
    }

    private NormalizedMemoryEvent correct(
        String id, TimeInterval valid, long recordedAt, String targetId) {
        return normalizer.normalize(MemoryEvent.correct(id, fact("Shanghai"), valid, at(recordedAt),
            evidence(id)).targeting(targetId), KEY);
    }

    private NormalizedMemoryEvent retract(
        String id, TimeInterval valid, long recordedAt, String targetId) {
        return normalizer.normalize(MemoryEvent.retract(id, "fact-city", valid, at(recordedAt),
            evidence(id)).targeting(targetId), KEY);
    }

    private static MemoryFact fact(String value) {
        return MemoryFact.attribute("fact-city", new MemoryEntity("alice", "person"),
            "city", value);
    }

    private static List<Evidence> evidence(String id) {
        return Collections.singletonList(new Evidence("evidence-" + id,
            new Source("registry", "Registry"), "Evidence for " + id));
    }

    private static String addId(NormalizedMemoryEvent event) {
        return VersionIdentity.create(event, event.getEvent().getFact().get(),
            MemoryFactVersionStatus.ACTIVE, event.getValidTime(), null);
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
