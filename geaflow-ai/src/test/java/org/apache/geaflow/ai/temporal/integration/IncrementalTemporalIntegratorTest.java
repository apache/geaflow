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
import org.apache.geaflow.ai.temporal.model.MemoryEntity;
import org.apache.geaflow.ai.temporal.model.MemoryEvent;
import org.apache.geaflow.ai.temporal.model.MemoryFact;
import org.apache.geaflow.ai.temporal.model.MemoryFactVersion;
import org.apache.geaflow.ai.temporal.model.Source;
import org.apache.geaflow.ai.temporal.model.TimeInterval;
import org.apache.geaflow.ai.temporal.oracle.FullReplayOracle;
import org.apache.geaflow.ai.temporal.query.BitemporalQuery;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class IncrementalTemporalIntegratorTest {

    private final IncrementalTemporalIntegrator integrator =
        new IncrementalTemporalIntegrator();
    private final FullReplayOracle oracle = new FullReplayOracle();
    private final BitemporalQuery query = new BitemporalQuery();

    @Test
    public void testOrderedEventsMatchFullReplayAfterEachApply() {
        List<MemoryEvent> events = Arrays.asList(
            addEvent(
                "event-add",
                "fact-alice-city",
                "person:alice",
                "Beijing",
                "2024-01-01T00:00:00Z",
                "2024-03-01T00:00:00Z"),
            correctEvent(
                "event-correct",
                "fact-alice-city",
                "person:alice",
                "Shanghai",
                "2024-04-01T00:00:00Z",
                "2024-09-01T00:00:00Z",
                "2024-06-01T00:00:00Z"),
            retractEvent(
                "event-retract",
                "fact-alice-city",
                "2024-08-01T00:00:00Z",
                "2024-10-01T00:00:00Z",
                "2024-11-01T00:00:00Z"));

        assertMatchesAfterEachApply(events);
    }

    @Test
    public void testLateEventMatchesFullReplayAndKeepsOtherFact() {
        MemoryEvent aliceAdd = addEvent(
            "event-alice-add",
            "fact-alice-city",
            "person:alice",
            "Beijing",
            "2024-01-01T00:00:00Z",
            "2024-03-01T00:00:00Z");
        MemoryEvent bobAdd = addEvent(
            "event-bob-add",
            "fact-bob-city",
            "person:bob",
            "Paris",
            "2024-01-01T00:00:00Z",
            "2024-04-01T00:00:00Z");
        MemoryEvent correction = correctEvent(
            "event-alice-correct",
            "fact-alice-city",
            "person:alice",
            "Shanghai",
            "2024-04-01T00:00:00Z",
            "2024-09-01T00:00:00Z",
            "2024-06-01T00:00:00Z");
        MemoryEvent retract = retractEvent(
            "event-alice-retract",
            "fact-alice-city",
            "2024-08-01T00:00:00Z",
            "2024-10-01T00:00:00Z",
            "2024-11-01T00:00:00Z");
        MemoryEvent lateCorrection = correctEvent(
            "event-alice-late",
            "fact-alice-city",
            "person:alice",
            "Tianjin",
            "2024-02-01T00:00:00Z",
            "2024-03-01T00:00:00Z",
            "2024-05-15T00:00:00Z");

        List<MemoryEvent> arrivalOrder = Arrays.asList(
            aliceAdd,
            bobAdd,
            correction,
            retract,
            lateCorrection);
        assertMatchesAfterEachApply(arrivalOrder);

        List<MemoryFactVersion> visible = query.query(
            integrator.snapshot(),
            time("2024-02-15T00:00:00Z"),
            time("2024-12-01T00:00:00Z"));

        Assertions.assertEquals(
            lateCorrection.getFact().get(),
            findVersion(visible, "fact-alice-city").getFact());
        Assertions.assertEquals(
            bobAdd.getFact().get(),
            findVersion(visible, "fact-bob-city").getFact());
    }

    @Test
    public void testDuplicateAndConflictingEventIdAreAtomic() {
        MemoryEvent event = addEvent(
            "event-add",
            "fact-alice-city",
            "person:alice",
            "Beijing",
            "2024-01-01T00:00:00Z",
            "2024-03-01T00:00:00Z");
        MemoryEvent duplicate = addEvent(
            "event-add",
            "fact-alice-city",
            "person:alice",
            "Beijing",
            "2024-01-01T00:00:00Z",
            "2024-03-01T00:00:00Z");
        MemoryEvent conflict = addEvent(
            "event-add",
            "fact-alice-city",
            "person:alice",
            "Shanghai",
            "2024-01-01T00:00:00Z",
            "2024-03-01T00:00:00Z");

        integrator.apply(event);
        List<MemoryFactVersion> expected = integrator.snapshot();

        integrator.apply(duplicate);
        Assertions.assertEquals(expected, integrator.snapshot());
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> integrator.apply(conflict));
        Assertions.assertEquals(expected, integrator.snapshot());
    }

    @Test
    public void testInvalidEventDoesNotChangeState() {
        MemoryEvent add = addEvent(
            "event-add",
            "fact-alice-city",
            "person:alice",
            "Beijing",
            "2024-01-01T00:00:00Z",
            "2024-03-01T00:00:00Z");
        MemoryEvent invalid = correctEvent(
            "event-change",
            "fact-alice-city",
            "person:alice",
            "Shanghai",
            "2023-12-01T00:00:00Z",
            "2024-02-01T00:00:00Z",
            "2024-06-01T00:00:00Z");
        MemoryEvent retry = correctEvent(
            "event-change",
            "fact-alice-city",
            "person:alice",
            "Shanghai",
            "2024-04-01T00:00:00Z",
            "2024-09-01T00:00:00Z",
            "2024-06-01T00:00:00Z");

        integrator.apply(add);
        List<MemoryFactVersion> before = integrator.snapshot();

        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> integrator.apply(invalid));
        Assertions.assertEquals(before, integrator.snapshot());

        integrator.apply(retry);
        Assertions.assertEquals(
            oracle.replay(Arrays.asList(add, retry)),
            integrator.snapshot());
    }

    @Test
    public void testSnapshotIsDeterministicAndImmutable() {
        MemoryEvent bob = addEvent(
            "event-b",
            "fact-bob-city",
            "person:bob",
            "Paris",
            "2024-01-01T00:00:00Z",
            "2024-03-01T00:00:00Z");
        MemoryEvent alice = addEvent(
            "event-a",
            "fact-alice-city",
            "person:alice",
            "Beijing",
            "2024-01-01T00:00:00Z",
            "2024-03-01T00:00:00Z");

        integrator.apply(bob);
        integrator.apply(alice);
        List<MemoryFactVersion> snapshot = integrator.snapshot();

        Assertions.assertEquals(2, snapshot.size());
        Assertions.assertEquals(
            "event-a:version:0",
            snapshot.get(0).getId());
        Assertions.assertEquals(
            "event-b:version:0",
            snapshot.get(1).getId());
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () -> snapshot.clear());
    }

    @Test
    public void testEventSnapshotIsDeterministicAndImmutable() {
        MemoryEvent tieLater = addEvent(
            "event-b",
            "fact-bob-city",
            "person:bob",
            "Paris",
            "2024-01-01T00:00:00Z",
            "2024-04-01T00:00:00Z");
        MemoryEvent early = addEvent(
            "event-c",
            "fact-carol-city",
            "person:carol",
            "Rome",
            "2024-01-01T00:00:00Z",
            "2024-03-01T00:00:00Z");
        MemoryEvent tieEarlier = addEvent(
            "event-a",
            "fact-alice-city",
            "person:alice",
            "Beijing",
            "2024-01-01T00:00:00Z",
            "2024-04-01T00:00:00Z");

        integrator.apply(tieLater);
        integrator.apply(early);
        integrator.apply(tieEarlier);
        integrator.apply(tieLater);
        List<MemoryEvent> snapshot = integrator.eventSnapshot();

        Assertions.assertEquals(
            Arrays.asList(early, tieEarlier, tieLater),
            snapshot);
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () -> snapshot.clear());
    }

    @Test
    public void testEmptyAndNullInput() {
        Assertions.assertTrue(integrator.snapshot().isEmpty());
        Assertions.assertThrows(
            NullPointerException.class,
            () -> integrator.apply((MemoryEvent) null));
        Assertions.assertTrue(integrator.snapshot().isEmpty());
    }

    private void assertMatchesAfterEachApply(
        List<MemoryEvent> arrivalOrder) {
        List<MemoryEvent> received = new ArrayList<>();
        for (MemoryEvent event : arrivalOrder) {
            integrator.apply(event);
            received.add(event);
            Assertions.assertEquals(
                oracle.replay(received),
                integrator.snapshot());
        }
    }

    private static MemoryFactVersion findVersion(
        List<MemoryFactVersion> versions,
        String factId) {
        for (MemoryFactVersion version : versions) {
            if (version.getFact().getId().equals(factId)) {
                return version;
            }
        }
        throw new AssertionError(
            "Missing fact version: " + factId);
    }

    private static MemoryEvent addEvent(
        String eventId,
        String factId,
        String subjectId,
        String literalValue,
        String validStart,
        String transactionTime) {
        return MemoryEvent.add(
            eventId,
            fact(factId, subjectId, literalValue),
            TimeInterval.unboundedFrom(time(validStart)),
            time(transactionTime),
            evidence(eventId));
    }

    private static MemoryEvent correctEvent(
        String eventId,
        String factId,
        String subjectId,
        String literalValue,
        String validStart,
        String validEnd,
        String transactionTime) {
        return MemoryEvent.correct(
            eventId,
            fact(factId, subjectId, literalValue),
            interval(validStart, validEnd),
            time(transactionTime),
            evidence(eventId));
    }

    private static MemoryEvent retractEvent(
        String eventId,
        String factId,
        String validStart,
        String validEnd,
        String transactionTime) {
        return MemoryEvent.retract(
            eventId,
            factId,
            interval(validStart, validEnd),
            time(transactionTime),
            evidence(eventId));
    }

    private static MemoryFact fact(
        String factId,
        String subjectId,
        String literalValue) {
        return MemoryFact.attribute(
            factId,
            new MemoryEntity(subjectId, "person"),
            "city",
            literalValue);
    }

    private static TimeInterval interval(
        String start,
        String end) {
        return new TimeInterval(time(start), time(end));
    }

    private static List<Evidence> evidence(String eventId) {
        return Collections.singletonList(new Evidence(
            "evidence-" + eventId,
            new Source("source-1", "customer-database"),
            "Evidence for " + eventId));
    }

    private static Instant time(String value) {
        return Instant.parse(value);
    }
}
