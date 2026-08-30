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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class FullReplayOracleTest {

    private final FullReplayOracle oracle = new FullReplayOracle();

    @Test
    public void testReplayAddEvent() {
        MemoryEvent event = addEvent(
            "event-1",
            "fact-name-alice",
            "person:alice",
            "Alice",
            "2024-01-01T00:00:00Z",
            "2024-03-01T00:00:00Z");

        List<MemoryFactVersion> versions =
            oracle.replay(Collections.singletonList(event));

        Assertions.assertEquals(1, versions.size());

        MemoryFactVersion version = versions.get(0);
        Assertions.assertEquals(
            "event-1:version:0",
            version.getId());
        Assertions.assertEquals(
            event.getFact().get(),
            version.getFact());
        Assertions.assertEquals(
            event.getValidTime(),
            version.getValidTime());
        Assertions.assertEquals(
            TimeInterval.unboundedFrom(
                event.getTransactionTime()),
            version.getTransactionTime());
        Assertions.assertEquals(
            event.getEvidence(),
            version.getEvidence());
    }

    @Test
    public void testReplayUsesDeterministicOrder() {
        MemoryEvent eventB = addEvent(
            "event-b",
            "fact-name-bob",
            "person:bob",
            "Bob",
            "2024-01-01T00:00:00Z",
            "2024-03-01T00:00:00Z");
        MemoryEvent eventA = addEvent(
            "event-a",
            "fact-name-alice",
            "person:alice",
            "Alice",
            "2024-01-01T00:00:00Z",
            "2024-03-01T00:00:00Z");
        MemoryEvent eventC = addEvent(
            "event-c",
            "fact-name-carol",
            "person:carol",
            "Carol",
            "2024-01-01T00:00:00Z",
            "2024-04-01T00:00:00Z");

        List<MemoryFactVersion> shuffled = oracle.replay(
            Arrays.asList(eventC, eventB, eventA));
        List<MemoryFactVersion> ordered = oracle.replay(
            Arrays.asList(eventA, eventB, eventC));

        Assertions.assertEquals(ordered, shuffled);
        Assertions.assertEquals(
            "event-a:version:0",
            shuffled.get(0).getId());
        Assertions.assertEquals(
            "event-b:version:0",
            shuffled.get(1).getId());
        Assertions.assertEquals(
            "event-c:version:0",
            shuffled.get(2).getId());
    }

    @Test
    public void testDuplicateEventIsIdempotent() {
        MemoryEvent event = addEvent(
            "event-1",
            "fact-name-alice",
            "person:alice",
            "Alice",
            "2024-01-01T00:00:00Z",
            "2024-03-01T00:00:00Z");
        MemoryEvent duplicate = addEvent(
            "event-1",
            "fact-name-alice",
            "person:alice",
            "Alice",
            "2024-01-01T00:00:00Z",
            "2024-03-01T00:00:00Z");

        List<MemoryFactVersion> once =
            oracle.replay(Collections.singletonList(event));
        List<MemoryFactVersion> repeated =
            oracle.replay(Arrays.asList(event, duplicate, event));

        Assertions.assertEquals(once, repeated);
        Assertions.assertEquals(1, repeated.size());
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () -> repeated.clear());
    }

    @Test
    public void testRejectConflictingEventId() {
        MemoryEvent original = addEvent(
            "event-1",
            "fact-name-alice",
            "person:alice",
            "Alice",
            "2024-01-01T00:00:00Z",
            "2024-03-01T00:00:00Z");
        MemoryEvent conflict = addEvent(
            "event-1",
            "fact-name-alice",
            "person:alice",
            "Alice Smith",
            "2024-01-01T00:00:00Z",
            "2024-03-01T00:00:00Z");

        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> oracle.replay(
                Arrays.asList(original, conflict)));
    }

    @Test
    public void testRejectUnsupportedOperations() {
        MemoryFact correctedFact = MemoryFact.attribute(
            "fact-name-alice",
            new MemoryEntity("person:alice", "person"),
            "name",
            "Alice Smith");

        MemoryEvent correct = MemoryEvent.correct(
            "event-correct",
            correctedFact,
            TimeInterval.unboundedFrom(
                time("2024-01-01T00:00:00Z")),
            time("2024-06-01T00:00:00Z"),
            evidence("event-correct"));

        MemoryEvent retract = MemoryEvent.retract(
            "event-retract",
            "fact-name-alice",
            TimeInterval.unboundedFrom(
                time("2025-01-01T00:00:00Z")),
            time("2025-02-01T00:00:00Z"),
            evidence("event-retract"));

        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () -> oracle.replay(
                Collections.singletonList(correct)));
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () -> oracle.replay(
                Collections.singletonList(retract)));
    }

    @Test
    public void testEmptyAndInvalidInput() {
        Assertions.assertTrue(
            oracle.replay(Collections.emptyList()).isEmpty());
        Assertions.assertThrows(
            NullPointerException.class,
            () -> oracle.replay(null));

        List<MemoryEvent> eventsWithNull = new ArrayList<>();
        eventsWithNull.add(null);

        Assertions.assertThrows(
            NullPointerException.class,
            () -> oracle.replay(eventsWithNull));
    }

    private static MemoryEvent addEvent(
        String eventId,
        String factId,
        String subjectId,
        String literalValue,
        String validStart,
        String transactionTime) {
        MemoryFact fact = MemoryFact.attribute(
            factId,
            new MemoryEntity(subjectId, "person"),
            "name",
            literalValue);

        return MemoryEvent.add(
            eventId,
            fact,
            TimeInterval.unboundedFrom(time(validStart)),
            time(transactionTime),
            evidence(eventId));
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
