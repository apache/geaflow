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

package org.apache.geaflow.ai.temporal.model;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class MemoryEventTest {

    @Test
    public void testAddLateEvent() {
        MemoryFact fact = fact("Alice");
        TimeInterval validTime = TimeInterval.unboundedFrom(
            time("2024-01-01T00:00:00Z"));
        Instant transactionTime =
            time("2024-03-01T00:00:00Z");
        List<Evidence> evidence = evidenceList();

        MemoryEvent event = MemoryEvent.add(
            "event-1",
            fact,
            validTime,
            transactionTime,
            evidence);

        Assertions.assertEquals("event-1", event.getId());
        Assertions.assertEquals(
            MemoryEventOperation.ADD,
            event.getOperation());
        Assertions.assertEquals("fact-name-alice", event.getFactId());
        Assertions.assertEquals(fact, event.getFact().get());
        Assertions.assertEquals(validTime, event.getValidTime());
        Assertions.assertEquals(
            transactionTime,
            event.getTransactionTime());
        Assertions.assertEquals(evidence, event.getEvidence());
        Assertions.assertTrue(
            event.getValidTime().getStart()
                .isBefore(event.getTransactionTime()));
    }

    @Test
    public void testCorrectEvent() {
        MemoryFact corrected = fact("Alice Smith");

        MemoryEvent event = MemoryEvent.correct(
            "event-2",
            corrected,
            TimeInterval.unboundedFrom(
                time("2024-01-01T00:00:00Z")),
            time("2024-06-01T00:00:00Z"),
            evidenceList());

        Assertions.assertEquals(
            MemoryEventOperation.CORRECT,
            event.getOperation());
        Assertions.assertEquals("fact-name-alice", event.getFactId());
        Assertions.assertEquals(corrected, event.getFact().get());
    }

    @Test
    public void testRetractEvent() {
        TimeInterval validTime = TimeInterval.unboundedFrom(
            time("2025-01-01T00:00:00Z"));

        MemoryEvent event = MemoryEvent.retract(
            "event-3",
            "fact-name-alice",
            validTime,
            time("2025-02-01T00:00:00Z"),
            evidenceList());

        Assertions.assertEquals(
            MemoryEventOperation.RETRACT,
            event.getOperation());
        Assertions.assertEquals("fact-name-alice", event.getFactId());
        Assertions.assertFalse(event.getFact().isPresent());
        Assertions.assertEquals(validTime, event.getValidTime());
    }

    @Test
    public void testValueSemanticsAndEvidenceCopy() {
        Evidence first = evidence(
            "evidence-1",
            "Alice is the recorded name");
        List<Evidence> mutableEvidence = new ArrayList<>();
        mutableEvidence.add(first);

        MemoryEvent event = MemoryEvent.add(
            "event-1",
            fact("Alice"),
            TimeInterval.unboundedFrom(
                time("2024-01-01T00:00:00Z")),
            time("2024-03-01T00:00:00Z"),
            mutableEvidence);

        MemoryEvent same = MemoryEvent.add(
            "event-1",
            fact("Alice"),
            TimeInterval.unboundedFrom(
                time("2024-01-01T00:00:00Z")),
            time("2024-03-01T00:00:00Z"),
            Collections.singletonList(evidence(
                "evidence-1",
                "Alice is the recorded name")));

        mutableEvidence.add(evidence(
            "evidence-2",
            "An independent record"));

        Assertions.assertEquals(
            Collections.singletonList(first),
            event.getEvidence());
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () -> event.getEvidence().clear());
        Assertions.assertEquals(event, same);
        Assertions.assertEquals(event.hashCode(), same.hashCode());

        Assertions.assertNotEquals(
            event,
            MemoryEvent.add(
                "event-1",
                fact("Alice Smith"),
                TimeInterval.unboundedFrom(
                    time("2024-01-01T00:00:00Z")),
                time("2024-03-01T00:00:00Z"),
                evidenceList()));
    }

    @Test
    public void testRejectInvalidEvent() {
        MemoryFact fact = fact("Alice");
        TimeInterval validTime = TimeInterval.unboundedFrom(
            time("2024-01-01T00:00:00Z"));
        Instant transactionTime =
            time("2024-03-01T00:00:00Z");
        List<Evidence> evidence = evidenceList();

        Assertions.assertThrows(
            NullPointerException.class,
            () -> MemoryEvent.add(
                null,
                fact,
                validTime,
                transactionTime,
                evidence));
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> MemoryEvent.add(
                " ",
                fact,
                validTime,
                transactionTime,
                evidence));
        Assertions.assertThrows(
            NullPointerException.class,
            () -> MemoryEvent.add(
                "event-1",
                null,
                validTime,
                transactionTime,
                evidence));
        Assertions.assertThrows(
            NullPointerException.class,
            () -> MemoryEvent.retract(
                "event-1",
                null,
                validTime,
                transactionTime,
                evidence));
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> MemoryEvent.retract(
                "event-1",
                " ",
                validTime,
                transactionTime,
                evidence));
        Assertions.assertThrows(
            NullPointerException.class,
            () -> MemoryEvent.add(
                "event-1",
                fact,
                null,
                transactionTime,
                evidence));
        Assertions.assertThrows(
            NullPointerException.class,
            () -> MemoryEvent.add(
                "event-1",
                fact,
                validTime,
                null,
                evidence));
        Assertions.assertThrows(
            NullPointerException.class,
            () -> MemoryEvent.add(
                "event-1",
                fact,
                validTime,
                transactionTime,
                null));
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> MemoryEvent.add(
                "event-1",
                fact,
                validTime,
                transactionTime,
                Collections.emptyList()));

        List<Evidence> evidenceWithNull = new ArrayList<>();
        evidenceWithNull.add(null);

        Assertions.assertThrows(
            NullPointerException.class,
            () -> MemoryEvent.add(
                "event-1",
                fact,
                validTime,
                transactionTime,
                evidenceWithNull));
    }

    private static MemoryFact fact(String literalValue) {
        return MemoryFact.attribute(
            "fact-name-alice",
            new MemoryEntity("person:alice", "person"),
            "name",
            literalValue);
    }

    private static List<Evidence> evidenceList() {
        return Collections.singletonList(evidence(
            "evidence-1",
            "Alice is the recorded name"));
    }

    private static Evidence evidence(String id, String content) {
        return new Evidence(
            id,
            new Source("source-1", "customer-database"),
            content);
    }

    private static Instant time(String value) {
        return Instant.parse(value);
    }
}
