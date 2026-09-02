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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TemporalEventAggregateFunctionTest {

    private final TemporalEventAggregateFunction function =
        new TemporalEventAggregateFunction();
    private final FullReplayOracle oracle = new FullReplayOracle();

    @Test
    public void testCreateAddAndGetResultMatchFullReplay() {
        IncrementalTemporalIntegrator accumulator =
            function.createAccumulator();
        IncrementalTemporalIntegrator other =
            function.createAccumulator();
        List<MemoryEvent> events = Arrays.asList(
            addEvent(
                "event-add",
                "Beijing",
                "2024-03-01T00:00:00Z"),
            correctEvent(
                "event-correct",
                "Shanghai",
                "2024-04-01T00:00:00Z",
                "2024-09-01T00:00:00Z",
                "2024-06-01T00:00:00Z"),
            retractEvent(
                "event-retract",
                "2024-08-01T00:00:00Z",
                "2024-10-01T00:00:00Z",
                "2024-11-01T00:00:00Z"),
            correctEvent(
                "event-late",
                "Tianjin",
                "2024-02-01T00:00:00Z",
                "2024-03-01T00:00:00Z",
                "2024-05-01T00:00:00Z"));

        Assertions.assertNotSame(accumulator, other);
        Assertions.assertTrue(function.getResult(other).isEmpty());

        List<MemoryEvent> received = new ArrayList<>();
        for (MemoryEvent event : events) {
            function.add(event, accumulator);
            received.add(event);
            Assertions.assertEquals(
                oracle.replay(received),
                function.getResult(accumulator));
        }

        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () -> function.getResult(accumulator).clear());
    }

    @Test
    public void testDuplicateAndConflictRemainAtomic() {
        IncrementalTemporalIntegrator accumulator =
            function.createAccumulator();
        MemoryEvent event = addEvent(
            "event-add",
            "Beijing",
            "2024-03-01T00:00:00Z");
        MemoryEvent duplicate = addEvent(
            "event-add",
            "Beijing",
            "2024-03-01T00:00:00Z");
        MemoryEvent conflict = addEvent(
            "event-add",
            "Shanghai",
            "2024-03-01T00:00:00Z");

        function.add(event, accumulator);
        List<MemoryFactVersion> expected =
            function.getResult(accumulator);

        function.add(duplicate, accumulator);
        Assertions.assertEquals(
            expected,
            function.getResult(accumulator));
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> function.add(conflict, accumulator));
        Assertions.assertEquals(
            expected,
            function.getResult(accumulator));
    }

    @Test
    public void testMergeReplaysEventsWithoutMutatingInputs() {
        MemoryEvent add = addEvent(
            "event-add",
            "Beijing",
            "2024-03-01T00:00:00Z");
        MemoryEvent correction = correctEvent(
            "event-correct",
            "Shanghai",
            "2024-04-01T00:00:00Z",
            "2024-09-01T00:00:00Z",
            "2024-06-01T00:00:00Z");
        MemoryEvent retract = retractEvent(
            "event-retract",
            "2024-08-01T00:00:00Z",
            "2024-10-01T00:00:00Z",
            "2024-11-01T00:00:00Z");
        MemoryEvent lateCorrection = correctEvent(
            "event-late",
            "Tianjin",
            "2024-02-01T00:00:00Z",
            "2024-03-01T00:00:00Z",
            "2024-05-01T00:00:00Z");
        IncrementalTemporalIntegrator left =
            function.createAccumulator();
        IncrementalTemporalIntegrator right =
            function.createAccumulator();
        addAll(left, add, correction, retract);
        addAll(right, add, lateCorrection);
        List<MemoryFactVersion> leftBefore =
            function.getResult(left);
        List<MemoryFactVersion> rightBefore =
            function.getResult(right);

        IncrementalTemporalIntegrator merged =
            function.merge(left, right);
        IncrementalTemporalIntegrator mergedAgain =
            function.merge(right, left);

        Assertions.assertNotSame(left, merged);
        Assertions.assertNotSame(right, merged);
        Assertions.assertEquals(
            oracle.replay(Arrays.asList(
                add,
                correction,
                retract,
                lateCorrection)),
            function.getResult(merged));
        Assertions.assertEquals(
            function.getResult(merged),
            function.getResult(mergedAgain));
        Assertions.assertEquals(leftBefore, function.getResult(left));
        Assertions.assertEquals(rightBefore, function.getResult(right));
    }

    @Test
    public void testNullArgumentsRejected() {
        IncrementalTemporalIntegrator accumulator =
            function.createAccumulator();
        MemoryEvent event = addEvent(
            "event-add",
            "Beijing",
            "2024-03-01T00:00:00Z");

        Assertions.assertThrows(
            NullPointerException.class,
            () -> function.add(null, accumulator));
        Assertions.assertThrows(
            NullPointerException.class,
            () -> function.add(event, null));
        Assertions.assertThrows(
            NullPointerException.class,
            () -> function.getResult(null));
        Assertions.assertThrows(
            NullPointerException.class,
            () -> function.merge(null, accumulator));
        Assertions.assertThrows(
            NullPointerException.class,
            () -> function.merge(accumulator, null));
    }

    private void addAll(
        IncrementalTemporalIntegrator accumulator,
        MemoryEvent... events) {
        for (MemoryEvent event : events) {
            function.add(event, accumulator);
        }
    }

    private static MemoryEvent addEvent(
        String eventId,
        String value,
        String transactionTime) {
        return MemoryEvent.add(
            eventId,
            fact(value),
            TimeInterval.unboundedFrom(
                time("2024-01-01T00:00:00Z")),
            time(transactionTime),
            evidence(eventId));
    }

    private static MemoryEvent correctEvent(
        String eventId,
        String value,
        String validStart,
        String validEnd,
        String transactionTime) {
        return MemoryEvent.correct(
            eventId,
            fact(value),
            interval(validStart, validEnd),
            time(transactionTime),
            evidence(eventId));
    }

    private static MemoryEvent retractEvent(
        String eventId,
        String validStart,
        String validEnd,
        String transactionTime) {
        return MemoryEvent.retract(
            eventId,
            "fact-alice-city",
            interval(validStart, validEnd),
            time(transactionTime),
            evidence(eventId));
    }

    private static MemoryFact fact(String value) {
        return MemoryFact.attribute(
            "fact-alice-city",
            new MemoryEntity("person:alice", "person"),
            "city",
            value);
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
