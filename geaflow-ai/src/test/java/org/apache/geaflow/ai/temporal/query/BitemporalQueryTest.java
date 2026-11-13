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

package org.apache.geaflow.ai.temporal.query;

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

public class BitemporalQueryTest {

    private final BitemporalQuery query = new BitemporalQuery();
    private final FullReplayOracle oracle = new FullReplayOracle();

    @Test
    public void testQueryBeforeAndAfterCorrection() {
        MemoryEvent add = addEvent(
            "event-add",
            "fact-name-alice",
            "person:alice",
            "Alice",
            "2024-01-01T00:00:00Z",
            "2024-03-01T00:00:00Z");
        MemoryEvent correction = correctEvent(
            "event-correct",
            "fact-name-alice",
            "person:alice",
            "Alice Smith",
            "2024-04-01T00:00:00Z",
            "2024-09-01T00:00:00Z",
            "2024-06-01T00:00:00Z");
        List<MemoryFactVersion> versions =
            oracle.replay(Arrays.asList(correction, add));

        List<MemoryFactVersion> beforeCorrection = query.query(
            versions,
            time("2024-05-01T00:00:00Z"),
            time("2024-05-01T00:00:00Z"));
        List<MemoryFactVersion> afterCorrection = query.query(
            versions,
            time("2024-05-01T00:00:00Z"),
            time("2024-07-01T00:00:00Z"));

        Assertions.assertEquals(1, beforeCorrection.size());
        Assertions.assertEquals(
            add.getFact().get(),
            beforeCorrection.get(0).getFact());
        Assertions.assertEquals(1, afterCorrection.size());
        Assertions.assertEquals(
            correction.getFact().get(),
            afterCorrection.get(0).getFact());
    }

    @Test
    public void testQueryBeforeAndAfterRetraction() {
        MemoryEvent add = addEvent(
            "event-add",
            "fact-name-alice",
            "person:alice",
            "Alice",
            "2024-01-01T00:00:00Z",
            "2024-03-01T00:00:00Z");
        MemoryEvent retract = retractEvent(
            "event-retract",
            "fact-name-alice",
            "2024-04-01T00:00:00Z",
            "2024-09-01T00:00:00Z",
            "2024-06-01T00:00:00Z");
        List<MemoryFactVersion> versions =
            oracle.replay(Arrays.asList(retract, add));

        List<MemoryFactVersion> beforeRetraction = query.query(
            versions,
            time("2024-05-01T00:00:00Z"),
            time("2024-05-01T00:00:00Z"));
        List<MemoryFactVersion> afterRetraction = query.query(
            versions,
            time("2024-05-01T00:00:00Z"),
            time("2024-07-01T00:00:00Z"));
        List<MemoryFactVersion> outsideRetraction = query.query(
            versions,
            time("2024-10-01T00:00:00Z"),
            time("2024-07-01T00:00:00Z"));

        Assertions.assertEquals(1, beforeRetraction.size());
        Assertions.assertEquals(
            add.getFact().get(),
            beforeRetraction.get(0).getFact());
        Assertions.assertTrue(afterRetraction.isEmpty());
        Assertions.assertEquals(1, outsideRetraction.size());
        Assertions.assertEquals(
            add.getFact().get(),
            outsideRetraction.get(0).getFact());
    }

    @Test
    public void testHalfOpenBoundaries() {
        MemoryFactVersion version = new MemoryFactVersion(
            "version-1",
            fact("fact-name-alice", "person:alice", "Alice"),
            interval(
                "2024-01-01T00:00:00Z",
                "2024-06-01T00:00:00Z"),
            interval(
                "2024-03-01T00:00:00Z",
                "2024-09-01T00:00:00Z"),
            evidence("version-1"));
        List<MemoryFactVersion> versions =
            Collections.singletonList(version);

        Assertions.assertEquals(
            1,
            query.query(
                versions,
                time("2024-01-01T00:00:00Z"),
                time("2024-03-01T00:00:00Z")).size());
        Assertions.assertTrue(
            query.query(
                versions,
                time("2024-06-01T00:00:00Z"),
                time("2024-03-01T00:00:00Z")).isEmpty());
        Assertions.assertTrue(
            query.query(
                versions,
                time("2024-05-01T00:00:00Z"),
                time("2024-09-01T00:00:00Z")).isEmpty());
    }

    @Test
    public void testResultIsDeterministicAndImmutable() {
        MemoryFactVersion bob = currentVersion(
            "version-bob",
            fact("fact-name-bob", "person:bob", "Bob"));
        MemoryFactVersion alice = currentVersion(
            "version-alice",
            fact("fact-name-alice", "person:alice", "Alice"));

        List<MemoryFactVersion> result = query.query(
            Arrays.asList(bob, alice),
            time("2024-05-01T00:00:00Z"),
            time("2024-05-01T00:00:00Z"));

        Assertions.assertEquals(2, result.size());
        Assertions.assertEquals(
            "fact-name-alice",
            result.get(0).getFact().getId());
        Assertions.assertEquals(
            "fact-name-bob",
            result.get(1).getFact().getId());
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () -> result.clear());
    }

    @Test
    public void testEmptyAndInvalidInput() {
        Instant queryTime = time("2024-05-01T00:00:00Z");

        Assertions.assertTrue(
            query.query(
                Collections.emptyList(),
                queryTime,
                queryTime).isEmpty());
        Assertions.assertThrows(
            NullPointerException.class,
            () -> query.query(null, queryTime, queryTime));
        Assertions.assertThrows(
            NullPointerException.class,
            () -> query.query(
                Collections.emptyList(),
                null,
                queryTime));
        Assertions.assertThrows(
            NullPointerException.class,
            () -> query.query(
                Collections.emptyList(),
                queryTime,
                null));

        List<MemoryFactVersion> versionsWithNull = new ArrayList<>();
        versionsWithNull.add(null);
        Assertions.assertThrows(
            NullPointerException.class,
            () -> query.query(
                versionsWithNull,
                queryTime,
                queryTime));
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

    private static MemoryFactVersion currentVersion(
        String versionId,
        MemoryFact fact) {
        return new MemoryFactVersion(
            versionId,
            fact,
            TimeInterval.unboundedFrom(
                time("2024-01-01T00:00:00Z")),
            TimeInterval.unboundedFrom(
                time("2024-03-01T00:00:00Z")),
            evidence(versionId));
    }

    private static MemoryFact fact(
        String factId,
        String subjectId,
        String literalValue) {
        return MemoryFact.attribute(
            factId,
            new MemoryEntity(subjectId, "person"),
            "name",
            literalValue);
    }

    private static TimeInterval interval(
        String start,
        String end) {
        return new TimeInterval(time(start), time(end));
    }

    private static List<Evidence> evidence(String id) {
        return Collections.singletonList(new Evidence(
            "evidence-" + id,
            new Source("source-1", "customer-database"),
            "Evidence for " + id));
    }

    private static Instant time(String value) {
        return Instant.parse(value);
    }
}
