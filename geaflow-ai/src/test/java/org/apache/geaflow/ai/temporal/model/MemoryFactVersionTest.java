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

public class MemoryFactVersionTest {

    @Test
    public void testBitemporalVersion() {
        MemoryFact fact = fact("Alice");
        TimeInterval validTime = TimeInterval.unboundedFrom(
            time("2024-01-01T00:00:00Z"));
        TimeInterval transactionTime = interval(
            "2024-03-01T00:00:00Z",
            "2024-06-01T00:00:00Z");
        Evidence evidence = evidence(
            "evidence-1",
            "Alice is the recorded name");

        MemoryFactVersion version = new MemoryFactVersion(
            "version-1",
            fact,
            validTime,
            transactionTime,
            Collections.singletonList(evidence));

        Assertions.assertEquals("version-1", version.getId());
        Assertions.assertEquals(fact, version.getFact());
        Assertions.assertEquals(validTime, version.getValidTime());
        Assertions.assertEquals(
            transactionTime,
            version.getTransactionTime());
        Assertions.assertEquals(
            Collections.singletonList(evidence),
            version.getEvidence());
    }

    @Test
    public void testValueSemantics() {
        MemoryFactVersion version = new MemoryFactVersion(
            "version-1",
            fact("Alice"),
            interval(
                "2024-01-01T00:00:00Z",
                "2025-01-01T00:00:00Z"),
            TimeInterval.unboundedFrom(
                time("2024-03-01T00:00:00Z")),
            Collections.singletonList(evidence(
                "evidence-1",
                "Alice is the recorded name")));

        MemoryFactVersion same = new MemoryFactVersion(
            "version-1",
            fact("Alice"),
            interval(
                "2024-01-01T00:00:00Z",
                "2025-01-01T00:00:00Z"),
            TimeInterval.unboundedFrom(
                time("2024-03-01T00:00:00Z")),
            Collections.singletonList(evidence(
                "evidence-1",
                "Alice is the recorded name")));

        Assertions.assertEquals(version, same);
        Assertions.assertEquals(version.hashCode(), same.hashCode());

        Assertions.assertNotEquals(
            version,
            new MemoryFactVersion(
                "version-1",
                fact("Alice Smith"),
                interval(
                    "2024-01-01T00:00:00Z",
                    "2025-01-01T00:00:00Z"),
                TimeInterval.unboundedFrom(
                    time("2024-03-01T00:00:00Z")),
                Collections.singletonList(evidence(
                    "evidence-1",
                    "Alice is the recorded name"))));

        Assertions.assertNotEquals(
            version,
            new MemoryFactVersion(
                "version-1",
                fact("Alice"),
                interval(
                    "2024-01-01T00:00:00Z",
                    "2025-01-01T00:00:00Z"),
                TimeInterval.unboundedFrom(
                    time("2024-04-01T00:00:00Z")),
                Collections.singletonList(evidence(
                    "evidence-1",
                    "Alice is the recorded name"))));
    }

    @Test
    public void testEvidenceIsDefensivelyCopied() {
        Evidence first = evidence(
            "evidence-1",
            "Alice is the recorded name");
        List<Evidence> evidence = new ArrayList<>();
        evidence.add(first);

        MemoryFactVersion version = new MemoryFactVersion(
            "version-1",
            fact("Alice"),
            TimeInterval.unboundedFrom(
                time("2024-01-01T00:00:00Z")),
            TimeInterval.unboundedFrom(
                time("2024-03-01T00:00:00Z")),
            evidence);

        evidence.add(evidence(
            "evidence-2",
            "A later independent record"));

        Assertions.assertEquals(
            Collections.singletonList(first),
            version.getEvidence());
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () -> version.getEvidence().clear());
    }

    @Test
    public void testStatusDefaultsToActiveAndAffectsValueSemantics() {
        MemoryFact fact = fact("Alice");
        TimeInterval validTime = TimeInterval.unboundedFrom(
            time("2024-01-01T00:00:00Z"));
        TimeInterval transactionTime = TimeInterval.unboundedFrom(
            time("2024-03-01T00:00:00Z"));
        List<Evidence> evidence = Collections.singletonList(
            evidence("evidence-1", "Alice is the recorded name"));
        MemoryFactVersion active = new MemoryFactVersion(
            "version-1",
            fact,
            validTime,
            transactionTime,
            evidence);
        MemoryFactVersion explicitActive = new MemoryFactVersion(
            "version-1",
            fact,
            MemoryFactVersionStatus.ACTIVE,
            validTime,
            transactionTime,
            evidence);
        MemoryFactVersion retracted = new MemoryFactVersion(
            "version-1",
            fact,
            MemoryFactVersionStatus.RETRACTED,
            validTime,
            transactionTime,
            evidence);

        Assertions.assertEquals(
            MemoryFactVersionStatus.ACTIVE,
            active.getStatus());
        Assertions.assertEquals(active, explicitActive);
        Assertions.assertNotEquals(active, retracted);
        Assertions.assertThrows(
            NullPointerException.class,
            () -> new MemoryFactVersion(
                "version-1",
                fact,
                null,
                validTime,
                transactionTime,
                evidence));
    }

    @Test
    public void testRejectInvalidVersion() {
        MemoryFact fact = fact("Alice");
        TimeInterval validTime = TimeInterval.unboundedFrom(
            time("2024-01-01T00:00:00Z"));
        TimeInterval transactionTime = TimeInterval.unboundedFrom(
            time("2024-03-01T00:00:00Z"));
        List<Evidence> evidence = Collections.singletonList(
            evidence("evidence-1", "Alice is the recorded name"));

        Assertions.assertThrows(
            NullPointerException.class,
            () -> new MemoryFactVersion(
                null,
                fact,
                validTime,
                transactionTime,
                evidence));
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> new MemoryFactVersion(
                " ",
                fact,
                validTime,
                transactionTime,
                evidence));
        Assertions.assertThrows(
            NullPointerException.class,
            () -> new MemoryFactVersion(
                "version-1",
                null,
                validTime,
                transactionTime,
                evidence));
        Assertions.assertThrows(
            NullPointerException.class,
            () -> new MemoryFactVersion(
                "version-1",
                fact,
                null,
                transactionTime,
                evidence));
        Assertions.assertThrows(
            NullPointerException.class,
            () -> new MemoryFactVersion(
                "version-1",
                fact,
                validTime,
                null,
                evidence));
        Assertions.assertThrows(
            NullPointerException.class,
            () -> new MemoryFactVersion(
                "version-1",
                fact,
                validTime,
                transactionTime,
                null));
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> new MemoryFactVersion(
                "version-1",
                fact,
                validTime,
                transactionTime,
                Collections.emptyList()));

        List<Evidence> evidenceWithNull = new ArrayList<>();
        evidenceWithNull.add(null);

        Assertions.assertThrows(
            NullPointerException.class,
            () -> new MemoryFactVersion(
                "version-1",
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

    private static Evidence evidence(String id, String content) {
        return new Evidence(
            id,
            new Source("source-1", "customer-database"),
            content);
    }

    private static TimeInterval interval(String start, String end) {
        return new TimeInterval(time(start), time(end));
    }

    private static Instant time(String value) {
        return Instant.parse(value);
    }
}
