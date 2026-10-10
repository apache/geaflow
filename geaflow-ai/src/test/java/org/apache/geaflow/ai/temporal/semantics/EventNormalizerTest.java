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

package org.apache.geaflow.ai.temporal.semantics;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.geaflow.ai.temporal.model.Evidence;
import org.apache.geaflow.ai.temporal.model.FactKey;
import org.apache.geaflow.ai.temporal.model.FactValue;
import org.apache.geaflow.ai.temporal.model.MemoryEntity;
import org.apache.geaflow.ai.temporal.model.MemoryEvent;
import org.apache.geaflow.ai.temporal.model.MemoryEventOperation;
import org.apache.geaflow.ai.temporal.model.MemoryFact;
import org.apache.geaflow.ai.temporal.model.Source;
import org.apache.geaflow.ai.temporal.model.TimeInterval;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class EventNormalizerTest {

    private final EventNormalizer normalizer = new EventNormalizer();

    @Test
    public void testNormalizeUnicodeTimeEvidenceAndEntityReference() {
        String decomposedEventId = "event-e\u0301";
        MemoryFact relationship = MemoryFact.relationship(
            "fact-location",
            new MemoryEntity("person:cafe\u0301", "pe\u0301rson"),
            "li\u0301ves_in",
            new MemoryEntity("city:be\u0301ijing", "ci\u0301ty"));
        MemoryEvent event = MemoryEvent.add(
            decomposedEventId,
            relationship,
            new TimeInterval(
                time("2024-01-01T00:00:00.123456789Z"),
                time("2025-01-01T00:00:00.987654321Z")),
            time("2024-03-01T00:00:00.456789123Z"),
            Arrays.asList(
                evidence(
                    "evidence-b",
                    "source-b",
                    "registre\u0301-b",
                    "second"),
                evidence(
                    "evidence-a",
                    "source-a",
                    "registre\u0301-a",
                    "first")));
        FactKey key = new FactKey(
            "person:cafe\u0301",
            "li\u0301ves_in",
            "pro\u0301file");

        NormalizedMemoryEvent normalized =
            normalizer.normalize(event, key);

        Assertions.assertEquals("event-\u00e9", normalized.getEventId());
        Assertions.assertEquals(
            MemoryEventOperation.ADD,
            normalized.getOperation());
        Assertions.assertEquals("fact-location", normalized.getFactId());
        Assertions.assertEquals(
            new FactKey(
                "person:caf\u00e9",
                "l\u00edves_in",
                "pr\u00f3file"),
            normalized.getFactKey());
        Assertions.assertEquals(
            Optional.of(FactValue.entityReference("city:b\u00e9ijing")),
            normalized.getFactValue());
        Assertions.assertEquals(
            time("2024-01-01T00:00:00.123456789Z"),
            normalized.getValidTime().getStart());
        Assertions.assertEquals(
            Optional.of(time("2025-01-01T00:00:00.987654321Z")),
            normalized.getValidTime().getEnd());
        Assertions.assertEquals(
            time("2024-03-01T00:00:00.456789123Z"),
            normalized.getRecordedAt());
        Assertions.assertEquals(
            Arrays.asList("evidence-a", "evidence-b"),
            Arrays.asList(
                normalized.getEvidence().get(0).getId(),
                normalized.getEvidence().get(1).getId()));
        Assertions.assertEquals(
            "registr\u00e9-a",
            normalized.getEvidence().get(0).getSource().getName());
        Assertions.assertTrue(
            normalized.getPayloadHash().matches("[0-9a-f]{64}"));
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () -> normalized.getEvidence().clear());
        Assertions.assertEquals(decomposedEventId, event.getId());
    }

    @Test
    public void testPayloadHashIsCanonicalAndCoversPayload() {
        FactKey key = key("profile");
        Evidence first = evidence(
            "evidence-a",
            "source-a",
            "registry-a",
            "first");
        Evidence second = evidence(
            "evidence-b",
            "source-b",
            "registry-b",
            "second");
        NormalizedMemoryEvent original = normalizer.normalize(
            addEvent(
                "event-1",
                "Beijing",
                "2024-03-01T00:00:00Z",
                Arrays.asList(second, first)),
            key);
        NormalizedMemoryEvent samePayload = normalizer.normalize(
            addEvent(
                "event-2",
                "Beijing",
                "2024-03-01T00:00:00Z",
                Arrays.asList(first, second)),
            key);
        NormalizedMemoryEvent differentValue = normalizer.normalize(
            addEvent(
                "event-1",
                "Shanghai",
                "2024-03-01T00:00:00Z",
                Arrays.asList(first, second)),
            key);
        NormalizedMemoryEvent differentRecordedAt = normalizer.normalize(
            addEvent(
                "event-1",
                "Beijing",
                "2024-04-01T00:00:00Z",
                Arrays.asList(first, second)),
            key);
        NormalizedMemoryEvent differentRecordedAtNanos = normalizer.normalize(
            addEvent(
                "event-1",
                "Beijing",
                "2024-03-01T00:00:00.000000001Z",
                Arrays.asList(first, second)),
            key);
        NormalizedMemoryEvent differentScope = normalizer.normalize(
            addEvent(
                "event-1",
                "Beijing",
                "2024-03-01T00:00:00Z",
                Arrays.asList(first, second)),
            key("account"));

        Assertions.assertEquals(
            Optional.of(FactValue.literal("Beijing")),
            original.getFactValue());
        Assertions.assertEquals(
            original.getPayloadHash(),
            samePayload.getPayloadHash());
        Assertions.assertNotEquals(
            original.getPayloadHash(),
            differentValue.getPayloadHash());
        Assertions.assertNotEquals(
            original.getPayloadHash(),
            differentRecordedAt.getPayloadHash());
        Assertions.assertNotEquals(
            original.getPayloadHash(),
            differentRecordedAtNanos.getPayloadHash());
        Assertions.assertNotEquals(
            original.getPayloadHash(),
            differentScope.getPayloadHash());
    }

    @Test
    public void testNormalizeRetractAndRejectMismatchedKey() {
        FactKey key = key("profile");
        MemoryEvent retract = MemoryEvent.retract(
            "event-retract",
            "fact-location",
            TimeInterval.unboundedFrom(
                time("2024-01-01T00:00:00.123456789Z")),
            time("2024-06-01T00:00:00.987654321Z"),
            Collections.singletonList(evidence(
                "evidence-a",
                "source-a",
                "registry-a",
                "withdrawn")));

        NormalizedMemoryEvent normalized =
            normalizer.normalize(retract, key);

        Assertions.assertEquals(key, normalized.getFactKey());
        Assertions.assertEquals(
            Optional.empty(),
            normalized.getFactValue());
        Assertions.assertEquals(
            time("2024-06-01T00:00:00.987654321Z"),
            normalized.getRecordedAt());
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> normalizer.normalize(
                addEvent(
                    "event-add",
                    "Beijing",
                    "2024-03-01T00:00:00Z",
                    Collections.singletonList(evidence(
                        "evidence-a",
                        "source-a",
                        "registry-a",
                        "first"))),
                new FactKey(
                    "person:bob",
                    "location",
                    "profile")));
        Assertions.assertThrows(
            NullPointerException.class,
            () -> normalizer.normalize(null, key));
        Assertions.assertThrows(
            NullPointerException.class,
            () -> normalizer.normalize(retract, null));
    }

    private static MemoryEvent addEvent(
        String eventId,
        String value,
        String recordedAt,
        List<Evidence> evidence) {
        return MemoryEvent.add(
            eventId,
            MemoryFact.attribute(
                "fact-location",
                new MemoryEntity("person:alice", "person"),
                "location",
                value),
            TimeInterval.unboundedFrom(
                time("2024-01-01T00:00:00Z")),
            time(recordedAt),
            evidence);
    }

    private static FactKey key(String scope) {
        return new FactKey(
            "person:alice",
            "location",
            scope);
    }

    private static Evidence evidence(
        String evidenceId,
        String sourceId,
        String sourceName,
        String content) {
        return new Evidence(
            evidenceId,
            new Source(sourceId, sourceName),
            content);
    }

    private static Instant time(String value) {
        return Instant.parse(value);
    }
}
