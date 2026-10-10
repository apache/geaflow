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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Compares events, versions, generators and relations in that order.
 * Paths align events and versions by ID; times use ISO-8601 or "infinity".
 * Context comes from the expected side, or the present side for a missing record.
 * Generator differences name the expected event; relation differences use the
 * expected from-version's FactKey and generating event.
 */
public class SnapshotComparatorTest {

    private static final FactKey KEY = new FactKey("person:alice", "location", "profile");
    private static final List<Evidence> EVIDENCE = Collections.singletonList(
        new Evidence("evidence-a", new Source("source-a", "registry"), "recorded location"));

    private final SnapshotComparator comparator = new SnapshotComparator();

    @Test
    public void testEquivalentSnapshotsHaveNoDifferenceOrContext() {
        CanonicalSnapshot empty = snapshot(Collections.emptyList(), Collections.emptyList(),
            Collections.emptyMap(), Collections.emptyList());
        assertEquivalent(comparator.compare(empty, empty));

        MemoryFactVersion first = version("version-a");
        MemoryFactVersion second = version("version-b");
        NormalizedMemoryEvent firstEvent = event("event-a", "profile");
        NormalizedMemoryEvent secondEvent = event("event-b", "profile");
        CanonicalSnapshot expected = snapshot(Arrays.asList(first, second),
            Arrays.asList(firstEvent, secondEvent), generatingIds(), Collections.emptyList());
        Map<String, String> reversedIds = new LinkedHashMap<>();
        reversedIds.put("version-b", "event-b");
        reversedIds.put("version-a", "event-a");
        CanonicalSnapshot actual = snapshot(Arrays.asList(second, first),
            Arrays.asList(secondEvent, firstEvent), reversedIds, Collections.emptyList());

        assertEquivalent(comparator.compare(expected, actual));
    }

    @Test
    public void testStatusPrecedesTransactionEndAndReportsVersionContext() {
        CanonicalSnapshot expected = singleVersion(version("version-a"));
        CanonicalSnapshot actual = singleVersion(version("version-a", "Paris",
            MemoryFactVersionStatus.RETRACTED, Instant.ofEpochMilli(5000)));

        DiffReport difference = comparator.compare(expected, actual);
        assertDifference(difference, "versions[version-a].status",
            "ACTIVE", "RETRACTED", KEY, "event-a");
        assertEquivalent(comparator.compare(expected, expected));
        assertDifference(difference, "versions[version-a].status",
            "ACTIVE", "RETRACTED", KEY, "event-a");
    }

    @Test
    public void testTransactionEndUsesExactInstantAndExplicitInfinity() {
        CanonicalSnapshot expected = singleVersion(version("version-a"));
        CanonicalSnapshot actual = singleVersion(version("version-a", "Paris",
            MemoryFactVersionStatus.ACTIVE, Instant.ofEpochMilli(5000)));

        assertDifference(comparator.compare(expected, actual),
            "versions[version-a].transactionTime.end", "infinity",
            "1970-01-01T00:00:05Z", KEY, "event-a");
    }

    @Test
    public void testTransactionEndPreservesNanosecondPrecision() {
        Instant expectedEnd = Instant.ofEpochSecond(5, 100);
        Instant actualEnd = Instant.ofEpochSecond(5, 200);
        CanonicalSnapshot expected = singleVersion(version("version-a", "Paris",
            MemoryFactVersionStatus.ACTIVE, expectedEnd));
        CanonicalSnapshot actual = singleVersion(version("version-a", "Paris",
            MemoryFactVersionStatus.ACTIVE, actualEnd));

        assertDifference(comparator.compare(expected, actual),
            "versions[version-a].transactionTime.end",
            expectedEnd.toString(), actualEnd.toString(), KEY, "event-a");
    }

    @Test
    public void testReportsOutputFactValueEvenWhenInputEventsMatch() {
        CanonicalSnapshot expected = singleVersion(version("version-a"));
        CanonicalSnapshot actual = singleVersion(version("version-a", "Rome",
            MemoryFactVersionStatus.ACTIVE, null));

        assertDifference(comparator.compare(expected, actual),
            "versions[version-a].fact.literalValue", "Paris", "Rome", KEY, "event-a");
    }

    @Test
    public void testEventOnlyScopeDifferencePrecedesPayloadHash() {
        NormalizedMemoryEvent expectedEvent = event("event-a", "profile");
        NormalizedMemoryEvent actualEvent = event("event-a", "private");
        Assertions.assertNotEquals(expectedEvent.getPayloadHash(), actualEvent.getPayloadHash());
        CanonicalSnapshot expected = snapshot(Collections.emptyList(),
            Collections.singletonList(expectedEvent), Collections.emptyMap(),
            Collections.emptyList());
        CanonicalSnapshot actual = snapshot(Collections.emptyList(),
            Collections.singletonList(actualEvent), Collections.emptyMap(),
            Collections.emptyList());

        assertDifference(comparator.compare(expected, actual), "events[event-a].factKey.scope",
            "profile", "private", KEY, "event-a");
    }

    @Test
    public void testMissingVersionAlignsByIdAndUsesPresentSideContext() {
        MemoryFactVersion first = version("version-a");
        MemoryFactVersion second = version("version-b");
        List<NormalizedMemoryEvent> events = Arrays.asList(event("event-a", "profile"),
            event("event-b", "profile"));
        CanonicalSnapshot expected = snapshot(Arrays.asList(second, first), events,
            generatingIds(), Collections.emptyList());
        CanonicalSnapshot actual = snapshot(Collections.singletonList(second), events,
            Collections.singletonMap("version-b", "event-b"), Collections.emptyList());

        assertDifference(comparator.compare(expected, actual), "versions[version-a]",
            "present", null, KEY, "event-a");
        assertDifference(comparator.compare(actual, expected), "versions[version-a]",
            null, "present", KEY, "event-a");
    }

    @Test
    public void testGeneratingEventDifferenceUsesExpectedEventContext() {
        List<MemoryFactVersion> versions = Collections.singletonList(version("version-a"));
        List<NormalizedMemoryEvent> events = Arrays.asList(event("event-a", "profile"),
            event("event-b", "profile"));
        CanonicalSnapshot expected = snapshot(versions, events,
            Collections.singletonMap("version-a", "event-a"), Collections.emptyList());
        CanonicalSnapshot actual = snapshot(versions, events,
            Collections.singletonMap("version-a", "event-b"), Collections.emptyList());

        assertDifference(comparator.compare(expected, actual), "generatingEventIds[version-a]",
            "event-a", "event-b", KEY, "event-a");
    }

    @Test
    public void testRelationTypeDifferenceReportsFromVersionContext() {
        List<MemoryFactVersion> versions = Arrays.asList(version("version-a"),
            version("version-b"));
        List<NormalizedMemoryEvent> events = Arrays.asList(event("event-a", "profile"),
            event("event-b", "profile"));
        CanonicalSnapshot expected = snapshot(versions, events, generatingIds(),
            Collections.singletonList(new VersionRelation(VersionRelationType.SUPERSEDES,
                "version-b", "version-a")));
        CanonicalSnapshot actual = snapshot(versions, events, generatingIds(),
            Collections.singletonList(new VersionRelation(VersionRelationType.DUPLICATE_OF,
                "version-b", "version-a")));

        assertDifference(comparator.compare(expected, actual), "relations[0].type",
            "SUPERSEDES", "DUPLICATE_OF", KEY, "event-b");
    }

    private static void assertEquivalent(DiffReport report) {
        Assertions.assertTrue(report.isEquivalent());
        Assertions.assertEquals(Optional.empty(), report.getFieldPath());
        Assertions.assertEquals(Optional.empty(), report.getExpectedValue());
        Assertions.assertEquals(Optional.empty(), report.getActualValue());
        Assertions.assertEquals(Optional.empty(), report.getFactKey());
        Assertions.assertEquals(Optional.empty(), report.getEventId());
    }

    private static void assertDifference(DiffReport report, String path, String expected,
                                         String actual, FactKey key, String eventId) {
        Assertions.assertFalse(report.isEquivalent());
        Assertions.assertEquals(Optional.of(path), report.getFieldPath());
        Assertions.assertEquals(Optional.ofNullable(expected), report.getExpectedValue());
        Assertions.assertEquals(Optional.ofNullable(actual), report.getActualValue());
        Assertions.assertEquals(Optional.of(key), report.getFactKey());
        Assertions.assertEquals(Optional.of(eventId), report.getEventId());
    }

    private static CanonicalSnapshot singleVersion(MemoryFactVersion version) {
        return snapshot(Collections.singletonList(version),
            Collections.singletonList(event("event-a", "profile")),
            Collections.singletonMap(version.getId(), "event-a"), Collections.emptyList());
    }

    private static CanonicalSnapshot snapshot(List<MemoryFactVersion> versions,
                                              List<NormalizedMemoryEvent> events,
                                              Map<String, String> generatingEventIds,
                                              List<VersionRelation> relations) {
        Map<FactKey, List<MemoryFactVersion>> byKey = versions.isEmpty()
            ? Collections.emptyMap() : Collections.singletonMap(KEY, versions);
        return new CanonicalSnapshot(new TemporalState(byKey, relations), events,
            generatingEventIds);
    }

    private static Map<String, String> generatingIds() {
        Map<String, String> ids = new LinkedHashMap<>();
        ids.put("version-a", "event-a");
        ids.put("version-b", "event-b");
        return ids;
    }

    private static NormalizedMemoryEvent event(String eventId, String scope) {
        MemoryEvent event = MemoryEvent.add(eventId, fact("Paris"), validTime(),
            Instant.ofEpochMilli(2000), EVIDENCE);
        return new EventNormalizer().normalize(event,
            new FactKey(KEY.getSubjectId(), KEY.getPredicate(), scope));
    }

    private static MemoryFactVersion version(String versionId) {
        return version(versionId, "Paris", MemoryFactVersionStatus.ACTIVE, null);
    }

    private static MemoryFactVersion version(String versionId, String value,
                                             MemoryFactVersionStatus status, Instant end) {
        return new MemoryFactVersion(versionId, fact(value), status, validTime(),
            new TimeInterval(Instant.ofEpochMilli(2000), end), EVIDENCE);
    }

    private static MemoryFact fact(String value) {
        return MemoryFact.attribute("fact-location", new MemoryEntity("person:alice", "person"),
            "location", value);
    }

    private static TimeInterval validTime() {
        return new TimeInterval(Instant.ofEpochMilli(1000), Instant.ofEpochMilli(9000));
    }
}
