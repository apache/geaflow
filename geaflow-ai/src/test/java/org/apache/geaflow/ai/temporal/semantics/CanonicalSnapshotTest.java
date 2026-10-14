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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
 * Preserves normalized payloads and explicit version-generation references.
 * Events use recordedAt/ID order; state keeps FactKey/system-time/valid-time/ID order.
 * Version evidence is sorted without dropping repeated entries.
 */
public class CanonicalSnapshotTest {

    private static final FactKey KEY = key("profile");
    private static final Instant FIRST = Instant.parse("2024-03-01T00:00:00Z");
    private static final Instant SECOND = Instant.parse("2024-06-01T00:00:00Z");
    private static final TimeInterval VALID = TimeInterval.unboundedFrom(
        Instant.parse("2024-01-01T00:00:00Z"));

    private final EventNormalizer normalizer = new EventNormalizer();

    @Test
    public void testCanonicalOrderDoesNotDependOnInputContainers() {
        FactKey account = key("account");
        Evidence first = evidence("evidence-a");
        Evidence second = evidence("evidence-b");
        NormalizedMemoryEvent eventA = add("event-a", KEY, FIRST);
        NormalizedMemoryEvent eventB = add("event-b", account, FIRST);
        NormalizedMemoryEvent eventC = add("event-0-later", KEY, SECOND);
        MemoryFactVersion versionA = version(
            "opaque-a", FIRST, Arrays.asList(first, first, second));
        MemoryFactVersion versionB = version(
            "opaque-b", FIRST, Collections.singletonList(first));
        MemoryFactVersion versionC = version(
            "opaque-c", SECOND, Collections.singletonList(second));
        VersionRelation supersedes = new VersionRelation(
            VersionRelationType.SUPERSEDES, "opaque-c", "opaque-a");
        VersionRelation duplicate = new VersionRelation(
            VersionRelationType.DUPLICATE_OF, "opaque-c", "opaque-a");
        Map<FactKey, List<MemoryFactVersion>> shuffled = new LinkedHashMap<>();
        shuffled.put(KEY, Arrays.asList(
            versionC,
            version("opaque-a", FIRST, Arrays.asList(second, first, first))));
        shuffled.put(account, Collections.singletonList(versionB));
        Map<String, String> generating = new LinkedHashMap<>();
        generating.put("opaque-c", "event-0-later");
        generating.put("opaque-b", "event-b");
        generating.put("opaque-a", "event-a");
        CanonicalSnapshot actual = new CanonicalSnapshot(
            new TemporalState(shuffled, Arrays.asList(duplicate, supersedes)),
            Arrays.asList(eventC, eventB, eventA),
            generating);

        Map<FactKey, List<MemoryFactVersion>> ordered = new LinkedHashMap<>();
        ordered.put(account, Collections.singletonList(versionB));
        ordered.put(KEY, Arrays.asList(versionA, versionC));
        CanonicalSnapshot expected = new CanonicalSnapshot(
            new TemporalState(ordered, Arrays.asList(supersedes, duplicate)),
            Arrays.asList(eventA, eventB, eventC),
            generating);

        Assertions.assertEquals(expected, actual);
        Assertions.assertEquals(expected.hashCode(), actual.hashCode());
        Assertions.assertEquals(
            Arrays.asList(eventA, eventB, eventC), actual.getEvents());
        Assertions.assertEquals(
            Arrays.asList(account, KEY),
            new ArrayList<>(actual.getState().getVersionsByFactKey().keySet()));
        Assertions.assertEquals(
            Arrays.asList(versionB, versionA, versionC),
            actual.getState().getVersions());
        Assertions.assertEquals(
            Arrays.asList("opaque-a", "opaque-b", "opaque-c"),
            new ArrayList<>(actual.getGeneratingEventIds().keySet()));
        Assertions.assertEquals(
            Arrays.asList(supersedes, duplicate), actual.getState().getRelations());
        Assertions.assertEquals(
            Arrays.asList(second, first, first),
            shuffled.get(KEY).get(1).getEvidence());
    }

    @Test
    public void testSnapshotDefensivelyCopiesInputsAndIsDeeplyImmutable() {
        NormalizedMemoryEvent event = add("event-a", KEY, FIRST);
        MemoryFactVersion version = version(
            "opaque-a", FIRST, event.getEvidence());
        List<NormalizedMemoryEvent> events = new ArrayList<>();
        events.add(event);
        Map<String, String> generating = new LinkedHashMap<>();
        generating.put(version.getId(), event.getEventId());
        CanonicalSnapshot snapshot = new CanonicalSnapshot(
            state(KEY, Collections.singletonList(version)), events, generating);

        events.clear();
        generating.clear();

        Assertions.assertEquals(Collections.singletonList(event), snapshot.getEvents());
        Assertions.assertEquals(
            Collections.singletonMap("opaque-a", "event-a"),
            snapshot.getGeneratingEventIds());
        Assertions.assertThrows(
            UnsupportedOperationException.class, () -> snapshot.getEvents().clear());
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () -> snapshot.getGeneratingEventIds().clear());
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () -> snapshot.getState().getVersionsByFactKey().clear());
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () -> snapshot.getState().getVersionsByFactKey().get(KEY).clear());
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () -> snapshot.getState().getVersions().clear());
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () -> snapshot.getState().getRelations().clear());
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () -> snapshot.getState().getVersions().get(0).getEvidence().clear());
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () -> snapshot.getEvents().get(0).getEvidence().clear());
    }

    @Test
    public void testPreservesTombstoneAndEventsWithoutMaterializedOutput() {
        FactKey relationshipKey = new FactKey("person:alice", "lives_in", "profile");
        MemoryFact relationship = MemoryFact.relationship(
            "fact-city", new MemoryEntity("person:alice", "person"),
            "lives_in", new MemoryEntity("city:beijing", "city"));
        Evidence proof = evidence("evidence-original");
        NormalizedMemoryEvent add = normalizer.normalize(
            MemoryEvent.add("event-a", relationship, VALID, FIRST,
                Arrays.asList(proof, proof)),
            relationshipKey);
        NormalizedMemoryEvent retract = normalizer.normalize(
            MemoryEvent.retract("event-b", "fact-city", VALID, FIRST,
                Collections.singletonList(evidence("evidence-retract"))),
            relationshipKey);
        MemoryFactVersion tombstone = new MemoryFactVersion(
            "opaque-tombstone", relationship, MemoryFactVersionStatus.RETRACTED,
            VALID, TimeInterval.unboundedFrom(FIRST), retract.getEvidence());
        CanonicalSnapshot snapshot = new CanonicalSnapshot(
            state(relationshipKey, Collections.singletonList(tombstone)),
            Arrays.asList(retract, add),
            Collections.singletonMap("opaque-tombstone", "event-b"));

        Assertions.assertEquals(Arrays.asList(add, retract), snapshot.getEvents());
        Assertions.assertEquals(
            Arrays.asList(proof, proof), snapshot.getEvents().get(0).getEvidence());
        Assertions.assertEquals(
            add.getPayloadHash(), snapshot.getEvents().get(0).getPayloadHash());
        Assertions.assertEquals(
            Collections.singletonList(tombstone), snapshot.getState().getVersions());
        Assertions.assertEquals(
            "city:beijing",
            snapshot.getState().getVersions().get(0).getFact().getTarget().get().getId());
        Assertions.assertEquals(1, snapshot.getGeneratingEventIds().size());
        Assertions.assertEquals(
            "event-b", snapshot.getGeneratingEventIds().get("opaque-tombstone"));
    }

    @Test
    public void testEmptyPartitionsAreOmittedAndNullInputsAreRejected() {
        TemporalState empty = state(KEY, Collections.emptyList());
        CanonicalSnapshot snapshot = new CanonicalSnapshot(
            empty, Collections.emptyList(), Collections.emptyMap());

        Assertions.assertTrue(snapshot.getState().getVersionsByFactKey().isEmpty());
        Assertions.assertTrue(snapshot.getState().getRelations().isEmpty());
        Assertions.assertTrue(snapshot.getEvents().isEmpty());
        Assertions.assertTrue(snapshot.getGeneratingEventIds().isEmpty());
        Assertions.assertThrows(NullPointerException.class,
            () -> new CanonicalSnapshot(null, Collections.emptyList(), Collections.emptyMap()));
        Assertions.assertThrows(NullPointerException.class,
            () -> new CanonicalSnapshot(empty, null, Collections.emptyMap()));
        Assertions.assertThrows(NullPointerException.class,
            () -> new CanonicalSnapshot(empty, Collections.emptyList(), null));
        Assertions.assertThrows(NullPointerException.class,
            () -> new CanonicalSnapshot(
                empty, Collections.singletonList(null), Collections.emptyMap()));
    }

    @Test
    public void testRepeatedEventIsNoopButReusedEventIdIsRejected() {
        NormalizedMemoryEvent original = add("event-a", KEY, FIRST);
        NormalizedMemoryEvent reused = add("event-a", KEY, SECOND);
        TemporalState empty = state(KEY, Collections.emptyList());
        CanonicalSnapshot once = new CanonicalSnapshot(
            empty, Collections.singletonList(original), Collections.emptyMap());
        CanonicalSnapshot repeated = new CanonicalSnapshot(
            empty, Arrays.asList(original, original), Collections.emptyMap());

        Assertions.assertEquals(once, repeated);
        Assertions.assertEquals(Collections.singletonList(original), repeated.getEvents());
        Assertions.assertThrows(IllegalArgumentException.class,
            () -> new CanonicalSnapshot(
                empty, Arrays.asList(original, reused), Collections.emptyMap()));
    }

    @Test
    public void testRejectsDuplicateVersionIdsAndMismatchedFactKey() {
        NormalizedMemoryEvent event = add("event-a", KEY, FIRST);
        MemoryFactVersion version = version("opaque-a", FIRST, event.getEvidence());
        List<NormalizedMemoryEvent> events = Collections.singletonList(event);
        Map<String, String> generating = Collections.singletonMap("opaque-a", "event-a");

        Assertions.assertThrows(IllegalArgumentException.class,
            () -> new CanonicalSnapshot(
                state(KEY, Arrays.asList(version, version)), events, generating));

        Map<FactKey, List<MemoryFactVersion>> duplicateAcrossKeys = new LinkedHashMap<>();
        duplicateAcrossKeys.put(KEY, Collections.singletonList(version));
        duplicateAcrossKeys.put(key("account"), Collections.singletonList(version));
        IllegalArgumentException duplicateId = Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> new CanonicalSnapshot(
                new TemporalState(duplicateAcrossKeys, Collections.emptyList()),
                events, generating));
        Assertions.assertEquals("Duplicate version id: opaque-a", duplicateId.getMessage());
        Assertions.assertThrows(IllegalArgumentException.class,
            () -> new CanonicalSnapshot(
                state(new FactKey("person:bob", "city", "profile"),
                    Collections.singletonList(version)),
                events, generating));
        Assertions.assertThrows(IllegalArgumentException.class,
            () -> new CanonicalSnapshot(
                state(new FactKey("person:alice", "name", "profile"),
                    Collections.singletonList(version)),
                events, generating));
    }

    @Test
    public void testRejectsMissingUnknownOrCrossScopeGenerationReferences() {
        NormalizedMemoryEvent event = add("event-a", KEY, FIRST);
        MemoryFactVersion version = version("opaque-a", FIRST, event.getEvidence());
        TemporalState state = state(KEY, Collections.singletonList(version));
        List<NormalizedMemoryEvent> events = Collections.singletonList(event);

        Assertions.assertThrows(IllegalArgumentException.class,
            () -> new CanonicalSnapshot(state, events, Collections.emptyMap()));
        Assertions.assertThrows(IllegalArgumentException.class,
            () -> new CanonicalSnapshot(state, events,
                Collections.singletonMap("opaque-a", "event-missing")));
        Map<String, String> extra = new LinkedHashMap<>();
        extra.put("opaque-a", "event-a");
        extra.put("opaque-missing", "event-a");
        Assertions.assertThrows(IllegalArgumentException.class,
            () -> new CanonicalSnapshot(state, events, extra));
        NormalizedMemoryEvent otherScope = add("event-account", key("account"), FIRST);
        Assertions.assertThrows(IllegalArgumentException.class,
            () -> new CanonicalSnapshot(state, Arrays.asList(event, otherScope),
                Collections.singletonMap("opaque-a", "event-account")));
        Assertions.assertThrows(NullPointerException.class,
            () -> new CanonicalSnapshot(state, events,
                Collections.singletonMap("opaque-a", null)));
    }

    @Test
    public void testRejectsEitherDanglingRelationEndpoint() {
        NormalizedMemoryEvent event = add("event-a", KEY, FIRST);
        MemoryFactVersion version = version("opaque-a", FIRST, event.getEvidence());
        Map<FactKey, List<MemoryFactVersion>> versions = Collections.singletonMap(
            KEY, Collections.singletonList(version));
        List<NormalizedMemoryEvent> events = Collections.singletonList(event);
        Map<String, String> generating = Collections.singletonMap("opaque-a", "event-a");
        VersionRelation missingTarget = new VersionRelation(
            VersionRelationType.SUPERSEDES, "opaque-a", "opaque-missing");
        VersionRelation missingSource = new VersionRelation(
            VersionRelationType.SUPERSEDES, "opaque-missing", "opaque-a");

        Assertions.assertThrows(IllegalArgumentException.class,
            () -> new CanonicalSnapshot(
                new TemporalState(versions, Collections.singletonList(missingTarget)),
                events, generating));
        Assertions.assertThrows(IllegalArgumentException.class,
            () -> new CanonicalSnapshot(
                new TemporalState(versions, Collections.singletonList(missingSource)),
                events, generating));
    }

    private NormalizedMemoryEvent add(String eventId, FactKey key, Instant recordedAt) {
        return normalizer.normalize(
            MemoryEvent.add(eventId, fact(), VALID, recordedAt,
                Collections.singletonList(evidence("evidence-" + eventId))),
            key);
    }

    private static MemoryFactVersion version(
        String id, Instant recordedAt, List<Evidence> evidence) {
        return new MemoryFactVersion(
            id, fact(), VALID, TimeInterval.unboundedFrom(recordedAt), evidence);
    }

    private static MemoryFact fact() {
        return MemoryFact.attribute(
            "fact-city", new MemoryEntity("person:alice", "person"), "city", "Beijing");
    }

    private static FactKey key(String scope) {
        return new FactKey("person:alice", "city", scope);
    }

    private static Evidence evidence(String id) {
        return new Evidence(id, new Source("source-registry", "registry"), "Proof " + id);
    }

    private static TemporalState state(FactKey key, List<MemoryFactVersion> versions) {
        return new TemporalState(Collections.singletonMap(key, versions), Collections.emptyList());
    }
}
