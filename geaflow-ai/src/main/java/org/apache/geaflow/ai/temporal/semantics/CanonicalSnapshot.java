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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import org.apache.geaflow.ai.temporal.model.Evidence;
import org.apache.geaflow.ai.temporal.model.FactKey;
import org.apache.geaflow.ai.temporal.model.MemoryFact;
import org.apache.geaflow.ai.temporal.model.MemoryFactVersion;
import org.apache.geaflow.ai.temporal.model.VersionRelation;

/**
 * An immutable canonical representation of temporal state and its input events.
 */
public final class CanonicalSnapshot {

    private static final Comparator<NormalizedMemoryEvent> EVENT_ORDER =
        Comparator.comparing(NormalizedMemoryEvent::getRecordedAt)
            .thenComparing(NormalizedMemoryEvent::getEventId);

    private static final Comparator<Evidence> EVIDENCE_ORDER =
        Comparator.comparing(Evidence::getId)
            .thenComparing(evidence -> evidence.getSource().getId())
            .thenComparing(evidence -> evidence.getSource().getName())
            .thenComparing(Evidence::getContent);

    private final TemporalState state;
    private final List<NormalizedMemoryEvent> events;
    private final Map<String, String> generatingEventIds;

    public CanonicalSnapshot(
        TemporalState state,
        List<NormalizedMemoryEvent> events,
        Map<String, String> generatingEventIds) {
        this.state = canonicalizeState(
            Objects.requireNonNull(state, "state"));
        this.events = canonicalizeEvents(events);
        this.generatingEventIds = canonicalizeGeneratingEventIds(
            generatingEventIds,
            this.state,
            this.events);
    }

    public TemporalState getState() {
        return state;
    }

    public List<NormalizedMemoryEvent> getEvents() {
        return events;
    }

    public Map<String, String> getGeneratingEventIds() {
        return generatingEventIds;
    }

    private static TemporalState canonicalizeState(
        TemporalState state) {
        Map<FactKey, List<MemoryFactVersion>> versionsByKey =
            new TreeMap<>();
        Set<String> versionIds = new HashSet<>();
        for (List<MemoryFactVersion> versions
            : state.getVersionsByFactKey().values()) {
            for (MemoryFactVersion version : versions) {
                if (!versionIds.add(version.getId())) {
                    throw new IllegalArgumentException(
                        "Duplicate version id: " + version.getId());
                }
            }
        }
        for (Map.Entry<FactKey, List<MemoryFactVersion>> entry
            : state.getVersionsByFactKey().entrySet()) {
            FactKey factKey = Objects.requireNonNull(
                entry.getKey(), "factKey");
            List<MemoryFactVersion> versions = Objects.requireNonNull(
                entry.getValue(), "versions");
            if (versions.isEmpty()) {
                continue;
            }
            List<MemoryFactVersion> canonicalVersions =
                new ArrayList<>();
            for (MemoryFactVersion version : versions) {
                Objects.requireNonNull(version, "version");
                validateFactKey(factKey, version.getFact());
                canonicalVersions.add(canonicalizeVersion(version));
            }
            versionsByKey.put(factKey, canonicalVersions);
        }
        for (VersionRelation relation : state.getRelations()) {
            Objects.requireNonNull(relation, "relation");
            if (!versionIds.contains(relation.getFromVersionId())
                || !versionIds.contains(relation.getToVersionId())) {
                throw new IllegalArgumentException(
                    "Version relation references an unknown version");
            }
        }
        return new TemporalState(versionsByKey, state.getRelations());
    }

    private static MemoryFactVersion canonicalizeVersion(
        MemoryFactVersion version) {
        List<Evidence> evidence = new ArrayList<>(version.getEvidence());
        for (Evidence item : evidence) {
            Objects.requireNonNull(item, "evidence");
        }
        Collections.sort(evidence, EVIDENCE_ORDER);
        return new MemoryFactVersion(
            version.getId(),
            version.getFact(),
            version.getStatus(),
            version.getValidTime(),
            version.getTransactionTime(),
            evidence);
    }

    private static void validateFactKey(
        FactKey factKey,
        MemoryFact fact) {
        if (!factKey.getSubjectId().equals(fact.getSubject().getId())
            || !factKey.getPredicate().equals(fact.getPredicate())) {
            throw new IllegalArgumentException(
                "Fact key does not match version fact");
        }
    }

    private static List<NormalizedMemoryEvent> canonicalizeEvents(
        List<NormalizedMemoryEvent> events) {
        List<NormalizedMemoryEvent> ordered = new ArrayList<>(
            Objects.requireNonNull(events, "events"));
        for (NormalizedMemoryEvent event : ordered) {
            Objects.requireNonNull(event, "event");
        }
        Collections.sort(ordered, EVENT_ORDER);

        Map<String, String> payloadHashes = new HashMap<>();
        List<NormalizedMemoryEvent> unique = new ArrayList<>();
        for (NormalizedMemoryEvent event : ordered) {
            String previousHash = payloadHashes.get(event.getEventId());
            if (previousHash == null) {
                payloadHashes.put(
                    event.getEventId(), event.getPayloadHash());
                unique.add(event);
            } else if (!previousHash.equals(event.getPayloadHash())) {
                throw new IllegalArgumentException(
                    "Event id reused with a different payload: "
                        + event.getEventId());
            }
        }
        return Collections.unmodifiableList(unique);
    }

    private static Map<String, String> canonicalizeGeneratingEventIds(
        Map<String, String> generatingEventIds,
        TemporalState state,
        List<NormalizedMemoryEvent> events) {
        Map<String, FactKey> versionKeys = new HashMap<>();
        for (Map.Entry<FactKey, List<MemoryFactVersion>> entry
            : state.getVersionsByFactKey().entrySet()) {
            for (MemoryFactVersion version : entry.getValue()) {
                versionKeys.put(version.getId(), entry.getKey());
            }
        }
        Map<String, NormalizedMemoryEvent> eventsById = new HashMap<>();
        for (NormalizedMemoryEvent event : events) {
            eventsById.put(event.getEventId(), event);
        }

        Map<String, String> ordered = new TreeMap<>();
        for (Map.Entry<String, String> entry : Objects.requireNonNull(
            generatingEventIds, "generatingEventIds").entrySet()) {
            String versionId = Objects.requireNonNull(
                entry.getKey(), "versionId");
            String eventId = Objects.requireNonNull(
                entry.getValue(), "eventId");
            ordered.put(versionId, eventId);
        }
        if (!ordered.keySet().equals(versionKeys.keySet())) {
            throw new IllegalArgumentException(
                "Generating event ids must match version ids");
        }
        for (Map.Entry<String, String> entry : ordered.entrySet()) {
            NormalizedMemoryEvent event = eventsById.get(entry.getValue());
            if (event == null) {
                throw new IllegalArgumentException(
                    "Generating event is unknown: " + entry.getValue());
            }
            if (!versionKeys.get(entry.getKey()).equals(
                event.getFactKey())) {
                throw new IllegalArgumentException(
                    "Generating event fact key does not match version");
            }
        }
        return Collections.unmodifiableMap(ordered);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof CanonicalSnapshot)) {
            return false;
        }
        CanonicalSnapshot that = (CanonicalSnapshot) object;
        return state.equals(that.state)
            && events.equals(that.events)
            && generatingEventIds.equals(that.generatingEventIds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(state, events, generatingEventIds);
    }
}
