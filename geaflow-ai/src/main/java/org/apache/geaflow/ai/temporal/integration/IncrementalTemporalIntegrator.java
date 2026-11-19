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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import org.apache.geaflow.ai.temporal.model.FactKey;
import org.apache.geaflow.ai.temporal.model.MemoryEvent;
import org.apache.geaflow.ai.temporal.model.MemoryEventOperation;
import org.apache.geaflow.ai.temporal.model.MemoryFactVersion;
import org.apache.geaflow.ai.temporal.model.TimeInterval;
import org.apache.geaflow.ai.temporal.model.VersionRelation;
import org.apache.geaflow.ai.temporal.semantics.CanonicalSnapshot;
import org.apache.geaflow.ai.temporal.semantics.EventLedger;
import org.apache.geaflow.ai.temporal.semantics.EventLedgerDecision;
import org.apache.geaflow.ai.temporal.semantics.NormalizedMemoryEvent;
import org.apache.geaflow.ai.temporal.semantics.TemporalErrorCode;
import org.apache.geaflow.ai.temporal.semantics.TemporalState;
import org.apache.geaflow.ai.temporal.semantics.TemporalValidationException;

/**
 * Incrementally integrates legacy events by fact id and normalized events by
 * fact key.
 */
public final class IncrementalTemporalIntegrator {

    private static final Comparator<MemoryEvent> EVENT_ORDER =
        Comparator.comparing(MemoryEvent::getTransactionTime)
            .thenComparing(MemoryEvent::getId);

    private static final Comparator<NormalizedMemoryEvent>
        NORMALIZED_EVENT_ORDER =
        Comparator.comparing(NormalizedMemoryEvent::getRecordedAt)
            .thenComparing(NormalizedMemoryEvent::getEventId);

    private static final Comparator<MemoryFactVersion> VERSION_ORDER =
        Comparator.comparing(
            (MemoryFactVersion version) ->
                version.getTransactionTime().getStart())
            .thenComparing(
                version -> version.getValidTime().getStart())
            .thenComparing(MemoryFactVersion::getId);

    private static final Comparator<MemoryFactVersion> VALID_TIME_ORDER =
        Comparator.comparing(
            (MemoryFactVersion version) ->
                version.getValidTime().getStart())
            .thenComparing(MemoryFactVersion::getId);

    private final Map<String, MemoryEvent> eventsById = new HashMap<>();
    private final Map<String, List<MemoryEvent>> eventsByFactId =
        new HashMap<>();
    private final Map<String, List<MemoryFactVersion>> versionsByFactId =
        new HashMap<>();

    private EventLedger normalizedEventLedger = new EventLedger();
    private Map<FactKey, List<NormalizedMemoryEvent>>
        normalizedEventsByFactKey = new HashMap<>();
    private Map<FactKey, List<MemoryFactVersion>>
        normalizedVersionsByFactKey = new HashMap<>();
    private Map<FactKey, List<VersionRelation>>
        normalizedRelationsByFactKey = new HashMap<>();
    private Map<FactKey, Map<String, String>>
        normalizedGeneratingEventIdsByFactKey = new HashMap<>();

    public void apply(MemoryEvent event) {
        Objects.requireNonNull(event, "event");

        MemoryEvent existing = eventsById.get(event.getId());
        if (existing != null) {
            if (!existing.equals(event)) {
                throw new IllegalArgumentException(
                    "Conflicting event id: " + event.getId());
            }
            return;
        }

        String factId = event.getFactId();
        List<MemoryEvent> existingEvents = eventsByFactId.get(factId);
        List<MemoryEvent> updatedEvents = existingEvents == null
            ? new ArrayList<>() : new ArrayList<>(existingEvents);
        boolean appended = existingEvents == null
            || EVENT_ORDER.compare(
                existingEvents.get(existingEvents.size() - 1),
                event) < 0;

        updatedEvents.add(event);
        Collections.sort(updatedEvents, EVENT_ORDER);

        List<MemoryFactVersion> updatedVersions;
        if (appended) {
            List<MemoryFactVersion> existingVersions =
                versionsByFactId.get(factId);
            updatedVersions = existingVersions == null
                ? new ArrayList<>()
                : new ArrayList<>(existingVersions);
            applyOrderedEvent(event, updatedVersions);
        } else {
            updatedVersions = replayFact(updatedEvents);
        }

        Collections.sort(updatedVersions, VERSION_ORDER);
        eventsById.put(event.getId(), event);
        eventsByFactId.put(
            factId,
            Collections.unmodifiableList(updatedEvents));
        versionsByFactId.put(
            factId,
            Collections.unmodifiableList(updatedVersions));
    }

    /**
     * Applies an already normalized event to the state for its fact key.
     */
    public void apply(NormalizedMemoryEvent event) {
        Objects.requireNonNull(event, "event");

        EventLedgerDecision decision = normalizedEventLedger.check(event);
        if (decision == EventLedgerDecision.DUPLICATE_NOOP) {
            return;
        }
        if (decision == EventLedgerDecision.REJECT_EVENT_ID_REUSE) {
            throw new TemporalValidationException(
                TemporalErrorCode.REJECT_EVENT_ID_REUSE,
                "Event id reused with a different payload: "
                    + event.getEventId());
        }

        FactKey factKey = event.getFactKey();
        List<NormalizedMemoryEvent> existingEvents =
            normalizedEventsByFactKey.get(factKey);
        List<NormalizedMemoryEvent> updatedEvents =
            existingEvents == null
                ? new ArrayList<>()
                : new ArrayList<>(existingEvents);
        boolean appended = existingEvents == null
            || NORMALIZED_EVENT_ORDER.compare(
                existingEvents.get(existingEvents.size() - 1),
                event) < 0;
        updatedEvents.add(event);
        Collections.sort(updatedEvents, NORMALIZED_EVENT_ORDER);

        List<MemoryFactVersion> updatedVersions;
        List<VersionRelation> updatedRelations;
        Map<String, String> updatedGeneratingEventIds;
        if (appended) {
            List<MemoryFactVersion> existingVersions =
                normalizedVersionsByFactKey.get(factKey);
            List<VersionRelation> existingRelations =
                normalizedRelationsByFactKey.get(factKey);
            updatedVersions = existingVersions == null
                ? new ArrayList<>()
                : new ArrayList<>(existingVersions);
            updatedRelations = existingRelations == null
                ? new ArrayList<>()
                : new ArrayList<>(existingRelations);
            updatedGeneratingEventIds = new HashMap<>(
                normalizedGeneratingEventIdsByFactKey.getOrDefault(
                    factKey, Collections.emptyMap()));
            applyNormalizedOrderedEvent(
                event,
                updatedVersions,
                updatedRelations,
                updatedGeneratingEventIds);
        } else {
            updatedVersions = new ArrayList<>();
            updatedRelations = new ArrayList<>();
            updatedGeneratingEventIds = new HashMap<>();
            for (NormalizedMemoryEvent orderedEvent : updatedEvents) {
                applyNormalizedOrderedEvent(
                    orderedEvent,
                    updatedVersions,
                    updatedRelations,
                    updatedGeneratingEventIds);
            }
        }

        Collections.sort(updatedVersions, VERSION_ORDER);
        Collections.sort(updatedRelations);
        normalizedEventLedger.commit(event);
        normalizedEventsByFactKey.put(
            factKey,
            Collections.unmodifiableList(updatedEvents));
        normalizedVersionsByFactKey.put(
            factKey,
            Collections.unmodifiableList(updatedVersions));
        normalizedRelationsByFactKey.put(
            factKey,
            Collections.unmodifiableList(updatedRelations));
        normalizedGeneratingEventIdsByFactKey.put(
            factKey, Collections.unmodifiableMap(updatedGeneratingEventIds));
    }

    /** Applies a finite batch in event order, publishing only after all events succeed. */
    public List<EventLedgerDecision> applyBatch(List<NormalizedMemoryEvent> events) {
        Objects.requireNonNull(events, "events");
        List<NormalizedMemoryEvent> ordered = new ArrayList<>(events);
        for (NormalizedMemoryEvent event : ordered) {
            Objects.requireNonNull(event, "event");
        }
        Collections.sort(ordered, NORMALIZED_EVENT_ORDER);
        IncrementalTemporalIntegrator staged = new IncrementalTemporalIntegrator();
        staged.normalizedEventLedger = new EventLedger(normalizedEventLedger);
        staged.normalizedEventsByFactKey.putAll(normalizedEventsByFactKey);
        staged.normalizedVersionsByFactKey.putAll(normalizedVersionsByFactKey);
        staged.normalizedRelationsByFactKey.putAll(normalizedRelationsByFactKey);
        staged.normalizedGeneratingEventIdsByFactKey.putAll(
            normalizedGeneratingEventIdsByFactKey);
        List<EventLedgerDecision> decisions = new ArrayList<>();
        Map<FactKey, List<NormalizedMemoryEvent>> updatedTimelines = new TreeMap<>();
        for (NormalizedMemoryEvent event : ordered) {
            EventLedgerDecision decision = staged.normalizedEventLedger.commit(event);
            decisions.add(decision);
            if (decision == EventLedgerDecision.ACCEPTED) {
                updatedTimelines.computeIfAbsent(event.getFactKey(), key ->
                    new ArrayList<>(normalizedEventsByFactKey.getOrDefault(
                        key, Collections.emptyList()))).add(event);
            }
        }
        // A batch prefix can invalidate an old target that the complete batch preserves.
        List<NormalizedMemoryEvent> replayEvents = new ArrayList<>();
        for (List<NormalizedMemoryEvent> timeline : updatedTimelines.values()) {
            Collections.sort(timeline, NORMALIZED_EVENT_ORDER);
            replayEvents.addAll(timeline);
        }
        Collections.sort(replayEvents, NORMALIZED_EVENT_ORDER);
        Map<FactKey, List<MemoryFactVersion>> replayVersions = new HashMap<>();
        Map<FactKey, List<VersionRelation>> replayRelations = new HashMap<>();
        Map<FactKey, Map<String, String>> replayGeneratingEventIds = new HashMap<>();
        for (NormalizedMemoryEvent event : replayEvents) {
            applyNormalizedOrderedEvent(event,
                replayVersions.computeIfAbsent(event.getFactKey(), key -> new ArrayList<>()),
                replayRelations.computeIfAbsent(event.getFactKey(), key -> new ArrayList<>()),
                replayGeneratingEventIds.computeIfAbsent(event.getFactKey(), key -> new HashMap<>()));
        }
        for (Map.Entry<FactKey, List<NormalizedMemoryEvent>> entry
            : updatedTimelines.entrySet()) {
            List<NormalizedMemoryEvent> timeline = entry.getValue();
            List<MemoryFactVersion> versions = replayVersions.get(entry.getKey());
            List<VersionRelation> relations = replayRelations.get(entry.getKey());
            Collections.sort(versions, VERSION_ORDER);
            Collections.sort(relations);
            staged.normalizedEventsByFactKey.put(entry.getKey(),
                Collections.unmodifiableList(timeline));
            staged.normalizedVersionsByFactKey.put(entry.getKey(),
                Collections.unmodifiableList(versions));
            staged.normalizedRelationsByFactKey.put(entry.getKey(),
                Collections.unmodifiableList(relations));
            staged.normalizedGeneratingEventIdsByFactKey.put(entry.getKey(),
                Collections.unmodifiableMap(replayGeneratingEventIds.get(entry.getKey())));
        }
        normalizedEventLedger = staged.normalizedEventLedger;
        normalizedEventsByFactKey = staged.normalizedEventsByFactKey;
        normalizedVersionsByFactKey = staged.normalizedVersionsByFactKey;
        normalizedRelationsByFactKey = staged.normalizedRelationsByFactKey;
        normalizedGeneratingEventIdsByFactKey = staged.normalizedGeneratingEventIdsByFactKey;
        return Collections.unmodifiableList(decisions);
    }

    public List<NormalizedMemoryEvent> normalizedEventSnapshot() {
        List<NormalizedMemoryEvent> events = new ArrayList<>();
        for (List<NormalizedMemoryEvent> timeline : normalizedEventsByFactKey.values()) {
            events.addAll(timeline);
        }
        Collections.sort(events, NORMALIZED_EVENT_ORDER);
        return Collections.unmodifiableList(events);
    }

    /** Returns normalized state, accepted events and explicit version provenance. */
    public CanonicalSnapshot canonicalSnapshot() {
        Map<String, String> generatingEventIds = new TreeMap<>();
        for (Map<String, String> keyGeneratingEventIds
            : normalizedGeneratingEventIdsByFactKey.values()) {
            generatingEventIds.putAll(keyGeneratingEventIds);
        }
        return new CanonicalSnapshot(
            stateSnapshot(), normalizedEventSnapshot(), generatingEventIds);
    }

    public List<MemoryFactVersion> snapshot() {
        List<MemoryFactVersion> snapshot = new ArrayList<>();
        for (List<MemoryFactVersion> versions :
            versionsByFactId.values()) {
            snapshot.addAll(versions);
        }

        Collections.sort(snapshot, VERSION_ORDER);
        return Collections.unmodifiableList(snapshot);
    }

    /**
     * Returns an immutable, deterministic snapshot of normalized state.
     */
    public TemporalState stateSnapshot() {
        Map<FactKey, List<MemoryFactVersion>> versionsByFactKey =
            new TreeMap<>();
        versionsByFactKey.putAll(normalizedVersionsByFactKey);

        List<VersionRelation> relations = new ArrayList<>();
        for (List<VersionRelation> keyRelations :
            normalizedRelationsByFactKey.values()) {
            relations.addAll(keyRelations);
        }
        Collections.sort(relations);
        return new TemporalState(versionsByFactKey, relations);
    }

    List<MemoryEvent> eventSnapshot() {
        List<MemoryEvent> snapshot =
            new ArrayList<>(eventsById.values());
        Collections.sort(snapshot, EVENT_ORDER);
        return Collections.unmodifiableList(snapshot);
    }

    private static void applyNormalizedOrderedEvent(
        NormalizedMemoryEvent event,
        List<MemoryFactVersion> versions,
        List<VersionRelation> relations,
        Map<String, String> generatingEventIds) {
        new ConflictResolver().apply(event, versions, relations, generatingEventIds);
    }

    private static List<MemoryFactVersion> replayFact(
        List<MemoryEvent> events) {
        List<MemoryFactVersion> versions = new ArrayList<>();
        for (MemoryEvent event : events) {
            applyOrderedEvent(event, versions);
        }
        return versions;
    }

    private static void applyOrderedEvent(
        MemoryEvent event,
        List<MemoryFactVersion> versions) {
        MemoryEventOperation operation = event.getOperation();
        if (operation == MemoryEventOperation.ADD) {
            applyAdd(event, versions);
        } else if (operation == MemoryEventOperation.CORRECT
            || operation == MemoryEventOperation.RETRACT) {
            applyChange(event, versions);
        } else {
            throw new UnsupportedOperationException(
                "Unsupported memory event operation: "
                    + operation);
        }
    }

    private static void applyAdd(
        MemoryEvent event,
        List<MemoryFactVersion> versions) {
        for (MemoryFactVersion version : versions) {
            if (isCurrent(version)
                && version.getFact().getId().equals(event.getFactId())
                && version.getValidTime().overlaps(
                    event.getValidTime())) {
                throw new IllegalArgumentException(
                    "Overlapping add for fact id: "
                        + event.getFactId());
            }
        }

        versions.add(new MemoryFactVersion(
            event.getId() + ":version:0",
            event.getFact().get(),
            event.getValidTime(),
            TimeInterval.unboundedFrom(
                event.getTransactionTime()),
            event.getEvidence()));
    }

    private static void applyChange(
        MemoryEvent event,
        List<MemoryFactVersion> versions) {
        List<MemoryFactVersion> affected = new ArrayList<>();
        for (MemoryFactVersion version : versions) {
            if (isCurrent(version)
                && version.getFact().getId().equals(event.getFactId())
                && version.getValidTime().overlaps(
                    event.getValidTime())) {
                affected.add(version);
            }
        }

        Collections.sort(affected, VALID_TIME_ORDER);
        if (!isFullyCovered(event.getValidTime(), affected)) {
            throw new IllegalArgumentException(
                "Event interval is not fully covered for fact id: "
                    + event.getFactId());
        }

        versions.removeAll(affected);

        int fragmentIndex = 1;
        for (MemoryFactVersion version : affected) {
            if (version.getTransactionTime().getStart()
                .isBefore(event.getTransactionTime())) {
                versions.add(new MemoryFactVersion(
                    version.getId(),
                    version.getFact(),
                    version.getValidTime(),
                    new TimeInterval(
                        version.getTransactionTime().getStart(),
                        event.getTransactionTime()),
                    version.getEvidence()));
            }

            for (TimeInterval remaining :
                version.getValidTime().subtract(
                    event.getValidTime())) {
                versions.add(new MemoryFactVersion(
                    event.getId() + ":version:"
                        + fragmentIndex++,
                    version.getFact(),
                    remaining,
                    TimeInterval.unboundedFrom(
                        event.getTransactionTime()),
                    version.getEvidence()));
            }
        }

        if (event.getOperation() == MemoryEventOperation.CORRECT) {
            versions.add(new MemoryFactVersion(
                event.getId() + ":version:0",
                event.getFact().get(),
                event.getValidTime(),
                TimeInterval.unboundedFrom(
                    event.getTransactionTime()),
                event.getEvidence()));
        }
    }

    private static boolean isFullyCovered(
        TimeInterval target,
        List<MemoryFactVersion> coveringVersions) {
        List<TimeInterval> uncovered = new ArrayList<>();
        uncovered.add(target);

        for (MemoryFactVersion version : coveringVersions) {
            List<TimeInterval> remaining = new ArrayList<>();
            for (TimeInterval interval : uncovered) {
                remaining.addAll(
                    interval.subtract(version.getValidTime()));
            }

            uncovered = remaining;
            if (uncovered.isEmpty()) {
                return true;
            }
        }

        return false;
    }

    private static boolean isCurrent(
        MemoryFactVersion version) {
        return !version.getTransactionTime()
            .getEnd().isPresent();
    }
}
