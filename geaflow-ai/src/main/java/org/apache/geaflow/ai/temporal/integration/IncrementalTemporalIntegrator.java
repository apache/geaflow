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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import org.apache.geaflow.ai.temporal.model.Evidence;
import org.apache.geaflow.ai.temporal.model.FactKey;
import org.apache.geaflow.ai.temporal.model.FactValue;
import org.apache.geaflow.ai.temporal.model.MemoryEvent;
import org.apache.geaflow.ai.temporal.model.MemoryEventOperation;
import org.apache.geaflow.ai.temporal.model.MemoryFact;
import org.apache.geaflow.ai.temporal.model.MemoryFactVersion;
import org.apache.geaflow.ai.temporal.model.MemoryFactVersionStatus;
import org.apache.geaflow.ai.temporal.model.TimeInterval;
import org.apache.geaflow.ai.temporal.model.VersionRelation;
import org.apache.geaflow.ai.temporal.model.VersionRelationType;
import org.apache.geaflow.ai.temporal.semantics.EventLedger;
import org.apache.geaflow.ai.temporal.semantics.EventLedgerDecision;
import org.apache.geaflow.ai.temporal.semantics.NormalizedMemoryEvent;
import org.apache.geaflow.ai.temporal.semantics.TemporalState;

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

    private static final Comparator<Evidence> EVIDENCE_ORDER =
        Comparator.comparing(Evidence::getId)
            .thenComparing(evidence -> evidence.getSource().getId())
            .thenComparing(evidence -> evidence.getSource().getName())
            .thenComparing(Evidence::getContent);

    private final Map<String, MemoryEvent> eventsById = new HashMap<>();
    private final Map<String, List<MemoryEvent>> eventsByFactId =
        new HashMap<>();
    private final Map<String, List<MemoryFactVersion>> versionsByFactId =
        new HashMap<>();

    private final EventLedger normalizedEventLedger = new EventLedger();
    private final Map<FactKey, List<NormalizedMemoryEvent>>
        normalizedEventsByFactKey = new HashMap<>();
    private final Map<FactKey, List<MemoryFactVersion>>
        normalizedVersionsByFactKey = new HashMap<>();
    private final Map<FactKey, List<VersionRelation>>
        normalizedRelationsByFactKey = new HashMap<>();

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
            throw new IllegalArgumentException(
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
            applyNormalizedOrderedEvent(
                event,
                updatedVersions,
                updatedRelations);
        } else {
            updatedVersions = new ArrayList<>();
            updatedRelations = new ArrayList<>();
            for (NormalizedMemoryEvent orderedEvent : updatedEvents) {
                applyNormalizedOrderedEvent(
                    orderedEvent,
                    updatedVersions,
                    updatedRelations);
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
        List<VersionRelation> relations) {
        MemoryEventOperation operation = event.getOperation();
        if (operation == MemoryEventOperation.ADD) {
            applyNormalizedAdd(event, versions, relations);
        } else if (operation == MemoryEventOperation.CORRECT
            || operation == MemoryEventOperation.RETRACT) {
            applyNormalizedChange(event, versions, relations);
        } else {
            throw new UnsupportedOperationException(
                "Unsupported memory event operation: "
                    + operation);
        }
    }

    private static void applyNormalizedAdd(
        NormalizedMemoryEvent event,
        List<MemoryFactVersion> versions,
        List<VersionRelation> relations) {
        List<MemoryFactVersion> duplicates = new ArrayList<>();
        for (MemoryFactVersion version : versions) {
            if (isCurrentActive(version)
                && version.getValidTime().overlaps(
                    event.getValidTime())
                && hasSameValue(event, version)) {
                duplicates.add(version);
            }
        }
        Collections.sort(duplicates, VALID_TIME_ORDER);

        TimeInterval validTime = event.getValidTime();
        List<Evidence> evidence = new ArrayList<>();
        mergeEvidence(evidence, event.getEvidence());
        Map<String, Boolean> materializedDuplicates = new HashMap<>();
        for (MemoryFactVersion duplicate : duplicates) {
            versions.remove(duplicate);
            boolean materialized = materializeClosedVersion(
                duplicate,
                event.getRecordedAt(),
                versions,
                relations);
            materializedDuplicates.put(duplicate.getId(), materialized);
            validTime = span(validTime, duplicate.getValidTime());
            mergeEvidence(evidence, duplicate.getEvidence());
        }

        MemoryFactVersion added = new MemoryFactVersion(
            event.getEventId() + ":version:0",
            event.getEvent().getFact().get(),
            MemoryFactVersionStatus.ACTIVE,
            validTime,
            TimeInterval.unboundedFrom(event.getRecordedAt()),
            evidence);
        versions.add(added);

        for (MemoryFactVersion duplicate : duplicates) {
            if (Boolean.TRUE.equals(
                materializedDuplicates.get(duplicate.getId()))) {
                addRelation(
                    relations,
                    new VersionRelation(
                        VersionRelationType.DUPLICATE_OF,
                        added.getId(),
                        duplicate.getId()));
            }
        }

        for (MemoryFactVersion version : versions) {
            if (version != added
                && isCurrentActive(version)
                && version.getValidTime().overlaps(validTime)
                && !hasSameValue(event, version)) {
                addRelation(
                    relations,
                    new VersionRelation(
                        VersionRelationType.CONFLICTS_WITH,
                        added.getId(),
                        version.getId()));
            }
        }
    }

    private static void applyNormalizedChange(
        NormalizedMemoryEvent event,
        List<MemoryFactVersion> versions,
        List<VersionRelation> relations) {
        List<MemoryFactVersion> affected = new ArrayList<>();
        for (MemoryFactVersion version : versions) {
            if (isCurrentActive(version)
                && version.getValidTime().overlaps(
                    event.getValidTime())) {
                affected.add(version);
            }
        }
        Collections.sort(affected, VALID_TIME_ORDER);
        if (!isFullyCovered(event.getValidTime(), affected)) {
            throw new IllegalArgumentException(
                "Event interval is not fully covered for fact key: "
                    + event.getFactKey());
        }

        Map<String, Boolean> materializedSources = new HashMap<>();
        for (MemoryFactVersion version : affected) {
            versions.remove(version);
            materializedSources.put(
                version.getId(),
                materializeClosedVersion(
                    version,
                    event.getRecordedAt(),
                    versions,
                    relations));
        }

        int versionIndex;
        if (event.getOperation() == MemoryEventOperation.CORRECT) {
            MemoryFactVersion corrected = new MemoryFactVersion(
                event.getEventId() + ":version:0",
                event.getEvent().getFact().get(),
                MemoryFactVersionStatus.ACTIVE,
                event.getValidTime(),
                TimeInterval.unboundedFrom(event.getRecordedAt()),
                event.getEvidence());
            versions.add(corrected);
            for (MemoryFactVersion source : affected) {
                addSupersedesIfMaterialized(
                    corrected,
                    source,
                    materializedSources,
                    relations);
            }
            versionIndex = 1;
        } else {
            versionIndex = addTombstones(
                event,
                affected,
                materializedSources,
                versions,
                relations);
        }

        for (MemoryFactVersion source : affected) {
            for (TimeInterval remaining : source.getValidTime()
                .subtract(event.getValidTime())) {
                MemoryFactVersion residue = new MemoryFactVersion(
                    event.getEventId() + ":version:"
                        + versionIndex++,
                    source.getFact(),
                    MemoryFactVersionStatus.ACTIVE,
                    remaining,
                    TimeInterval.unboundedFrom(
                        event.getRecordedAt()),
                    source.getEvidence());
                versions.add(residue);
                addSupersedesIfMaterialized(
                    residue,
                    source,
                    materializedSources,
                    relations);
            }
        }

        addCurrentConflicts(versions, relations);
    }

    private static void addCurrentConflicts(
        List<MemoryFactVersion> versions,
        List<VersionRelation> relations) {
        for (int leftIndex = 0;
            leftIndex < versions.size(); leftIndex++) {
            MemoryFactVersion left = versions.get(leftIndex);
            if (!isCurrentActive(left)) {
                continue;
            }
            for (int rightIndex = leftIndex + 1;
                rightIndex < versions.size(); rightIndex++) {
                MemoryFactVersion right = versions.get(rightIndex);
                if (isCurrentActive(right)
                    && left.getValidTime().overlaps(
                        right.getValidTime())
                    && !factValue(left.getFact()).equals(
                        factValue(right.getFact()))) {
                    addRelation(
                        relations,
                        new VersionRelation(
                            VersionRelationType.CONFLICTS_WITH,
                            left.getId(),
                            right.getId()));
                }
            }
        }
    }

    private static int addTombstones(
        NormalizedMemoryEvent event,
        List<MemoryFactVersion> affected,
        Map<String, Boolean> materializedSources,
        List<MemoryFactVersion> versions,
        List<VersionRelation> relations) {
        int versionIndex = 0;
        for (MemoryFactVersion source : affected) {
            TimeInterval overlap = source.getValidTime()
                .intersection(event.getValidTime()).get();
            MemoryFactVersion tombstone = new MemoryFactVersion(
                event.getEventId() + ":version:" + versionIndex++,
                source.getFact(),
                MemoryFactVersionStatus.RETRACTED,
                overlap,
                TimeInterval.unboundedFrom(event.getRecordedAt()),
                event.getEvidence());
            versions.add(tombstone);
            addSupersedesIfMaterialized(
                tombstone,
                source,
                materializedSources,
                relations);
        }
        return versionIndex;
    }

    private static boolean materializeClosedVersion(
        MemoryFactVersion version,
        Instant recordedAt,
        List<MemoryFactVersion> versions,
        List<VersionRelation> relations) {
        if (!version.getTransactionTime().getStart()
            .isBefore(recordedAt)) {
            removeRelationsFor(version.getId(), relations);
            return false;
        }

        versions.add(new MemoryFactVersion(
            version.getId(),
            version.getFact(),
            version.getStatus(),
            version.getValidTime(),
            new TimeInterval(
                version.getTransactionTime().getStart(),
                recordedAt),
            version.getEvidence()));
        return true;
    }

    private static void addSupersedesIfMaterialized(
        MemoryFactVersion replacement,
        MemoryFactVersion source,
        Map<String, Boolean> materializedSources,
        List<VersionRelation> relations) {
        if (Boolean.TRUE.equals(
            materializedSources.get(source.getId()))) {
            addRelation(
                relations,
                new VersionRelation(
                    VersionRelationType.SUPERSEDES,
                    replacement.getId(),
                    source.getId()));
        }
    }

    private static void addRelation(
        List<VersionRelation> relations,
        VersionRelation relation) {
        if (!relations.contains(relation)) {
            relations.add(relation);
        }
    }

    private static void removeRelationsFor(
        String versionId,
        List<VersionRelation> relations) {
        for (int index = relations.size() - 1; index >= 0; index--) {
            VersionRelation relation = relations.get(index);
            if (relation.getFromVersionId().equals(versionId)
                || relation.getToVersionId().equals(versionId)) {
                relations.remove(index);
            }
        }
    }

    private static boolean hasSameValue(
        NormalizedMemoryEvent event,
        MemoryFactVersion version) {
        Optional<FactValue> eventValue = event.getFactValue();
        return eventValue.isPresent()
            && eventValue.get().equals(factValue(version.getFact()));
    }

    private static FactValue factValue(MemoryFact fact) {
        if (fact.isRelationship()) {
            return FactValue.entityReference(
                fact.getTarget().get().getId());
        }
        return FactValue.literal(fact.getLiteralValue().get());
    }

    private static TimeInterval span(
        TimeInterval left,
        TimeInterval right) {
        Instant start = left.getStart().isBefore(right.getStart())
            ? left.getStart() : right.getStart();
        Instant end;
        if (!left.getEnd().isPresent()
            || !right.getEnd().isPresent()) {
            end = null;
        } else {
            Instant leftEnd = left.getEnd().get();
            Instant rightEnd = right.getEnd().get();
            end = leftEnd.isAfter(rightEnd) ? leftEnd : rightEnd;
        }
        return new TimeInterval(start, end);
    }

    private static void mergeEvidence(
        List<Evidence> target,
        List<Evidence> additions) {
        for (Evidence evidence : additions) {
            if (!target.contains(evidence)) {
                target.add(evidence);
            }
        }
        Collections.sort(target, EVIDENCE_ORDER);
    }

    private static boolean isCurrentActive(
        MemoryFactVersion version) {
        return version.getStatus() == MemoryFactVersionStatus.ACTIVE
            && isCurrent(version);
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
