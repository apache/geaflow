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

package org.apache.geaflow.ai.temporal.oracle;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import org.apache.geaflow.ai.temporal.adapter.MemoryGraphAdapter;
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
import org.apache.geaflow.ai.temporal.semantics.CanonicalSnapshot;
import org.apache.geaflow.ai.temporal.semantics.EventLedger;
import org.apache.geaflow.ai.temporal.semantics.EventLedgerDecision;
import org.apache.geaflow.ai.temporal.semantics.NormalizedMemoryEvent;
import org.apache.geaflow.ai.temporal.semantics.TemporalErrorCode;
import org.apache.geaflow.ai.temporal.semantics.TemporalState;
import org.apache.geaflow.ai.temporal.semantics.TemporalValidationException;
import org.apache.geaflow.ai.temporal.semantics.VersionIdentity;

/**
 * Recomputes temporal memory versions from a complete event collection.
 */
public final class FullReplayOracle implements ReplayMethod {

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

    private static final Comparator<Evidence> EVIDENCE_ORDER =
        Comparator.comparing(Evidence::getId)
            .thenComparing(evidence -> evidence.getSource().getId())
            .thenComparing(evidence -> evidence.getSource().getName())
            .thenComparing(Evidence::getContent);

    private static final Comparator<MemoryFactVersion> VALID_TIME_ORDER =
        Comparator.comparing(
            (MemoryFactVersion version) ->
                version.getValidTime().getStart())
            .thenComparing(MemoryFactVersion::getId);

    public List<MemoryFactVersion> replay(
        List<MemoryEvent> events) {
        Objects.requireNonNull(events, "events");

        Map<String, MemoryEvent> uniqueEvents = new HashMap<>();
        for (MemoryEvent event : events) {
            Objects.requireNonNull(event, "event");

            MemoryEvent existing = uniqueEvents.get(event.getId());
            if (existing == null) {
                uniqueEvents.put(event.getId(), event);
            } else if (!existing.equals(event)) {
                throw new IllegalArgumentException(
                    "Conflicting event id: " + event.getId());
            }
        }

        List<MemoryEvent> orderedEvents =
            new ArrayList<>(uniqueEvents.values());
        Collections.sort(orderedEvents, EVENT_ORDER);

        List<MemoryFactVersion> versions = new ArrayList<>();
        for (MemoryEvent event : orderedEvents) {
            MemoryEventOperation operation = event.getOperation();
            if (operation == MemoryEventOperation.ADD) {
                replayAdd(event, versions);
            } else if (operation == MemoryEventOperation.CORRECT
                || operation == MemoryEventOperation.RETRACT) {
                replayChange(event, versions);
            } else {
                throw new UnsupportedOperationException(
                    "Unsupported memory event operation: "
                        + operation);
            }
        }

        Collections.sort(versions, VERSION_ORDER);
        return Collections.unmodifiableList(versions);
    }

    /**
     * Recomputes canonical temporal state from normalized events.
     */
    public TemporalState replayNormalized(
        List<NormalizedMemoryEvent> events) {
        return replayNormalized(events, new HashMap<>());
    }

    /** Returns the formal oracle result after writing and reading MemoryGraph. */
    @Override
    public CanonicalSnapshot replayToSnapshot(
        List<NormalizedMemoryEvent> events) {
        Map<String, String> generatingEventIds = new HashMap<>();
        TemporalState state = replayNormalized(events, generatingEventIds);
        CanonicalSnapshot snapshot = new CanonicalSnapshot(
            state, events, generatingEventIds);
        MemoryGraphAdapter adapter = new MemoryGraphAdapter();
        return adapter.fromGraph(adapter.toGraph(snapshot));
    }

    private TemporalState replayNormalized(
        List<NormalizedMemoryEvent> events,
        Map<String, String> generatingEventIds) {
        Objects.requireNonNull(events, "events");

        List<NormalizedMemoryEvent> orderedEvents =
            new ArrayList<>(events.size());
        for (NormalizedMemoryEvent event : events) {
            orderedEvents.add(Objects.requireNonNull(event, "event"));
        }
        Collections.sort(orderedEvents, NORMALIZED_EVENT_ORDER);

        EventLedger validationLedger = new EventLedger();
        for (NormalizedMemoryEvent event : orderedEvents) {
            validationLedger.commit(event);
        }
        EventLedger ledger = new EventLedger();
        Map<FactKey, List<MemoryFactVersion>> versionsByFactKey =
            new HashMap<>();
        Set<VersionRelation> relations = new TreeSet<>();

        for (NormalizedMemoryEvent event : orderedEvents) {
            EventLedgerDecision decision = ledger.check(event);
            if (decision == EventLedgerDecision.DUPLICATE_NOOP) {
                continue;
            }
            if (decision
                == EventLedgerDecision.REJECT_EVENT_ID_REUSE) {
                throw new TemporalValidationException(
                    TemporalErrorCode.REJECT_EVENT_ID_REUSE,
                    "Event id reused with a different payload: "
                        + event.getEventId());
            }

            List<MemoryFactVersion> nextVersions =
                new ArrayList<>(versionsByFactKey.getOrDefault(
                    event.getFactKey(),
                    Collections.emptyList()));
            Set<VersionRelation> nextRelations =
                new TreeSet<>(relations);
            replayNormalizedEvent(
                event,
                nextVersions,
                nextRelations,
                generatingEventIds);

            ledger.commit(event);
            versionsByFactKey.put(
                event.getFactKey(),
                nextVersions);
            relations = nextRelations;
        }

        for (List<MemoryFactVersion> versions
            : versionsByFactKey.values()) {
            Collections.sort(versions, VERSION_ORDER);
        }
        return new TemporalState(
            versionsByFactKey,
            new ArrayList<>(relations));
    }

    private static void replayNormalizedEvent(
        NormalizedMemoryEvent event,
        List<MemoryFactVersion> versions,
        Set<VersionRelation> relations,
        Map<String, String> generatingEventIds) {
        if (event.getOperation() == MemoryEventOperation.ADD) {
            replayNormalizedAdd(event, versions, relations, generatingEventIds);
            return;
        }
        if (event.getOperation() == MemoryEventOperation.CORRECT
            || event.getOperation() == MemoryEventOperation.RETRACT) {
            replayNormalizedChange(event, versions, relations, generatingEventIds);
            return;
        }
        throw new UnsupportedOperationException(
            "Unsupported memory event operation: "
                + event.getOperation());
    }

    private static void replayNormalizedAdd(
        NormalizedMemoryEvent event,
        List<MemoryFactVersion> versions,
        Set<VersionRelation> relations,
        Map<String, String> generatingEventIds) {
        FactValue newValue = event.getFactValue().get();
        TimeInterval mergedValidTime = event.getValidTime();
        List<MemoryFactVersion> duplicates = new ArrayList<>();

        boolean expanded;
        do {
            expanded = false;
            for (MemoryFactVersion version : versions) {
                if (isCurrentActive(version)
                    && !duplicates.contains(version)
                    && factValue(version.getFact()).equals(newValue)
                    && version.getValidTime().overlaps(
                        mergedValidTime)) {
                    duplicates.add(version);
                    mergedValidTime = span(
                        mergedValidTime,
                        version.getValidTime());
                    expanded = true;
                }
            }
        } while (expanded);
        Collections.sort(duplicates, VALID_TIME_ORDER);

        List<Evidence> evidence =
            new ArrayList<>(event.getEvidence());
        versions.removeAll(duplicates);
        String newVersionId = VersionIdentity.create(
            event,
            event.getEvent().getFact().get(),
            MemoryFactVersionStatus.ACTIVE,
            mergedValidTime,
            null);
        for (MemoryFactVersion duplicate : duplicates) {
            evidence.addAll(duplicate.getEvidence());
            if (closeVersion(
                duplicate,
                event.getRecordedAt(),
                versions,
                relations,
                generatingEventIds)) {
                relations.add(new VersionRelation(
                    VersionRelationType.DUPLICATE_OF,
                    newVersionId,
                    duplicate.getId()));
            }
        }

        MemoryFactVersion added = new MemoryFactVersion(
            newVersionId,
            event.getEvent().getFact().get(),
            MemoryFactVersionStatus.ACTIVE,
            mergedValidTime,
            TimeInterval.unboundedFrom(event.getRecordedAt()),
            sortedUniqueEvidence(evidence));
        for (MemoryFactVersion version : versions) {
            if (isCurrentActive(version)
                && version.getValidTime().overlaps(
                    mergedValidTime)
                && !factValue(version.getFact()).equals(newValue)) {
                relations.add(new VersionRelation(
                    VersionRelationType.CONFLICTS_WITH,
                    newVersionId,
                    version.getId()));
            }
        }
        versions.add(added);
        generatingEventIds.put(added.getId(), event.getEventId());
    }

    private static void replayNormalizedChange(
        NormalizedMemoryEvent event,
        List<MemoryFactVersion> versions,
        Set<VersionRelation> relations,
        Map<String, String> generatingEventIds) {
        String targetId = event.getTargetVersionId().orElseThrow(
            () -> new TemporalValidationException(
                TemporalErrorCode.REJECT_TARGET_REQUIRED,
                "Change event requires a target version"));
        MemoryFactVersion target = null;
        for (MemoryFactVersion version : versions) {
            if (version.getId().equals(targetId)) {
                target = version;
                break;
            }
        }
        if (target == null) {
            throw new TemporalValidationException(
                TemporalErrorCode.REJECT_TARGET_NOT_FOUND,
                "Target version not found for fact key: " + targetId);
        }
        if (!isCurrentActive(target)) {
            throw new TemporalValidationException(
                TemporalErrorCode.REJECT_TARGET_INACTIVE,
                "Target version is not current and active: " + targetId);
        }
        if (!target.getValidTime().overlaps(event.getValidTime())) {
            throw new TemporalValidationException(
                TemporalErrorCode.REJECT_TARGET_NO_OVERLAP,
                "Target version does not overlap the change interval: "
                    + targetId);
        }

        TimeInterval changedTime = target.getValidTime()
            .intersection(event.getValidTime()).get();
        versions.remove(target);
        boolean sourceMaterialized = closeVersion(
            target, event.getRecordedAt(), versions, relations, generatingEventIds);
        boolean correction =
            event.getOperation() == MemoryEventOperation.CORRECT;
        MemoryFact changedFact = correction
            ? event.getEvent().getFact().get() : target.getFact();
        MemoryFactVersionStatus changedStatus = correction
            ? MemoryFactVersionStatus.ACTIVE
            : MemoryFactVersionStatus.RETRACTED;
        String changedId = VersionIdentity.create(
            event, changedFact, changedStatus, changedTime, targetId);
        versions.add(new MemoryFactVersion(
            changedId,
            changedFact,
            changedStatus,
            changedTime,
            TimeInterval.unboundedFrom(event.getRecordedAt()),
            event.getEvidence()));
        generatingEventIds.put(changedId, event.getEventId());
        addSupersedes(changedId, targetId, sourceMaterialized, relations);

        for (TimeInterval remaining
            : target.getValidTime().subtract(changedTime)) {
            String fragmentId = VersionIdentity.create(
                event,
                target.getFact(),
                MemoryFactVersionStatus.ACTIVE,
                remaining,
                targetId);
            versions.add(new MemoryFactVersion(
                fragmentId,
                target.getFact(),
                MemoryFactVersionStatus.ACTIVE,
                remaining,
                TimeInterval.unboundedFrom(event.getRecordedAt()),
                target.getEvidence()));
            generatingEventIds.put(fragmentId, event.getEventId());
            addSupersedes(
                fragmentId, targetId, sourceMaterialized, relations);
        }

        addCurrentConflicts(versions, relations);
    }

    private static void addCurrentConflicts(
        List<MemoryFactVersion> versions,
        Set<VersionRelation> relations) {
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
                    relations.add(new VersionRelation(
                        VersionRelationType.CONFLICTS_WITH,
                        left.getId(),
                        right.getId()));
                }
            }
        }
    }

    private static void addSupersedes(
        String replacementId,
        String sourceId,
        boolean sourceMaterialized,
        Set<VersionRelation> relations) {
        if (sourceMaterialized) {
            relations.add(new VersionRelation(
                VersionRelationType.SUPERSEDES,
                replacementId,
                sourceId));
        }
    }

    private static boolean closeVersion(
        MemoryFactVersion version,
        Instant recordedAt,
        List<MemoryFactVersion> versions,
        Set<VersionRelation> relations,
        Map<String, String> generatingEventIds) {
        Instant startedAt =
            version.getTransactionTime().getStart();
        if (startedAt.isAfter(recordedAt)) {
            throw new IllegalArgumentException(
                "Event recorded time precedes current version");
        }
        if (startedAt.equals(recordedAt)) {
            relations.removeIf(relation ->
                references(relation, version.getId()));
            generatingEventIds.remove(version.getId());
            return false;
        }
        versions.add(new MemoryFactVersion(
            version.getId(),
            version.getFact(),
            version.getStatus(),
            version.getValidTime(),
            new TimeInterval(startedAt, recordedAt),
            version.getEvidence()));
        return true;
    }

    private static boolean references(
        VersionRelation relation,
        String versionId) {
        return relation.getFromVersionId().equals(versionId)
            || relation.getToVersionId().equals(versionId);
    }

    private static FactValue factValue(MemoryFact fact) {
        if (fact.isRelationship()) {
            return FactValue.entityReference(
                fact.getTarget().get().getId());
        }
        return FactValue.literal(
            fact.getLiteralValue().get());
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

    private static List<Evidence> sortedUniqueEvidence(
        List<Evidence> evidence) {
        Collections.sort(evidence, EVIDENCE_ORDER);
        List<Evidence> unique = new ArrayList<>();
        for (Evidence item : evidence) {
            if (unique.isEmpty()
                || !unique.get(unique.size() - 1).equals(item)) {
                unique.add(item);
            }
        }
        return unique;
    }

    private static boolean isCurrentActive(
        MemoryFactVersion version) {
        return isCurrent(version)
            && version.getStatus()
                == MemoryFactVersionStatus.ACTIVE;
    }

    private static void replayAdd(
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

    private static void replayChange(
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
