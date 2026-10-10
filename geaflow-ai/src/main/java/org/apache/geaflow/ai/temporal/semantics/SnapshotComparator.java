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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.geaflow.ai.temporal.model.Evidence;
import org.apache.geaflow.ai.temporal.model.FactKey;
import org.apache.geaflow.ai.temporal.model.FactValue;
import org.apache.geaflow.ai.temporal.model.MemoryFact;
import org.apache.geaflow.ai.temporal.model.MemoryFactVersion;
import org.apache.geaflow.ai.temporal.model.TimeInterval;
import org.apache.geaflow.ai.temporal.model.VersionRelation;

/**
 * Finds the first stable, field-level difference between two canonical snapshots.
 */
public final class SnapshotComparator {

    public DiffReport compare(
        CanonicalSnapshot expected,
        CanonicalSnapshot actual) {
        Objects.requireNonNull(expected, "expected");
        Objects.requireNonNull(actual, "actual");

        DiffReport difference = compareEvents(expected, actual);
        if (difference != null) {
            return difference;
        }
        difference = compareVersions(expected, actual);
        if (difference != null) {
            return difference;
        }
        difference = compareGeneratingEventIds(expected, actual);
        if (difference != null) {
            return difference;
        }
        difference = compareRelations(expected, actual);
        return difference == null ? DiffReport.equivalent() : difference;
    }

    private static DiffReport compareEvents(
        CanonicalSnapshot expected,
        CanonicalSnapshot actual) {
        Map<String, NormalizedMemoryEvent> expectedEvents = eventsById(expected);
        Map<String, NormalizedMemoryEvent> actualEvents = eventsById(actual);
        Set<String> eventIds = keys(expectedEvents, actualEvents);
        for (String eventId : eventIds) {
            NormalizedMemoryEvent expectedEvent = expectedEvents.get(eventId);
            NormalizedMemoryEvent actualEvent = actualEvents.get(eventId);
            NormalizedMemoryEvent contextEvent = expectedEvent == null
                ? actualEvent : expectedEvent;
            FactKey contextKey = contextEvent.getFactKey();
            String prefix = "events[" + eventId + "]";
            DiffReport difference = comparePresence(
                prefix, expectedEvent, actualEvent, contextKey, eventId);
            if (difference != null) {
                return difference;
            }
            difference = compareFactKey(
                prefix + ".factKey",
                expectedEvent.getFactKey(),
                actualEvent.getFactKey(),
                contextKey,
                eventId);
            if (difference != null) {
                return difference;
            }
            difference = difference(
                prefix + ".operation",
                expectedEvent.getOperation().name(),
                actualEvent.getOperation().name(),
                contextKey,
                eventId);
            if (difference != null) {
                return difference;
            }
            difference = difference(
                prefix + ".factId",
                expectedEvent.getFactId(),
                actualEvent.getFactId(),
                contextKey,
                eventId);
            if (difference != null) {
                return difference;
            }
            difference = compareOptionalFact(
                prefix + ".fact",
                expectedEvent.getEvent().getFact().orElse(null),
                actualEvent.getEvent().getFact().orElse(null),
                contextKey,
                eventId);
            if (difference != null) {
                return difference;
            }
            difference = compareInterval(
                prefix + ".validTime",
                expectedEvent.getValidTime(),
                actualEvent.getValidTime(),
                contextKey,
                eventId);
            if (difference != null) {
                return difference;
            }
            difference = difference(
                prefix + ".recordedAt",
                time(expectedEvent.getRecordedAt()),
                time(actualEvent.getRecordedAt()),
                contextKey,
                eventId);
            if (difference != null) {
                return difference;
            }
            difference = compareEvidence(
                prefix + ".evidence",
                expectedEvent.getEvidence(),
                actualEvent.getEvidence(),
                contextKey,
                eventId);
            if (difference != null) {
                return difference;
            }
            difference = difference(
                prefix + ".payloadHash",
                expectedEvent.getPayloadHash(),
                actualEvent.getPayloadHash(),
                contextKey,
                eventId);
            if (difference != null) {
                return difference;
            }
        }
        return null;
    }

    private static DiffReport compareVersions(
        CanonicalSnapshot expected,
        CanonicalSnapshot actual) {
        Map<String, VersionEntry> expectedVersions = versionsById(expected);
        Map<String, VersionEntry> actualVersions = versionsById(actual);
        Set<String> versionIds = keys(expectedVersions, actualVersions);
        for (String versionId : versionIds) {
            VersionEntry expectedEntry = expectedVersions.get(versionId);
            VersionEntry actualEntry = actualVersions.get(versionId);
            VersionEntry contextEntry = expectedEntry == null
                ? actualEntry : expectedEntry;
            CanonicalSnapshot contextSnapshot = expectedEntry == null
                ? actual : expected;
            FactKey contextKey = contextEntry.factKey;
            String contextEventId = contextSnapshot.getGeneratingEventIds().get(versionId);
            String prefix = "versions[" + versionId + "]";
            DiffReport difference = comparePresence(
                prefix, expectedEntry, actualEntry, contextKey, contextEventId);
            if (difference != null) {
                return difference;
            }
            difference = compareFactKey(
                prefix + ".factKey",
                expectedEntry.factKey,
                actualEntry.factKey,
                contextKey,
                contextEventId);
            if (difference != null) {
                return difference;
            }
            MemoryFactVersion expectedVersion = expectedEntry.version;
            MemoryFactVersion actualVersion = actualEntry.version;
            difference = compareFact(
                prefix + ".fact",
                expectedVersion.getFact(),
                actualVersion.getFact(),
                contextKey,
                contextEventId);
            if (difference != null) {
                return difference;
            }
            difference = difference(
                prefix + ".status",
                expectedVersion.getStatus().name(),
                actualVersion.getStatus().name(),
                contextKey,
                contextEventId);
            if (difference != null) {
                return difference;
            }
            difference = compareInterval(
                prefix + ".validTime",
                expectedVersion.getValidTime(),
                actualVersion.getValidTime(),
                contextKey,
                contextEventId);
            if (difference != null) {
                return difference;
            }
            difference = compareInterval(
                prefix + ".transactionTime",
                expectedVersion.getTransactionTime(),
                actualVersion.getTransactionTime(),
                contextKey,
                contextEventId);
            if (difference != null) {
                return difference;
            }
            difference = compareEvidence(
                prefix + ".evidence",
                expectedVersion.getEvidence(),
                actualVersion.getEvidence(),
                contextKey,
                contextEventId);
            if (difference != null) {
                return difference;
            }
        }
        return null;
    }

    private static DiffReport compareGeneratingEventIds(
        CanonicalSnapshot expected,
        CanonicalSnapshot actual) {
        Map<String, String> expectedIds = expected.getGeneratingEventIds();
        Map<String, String> actualIds = actual.getGeneratingEventIds();
        Map<String, VersionEntry> expectedVersions = versionsById(expected);
        Map<String, VersionEntry> actualVersions = versionsById(actual);
        for (String versionId : keys(expectedIds, actualIds)) {
            String expectedEventId = expectedIds.get(versionId);
            String actualEventId = actualIds.get(versionId);
            VersionEntry contextEntry = expectedVersions.get(versionId);
            if (contextEntry == null) {
                contextEntry = actualVersions.get(versionId);
            }
            FactKey contextKey = contextEntry == null
                ? null : contextEntry.factKey;
            DiffReport difference = difference(
                "generatingEventIds[" + versionId + "]",
                expectedEventId,
                actualEventId,
                contextKey,
                expectedEventId);
            if (difference != null) {
                return difference;
            }
        }
        return null;
    }

    private static DiffReport compareRelations(
        CanonicalSnapshot expected,
        CanonicalSnapshot actual) {
        List<VersionRelation> expectedRelations = new ArrayList<>(
            expected.getState().getRelations());
        List<VersionRelation> actualRelations = new ArrayList<>(
            actual.getState().getRelations());
        Collections.sort(expectedRelations);
        Collections.sort(actualRelations);
        Map<String, VersionEntry> expectedVersions = versionsById(expected);
        Map<String, VersionEntry> actualVersions = versionsById(actual);
        int relationCount = Math.max(expectedRelations.size(), actualRelations.size());
        for (int index = 0; index < relationCount; index++) {
            VersionRelation expectedRelation = index < expectedRelations.size()
                ? expectedRelations.get(index) : null;
            VersionRelation actualRelation = index < actualRelations.size()
                ? actualRelations.get(index) : null;
            VersionRelation contextRelation = expectedRelation == null
                ? actualRelation : expectedRelation;
            CanonicalSnapshot contextSnapshot = expectedRelation == null
                ? actual : expected;
            Map<String, VersionEntry> contextVersions = expectedRelation == null
                ? actualVersions : expectedVersions;
            VersionEntry contextEntry = contextVersions.get(
                contextRelation.getFromVersionId());
            FactKey contextKey = contextEntry == null
                ? null : contextEntry.factKey;
            String contextEventId = contextSnapshot.getGeneratingEventIds().get(
                contextRelation.getFromVersionId());
            String prefix = "relations[" + index + "]";
            DiffReport difference = comparePresence(
                prefix,
                expectedRelation,
                actualRelation,
                contextKey,
                contextEventId);
            if (difference != null) {
                return difference;
            }
            difference = difference(
                prefix + ".type",
                expectedRelation.getType().name(),
                actualRelation.getType().name(),
                contextKey,
                contextEventId);
            if (difference != null) {
                return difference;
            }
            difference = difference(
                prefix + ".fromVersionId",
                expectedRelation.getFromVersionId(),
                actualRelation.getFromVersionId(),
                contextKey,
                contextEventId);
            if (difference != null) {
                return difference;
            }
            difference = difference(
                prefix + ".toVersionId",
                expectedRelation.getToVersionId(),
                actualRelation.getToVersionId(),
                contextKey,
                contextEventId);
            if (difference != null) {
                return difference;
            }
        }
        return null;
    }

    private static DiffReport compareFactKey(
        String prefix,
        FactKey expected,
        FactKey actual,
        FactKey contextKey,
        String contextEventId) {
        DiffReport difference = difference(
            prefix + ".subjectId",
            expected.getSubjectId(),
            actual.getSubjectId(),
            contextKey,
            contextEventId);
        if (difference != null) {
            return difference;
        }
        difference = difference(
            prefix + ".predicate",
            expected.getPredicate(),
            actual.getPredicate(),
            contextKey,
            contextEventId);
        if (difference != null) {
            return difference;
        }
        return difference(
            prefix + ".scope",
            expected.getScope(),
            actual.getScope(),
            contextKey,
            contextEventId);
    }

    private static DiffReport compareOptionalFact(
        String prefix,
        MemoryFact expected,
        MemoryFact actual,
        FactKey contextKey,
        String contextEventId) {
        DiffReport difference = comparePresence(
            prefix, expected, actual, contextKey, contextEventId);
        if (difference != null || expected == null) {
            return difference;
        }
        return compareFact(
            prefix, expected, actual, contextKey, contextEventId);
    }

    private static DiffReport compareFact(
        String prefix,
        MemoryFact expected,
        MemoryFact actual,
        FactKey contextKey,
        String contextEventId) {
        DiffReport difference = difference(
            prefix + ".id",
            expected.getId(),
            actual.getId(),
            contextKey,
            contextEventId);
        if (difference != null) {
            return difference;
        }
        difference = difference(
            prefix + ".subject.id",
            expected.getSubject().getId(),
            actual.getSubject().getId(),
            contextKey,
            contextEventId);
        if (difference != null) {
            return difference;
        }
        difference = difference(
            prefix + ".subject.label",
            expected.getSubject().getLabel(),
            actual.getSubject().getLabel(),
            contextKey,
            contextEventId);
        if (difference != null) {
            return difference;
        }
        difference = difference(
            prefix + ".predicate",
            expected.getPredicate(),
            actual.getPredicate(),
            contextKey,
            contextEventId);
        if (difference != null) {
            return difference;
        }
        difference = difference(
            prefix + ".kind",
            factKind(expected),
            factKind(actual),
            contextKey,
            contextEventId);
        if (difference != null) {
            return difference;
        }
        if (!expected.isRelationship()) {
            return difference(
                prefix + ".literalValue",
                expected.getLiteralValue().orElse(null),
                actual.getLiteralValue().orElse(null),
                contextKey,
                contextEventId);
        }
        difference = difference(
            prefix + ".target.id",
            expected.getTarget().get().getId(),
            actual.getTarget().get().getId(),
            contextKey,
            contextEventId);
        if (difference != null) {
            return difference;
        }
        return difference(
            prefix + ".target.label",
            expected.getTarget().get().getLabel(),
            actual.getTarget().get().getLabel(),
            contextKey,
            contextEventId);
    }

    private static DiffReport compareInterval(
        String prefix,
        TimeInterval expected,
        TimeInterval actual,
        FactKey contextKey,
        String contextEventId) {
        DiffReport difference = difference(
            prefix + ".start",
            time(expected.getStart()),
            time(actual.getStart()),
            contextKey,
            contextEventId);
        if (difference != null) {
            return difference;
        }
        return difference(
            prefix + ".end",
            end(expected),
            end(actual),
            contextKey,
            contextEventId);
    }

    private static DiffReport compareEvidence(
        String prefix,
        List<Evidence> expected,
        List<Evidence> actual,
        FactKey contextKey,
        String contextEventId) {
        int evidenceCount = Math.max(expected.size(), actual.size());
        for (int index = 0; index < evidenceCount; index++) {
            Evidence expectedEvidence = index < expected.size()
                ? expected.get(index) : null;
            Evidence actualEvidence = index < actual.size()
                ? actual.get(index) : null;
            String itemPrefix = prefix + "[" + index + "]";
            DiffReport difference = comparePresence(
                itemPrefix,
                expectedEvidence,
                actualEvidence,
                contextKey,
                contextEventId);
            if (difference != null) {
                return difference;
            }
            difference = difference(
                itemPrefix + ".id",
                expectedEvidence.getId(),
                actualEvidence.getId(),
                contextKey,
                contextEventId);
            if (difference != null) {
                return difference;
            }
            difference = difference(
                itemPrefix + ".source.id",
                expectedEvidence.getSource().getId(),
                actualEvidence.getSource().getId(),
                contextKey,
                contextEventId);
            if (difference != null) {
                return difference;
            }
            difference = difference(
                itemPrefix + ".source.name",
                expectedEvidence.getSource().getName(),
                actualEvidence.getSource().getName(),
                contextKey,
                contextEventId);
            if (difference != null) {
                return difference;
            }
            difference = difference(
                itemPrefix + ".content",
                expectedEvidence.getContent(),
                actualEvidence.getContent(),
                contextKey,
                contextEventId);
            if (difference != null) {
                return difference;
            }
        }
        return null;
    }

    private static DiffReport comparePresence(
        String path,
        Object expected,
        Object actual,
        FactKey contextKey,
        String contextEventId) {
        return difference(
            path,
            expected == null ? null : "present",
            actual == null ? null : "present",
            contextKey,
            contextEventId);
    }

    private static DiffReport difference(
        String path,
        String expected,
        String actual,
        FactKey contextKey,
        String contextEventId) {
        return Objects.equals(expected, actual)
            ? null
            : DiffReport.difference(
                path, expected, actual, contextKey, contextEventId);
    }

    private static String factKind(MemoryFact fact) {
        return fact.isRelationship()
            ? FactValue.Kind.ENTITY_REF.name()
            : FactValue.Kind.LITERAL.name();
    }

    private static String time(Instant instant) {
        return instant.toString();
    }

    private static String end(TimeInterval interval) {
        return interval.getEnd().isPresent()
            ? time(interval.getEnd().get()) : "infinity";
    }

    private static Map<String, NormalizedMemoryEvent> eventsById(
        CanonicalSnapshot snapshot) {
        Map<String, NormalizedMemoryEvent> events = new TreeMap<>();
        for (NormalizedMemoryEvent event : snapshot.getEvents()) {
            events.put(event.getEventId(), event);
        }
        return events;
    }

    private static Map<String, VersionEntry> versionsById(
        CanonicalSnapshot snapshot) {
        Map<String, VersionEntry> versions = new TreeMap<>();
        for (Map.Entry<FactKey, List<MemoryFactVersion>> entry
            : snapshot.getState().getVersionsByFactKey().entrySet()) {
            for (MemoryFactVersion version : entry.getValue()) {
                versions.put(
                    version.getId(),
                    new VersionEntry(entry.getKey(), version));
            }
        }
        return versions;
    }

    private static <T> Set<String> keys(
        Map<String, T> expected,
        Map<String, T> actual) {
        Set<String> keys = new TreeSet<>(expected.keySet());
        keys.addAll(actual.keySet());
        return keys;
    }

    private static final class VersionEntry {

        private final FactKey factKey;
        private final MemoryFactVersion version;

        private VersionEntry(
            FactKey factKey,
            MemoryFactVersion version) {
            this.factKey = factKey;
            this.version = version;
        }
    }
}
