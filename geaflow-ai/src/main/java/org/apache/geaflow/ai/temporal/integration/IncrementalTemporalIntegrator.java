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
import org.apache.geaflow.ai.temporal.model.MemoryEvent;
import org.apache.geaflow.ai.temporal.model.MemoryEventOperation;
import org.apache.geaflow.ai.temporal.model.MemoryFactVersion;
import org.apache.geaflow.ai.temporal.model.TimeInterval;

/**
 * Incrementally integrates temporal memory events by fact id.
 */
public final class IncrementalTemporalIntegrator {

    private static final Comparator<MemoryEvent> EVENT_ORDER =
        Comparator.comparing(MemoryEvent::getTransactionTime)
            .thenComparing(MemoryEvent::getId);

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

    public List<MemoryFactVersion> snapshot() {
        List<MemoryFactVersion> snapshot = new ArrayList<>();
        for (List<MemoryFactVersion> versions :
            versionsByFactId.values()) {
            snapshot.addAll(versions);
        }

        Collections.sort(snapshot, VERSION_ORDER);
        return Collections.unmodifiableList(snapshot);
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
