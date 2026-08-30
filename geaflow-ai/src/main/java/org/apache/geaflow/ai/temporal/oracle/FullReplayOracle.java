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
 * Recomputes temporal memory versions from a complete event collection.
 */
public final class FullReplayOracle {

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
