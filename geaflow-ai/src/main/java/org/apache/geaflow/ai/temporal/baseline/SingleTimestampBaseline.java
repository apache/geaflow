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

package org.apache.geaflow.ai.temporal.baseline;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import org.apache.geaflow.ai.temporal.model.FactKey;
import org.apache.geaflow.ai.temporal.model.FactValue;
import org.apache.geaflow.ai.temporal.model.MemoryEventOperation;
import org.apache.geaflow.ai.temporal.model.MemoryFactVersion;
import org.apache.geaflow.ai.temporal.model.MemoryFactVersionStatus;
import org.apache.geaflow.ai.temporal.model.TimeInterval;
import org.apache.geaflow.ai.temporal.model.VersionRelation;
import org.apache.geaflow.ai.temporal.model.VersionRelationType;
import org.apache.geaflow.ai.temporal.oracle.ReplayMethod;
import org.apache.geaflow.ai.temporal.semantics.CanonicalSnapshot;
import org.apache.geaflow.ai.temporal.semantics.EventLedger;
import org.apache.geaflow.ai.temporal.semantics.EventLedgerDecision;
import org.apache.geaflow.ai.temporal.semantics.NormalizedMemoryEvent;
import org.apache.geaflow.ai.temporal.semantics.TemporalState;

/**
 * Keeps the current distinct values while ignoring valid-time intervals.
 */
public final class SingleTimestampBaseline implements ReplayMethod {

    private static final Comparator<NormalizedMemoryEvent> EVENT_ORDER =
        Comparator.comparing(NormalizedMemoryEvent::getRecordedAt)
            .thenComparing(NormalizedMemoryEvent::getEventId);

    private static final TimeInterval ALL_VALID_TIME =
        TimeInterval.unboundedFrom(Instant.ofEpochMilli(Long.MIN_VALUE));

    @Override
    public CanonicalSnapshot replayToSnapshot(
        List<NormalizedMemoryEvent> events) {
        List<NormalizedMemoryEvent> ordered = new ArrayList<>(
            Objects.requireNonNull(events, "events"));
        for (NormalizedMemoryEvent event : ordered) {
            Objects.requireNonNull(event, "event");
        }
        Collections.sort(ordered, EVENT_ORDER);

        EventLedger ledger = new EventLedger();
        List<NormalizedMemoryEvent> accepted = new ArrayList<>();
        Map<FactKey, Map<FactValue, NormalizedMemoryEvent>> current =
            new TreeMap<>();
        for (NormalizedMemoryEvent event : ordered) {
            EventLedgerDecision decision = ledger.check(event);
            if (decision == EventLedgerDecision.DUPLICATE_NOOP) {
                continue;
            }
            if (decision == EventLedgerDecision.REJECT_EVENT_ID_REUSE) {
                throw new IllegalArgumentException(
                    "Event id reused with a different payload: "
                        + event.getEventId());
            }

            Map<FactValue, NormalizedMemoryEvent> next = new TreeMap<>();
            Map<FactValue, NormalizedMemoryEvent> existing =
                current.get(event.getFactKey());
            if (existing != null) {
                next.putAll(existing);
            }
            apply(event, next);
            ledger.commit(event);
            if (next.isEmpty()) {
                current.remove(event.getFactKey());
            } else {
                current.put(event.getFactKey(), next);
            }
            accepted.add(event);
        }

        Map<FactKey, List<MemoryFactVersion>> versions =
            new TreeMap<>();
        Map<String, String> generatingEventIds = new TreeMap<>();
        List<VersionRelation> relations = new ArrayList<>();
        for (Map.Entry<FactKey, Map<FactValue, NormalizedMemoryEvent>> entry
            : current.entrySet()) {
            List<MemoryFactVersion> versionsForKey = new ArrayList<>();
            for (NormalizedMemoryEvent event : entry.getValue().values()) {
                MemoryFactVersion version = version(event);
                versionsForKey.add(version);
                generatingEventIds.put(
                    version.getId(), event.getEventId());
            }
            versions.put(entry.getKey(), versionsForKey);
            addConflicts(versionsForKey, relations);
        }
        return new CanonicalSnapshot(
            new TemporalState(versions, relations),
            accepted,
            generatingEventIds);
    }

    private static void apply(
        NormalizedMemoryEvent event,
        Map<FactValue, NormalizedMemoryEvent> current) {
        MemoryEventOperation operation = event.getOperation();
        if (operation == MemoryEventOperation.ADD) {
            current.put(event.getFactValue().get(), event);
            return;
        }
        if (current.isEmpty()) {
            throw new IllegalArgumentException(
                "Operation requires current values for fact key: "
                    + event.getFactKey());
        }
        current.clear();
        if (operation == MemoryEventOperation.CORRECT) {
            current.put(event.getFactValue().get(), event);
        } else if (operation != MemoryEventOperation.RETRACT) {
            throw new UnsupportedOperationException(
                "Unsupported memory event operation: " + operation);
        }
    }

    private static MemoryFactVersion version(
        NormalizedMemoryEvent event) {
        return new MemoryFactVersion(
            versionId(event),
            event.getEvent().getFact().get(),
            MemoryFactVersionStatus.ACTIVE,
            ALL_VALID_TIME,
            TimeInterval.unboundedFrom(event.getRecordedAt()),
            event.getEvidence());
    }

    private static String versionId(NormalizedMemoryEvent event) {
        return event.getEventId() + ":version:0";
    }

    private static void addConflicts(
        List<MemoryFactVersion> versions,
        List<VersionRelation> relations) {
        for (int left = 0; left < versions.size(); left++) {
            for (int right = left + 1; right < versions.size(); right++) {
                relations.add(new VersionRelation(
                    VersionRelationType.CONFLICTS_WITH,
                    versions.get(left).getId(),
                    versions.get(right).getId()));
            }
        }
    }
}
