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
import org.apache.geaflow.ai.temporal.model.MemoryEventOperation;
import org.apache.geaflow.ai.temporal.model.MemoryFactVersion;
import org.apache.geaflow.ai.temporal.model.MemoryFactVersionStatus;
import org.apache.geaflow.ai.temporal.model.TimeInterval;
import org.apache.geaflow.ai.temporal.oracle.ReplayMethod;
import org.apache.geaflow.ai.temporal.semantics.CanonicalSnapshot;
import org.apache.geaflow.ai.temporal.semantics.EventLedger;
import org.apache.geaflow.ai.temporal.semantics.EventLedgerDecision;
import org.apache.geaflow.ai.temporal.semantics.NormalizedMemoryEvent;
import org.apache.geaflow.ai.temporal.semantics.TemporalState;

/**
 * Replays events using deterministic last-write-wins current-state semantics.
 */
public final class LwwBaseline implements ReplayMethod {

    private static final Comparator<NormalizedMemoryEvent> EVENT_ORDER =
        Comparator.comparing(NormalizedMemoryEvent::getRecordedAt)
            .thenComparing(NormalizedMemoryEvent::getEventId);

    private static final TimeInterval ALL_VALID_TIME =
        TimeInterval.unboundedFrom(
            Instant.ofEpochMilli(Long.MIN_VALUE));

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
        Map<FactKey, MemoryFactVersion> current = new TreeMap<>();
        Map<String, String> generatingEventIds = new TreeMap<>();
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

            apply(event, current, generatingEventIds);
            ledger.commit(event);
            accepted.add(event);
        }

        Map<FactKey, List<MemoryFactVersion>> versions =
            new TreeMap<>();
        for (Map.Entry<FactKey, MemoryFactVersion> entry
            : current.entrySet()) {
            versions.put(
                entry.getKey(),
                Collections.singletonList(entry.getValue()));
        }
        return new CanonicalSnapshot(
            new TemporalState(versions, Collections.emptyList()),
            accepted,
            generatingEventIds);
    }

    private static void apply(
        NormalizedMemoryEvent event,
        Map<FactKey, MemoryFactVersion> current,
        Map<String, String> generatingEventIds) {
        FactKey key = event.getFactKey();
        MemoryEventOperation operation = event.getOperation();
        if (operation == MemoryEventOperation.RETRACT) {
            MemoryFactVersion removed = current.remove(key);
            if (removed == null) {
                throw new IllegalArgumentException(
                    "Cannot retract a fact without current state: "
                        + event.getFactId());
            }
            generatingEventIds.remove(removed.getId());
            return;
        }
        if (operation == MemoryEventOperation.CORRECT
            && !current.containsKey(key)) {
            throw new IllegalArgumentException(
                "Cannot correct a fact without current state: "
                    + event.getFactId());
        }
        if (operation != MemoryEventOperation.ADD
            && operation != MemoryEventOperation.CORRECT) {
            throw new UnsupportedOperationException(
                "Unsupported memory event operation: " + operation);
        }

        MemoryFactVersion version = new MemoryFactVersion(
            event.getEventId() + ":version:0",
            event.getEvent().getFact().get(),
            MemoryFactVersionStatus.ACTIVE,
            ALL_VALID_TIME,
            TimeInterval.unboundedFrom(event.getRecordedAt()),
            event.getEvidence());
        MemoryFactVersion replaced = current.put(key, version);
        if (replaced != null) {
            generatingEventIds.remove(replaced.getId());
        }
        generatingEventIds.put(version.getId(), event.getEventId());
    }
}
