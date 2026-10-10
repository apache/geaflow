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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Tracks the committed payload hash for each normalized event id.
 */
public final class EventLedger {

    private final Map<String, String> payloadHashes = new HashMap<>();

    public EventLedgerDecision check(NormalizedMemoryEvent event) {
        Objects.requireNonNull(event, "event");
        String existing = payloadHashes.get(event.getEventId());
        if (existing == null) {
            return EventLedgerDecision.ACCEPTED;
        }
        if (existing.equals(event.getPayloadHash())) {
            return EventLedgerDecision.DUPLICATE_NOOP;
        }
        return EventLedgerDecision.REJECT_EVENT_ID_REUSE;
    }

    public EventLedgerDecision commit(NormalizedMemoryEvent event) {
        EventLedgerDecision decision = check(event);
        if (decision == EventLedgerDecision.REJECT_EVENT_ID_REUSE) {
            throw new IllegalArgumentException(
                "Event id reused with a different payload: "
                    + event.getEventId());
        }
        if (decision == EventLedgerDecision.ACCEPTED) {
            payloadHashes.put(
                event.getEventId(),
                event.getPayloadHash());
        }
        return decision;
    }

    public Optional<String> getPayloadHash(String eventId) {
        return Optional.ofNullable(payloadHashes.get(
            Objects.requireNonNull(eventId, "eventId")));
    }
}
