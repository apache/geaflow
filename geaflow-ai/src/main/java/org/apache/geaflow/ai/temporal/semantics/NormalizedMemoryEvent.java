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
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.geaflow.ai.temporal.model.Evidence;
import org.apache.geaflow.ai.temporal.model.FactKey;
import org.apache.geaflow.ai.temporal.model.FactValue;
import org.apache.geaflow.ai.temporal.model.MemoryEvent;
import org.apache.geaflow.ai.temporal.model.MemoryEventOperation;
import org.apache.geaflow.ai.temporal.model.TimeInterval;

/**
 * An immutable event after canonical input normalization.
 */
public final class NormalizedMemoryEvent {

    private final MemoryEvent event;
    private final FactKey factKey;
    private final FactValue factValue;
    private final String payloadHash;

    NormalizedMemoryEvent(
        MemoryEvent event,
        FactKey factKey,
        FactValue factValue,
        String payloadHash) {
        this.event = Objects.requireNonNull(event, "event");
        this.factKey = Objects.requireNonNull(factKey, "factKey");
        this.factValue = factValue;
        this.payloadHash = Objects.requireNonNull(
            payloadHash,
            "payloadHash");
    }

    public MemoryEvent getEvent() {
        return event;
    }

    public String getEventId() {
        return event.getId();
    }

    public MemoryEventOperation getOperation() {
        return event.getOperation();
    }

    public String getFactId() {
        return event.getFactId();
    }

    public FactKey getFactKey() {
        return factKey;
    }

    public Optional<FactValue> getFactValue() {
        return Optional.ofNullable(factValue);
    }

    public TimeInterval getValidTime() {
        return event.getValidTime();
    }

    public Instant getRecordedAt() {
        return event.getTransactionTime();
    }

    public List<Evidence> getEvidence() {
        return event.getEvidence();
    }

    public String getPayloadHash() {
        return payloadHash;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof NormalizedMemoryEvent)) {
            return false;
        }
        NormalizedMemoryEvent that =
            (NormalizedMemoryEvent) object;
        return event.equals(that.event)
            && factKey.equals(that.factKey)
            && Objects.equals(factValue, that.factValue)
            && payloadHash.equals(that.payloadHash);
    }

    @Override
    public int hashCode() {
        return Objects.hash(event, factKey, factValue, payloadHash);
    }
}
