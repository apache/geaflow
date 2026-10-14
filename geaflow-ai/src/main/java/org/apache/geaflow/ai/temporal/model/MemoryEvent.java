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

package org.apache.geaflow.ai.temporal.model;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * An immutable input event for temporal memory replay.
 */
public final class MemoryEvent {

    private final String id;
    private final MemoryEventOperation operation;
    private final String factId;
    private final MemoryFact fact;
    private final TimeInterval validTime;
    private final Instant transactionTime;
    private final List<Evidence> evidence;

    private MemoryEvent(
        String id,
        MemoryEventOperation operation,
        String factId,
        MemoryFact fact,
        TimeInterval validTime,
        Instant transactionTime,
        List<Evidence> evidence) {
        this.id = requireText(id, "id");
        this.operation =
            Objects.requireNonNull(operation, "operation");
        this.factId = requireText(factId, "fact id");
        this.fact = fact;
        this.validTime =
            Objects.requireNonNull(validTime, "validTime");
        this.transactionTime =
            Objects.requireNonNull(transactionTime, "transactionTime");
        this.evidence = copyEvidence(evidence);
    }

    public static MemoryEvent add(
        String id,
        MemoryFact fact,
        TimeInterval validTime,
        Instant transactionTime,
        List<Evidence> evidence) {
        Objects.requireNonNull(fact, "fact");
        return new MemoryEvent(
            id,
            MemoryEventOperation.ADD,
            fact.getId(),
            fact,
            validTime,
            transactionTime,
            evidence);
    }

    public static MemoryEvent correct(
        String id,
        MemoryFact fact,
        TimeInterval validTime,
        Instant transactionTime,
        List<Evidence> evidence) {
        Objects.requireNonNull(fact, "fact");
        return new MemoryEvent(
            id,
            MemoryEventOperation.CORRECT,
            fact.getId(),
            fact,
            validTime,
            transactionTime,
            evidence);
    }

    public static MemoryEvent retract(
        String id,
        String factId,
        TimeInterval validTime,
        Instant transactionTime,
        List<Evidence> evidence) {
        return new MemoryEvent(
            id,
            MemoryEventOperation.RETRACT,
            factId,
            null,
            validTime,
            transactionTime,
            evidence);
    }

    public String getId() {
        return id;
    }

    public MemoryEventOperation getOperation() {
        return operation;
    }

    public String getFactId() {
        return factId;
    }

    public Optional<MemoryFact> getFact() {
        return Optional.ofNullable(fact);
    }

    public TimeInterval getValidTime() {
        return validTime;
    }

    public Instant getTransactionTime() {
        return transactionTime;
    }

    public List<Evidence> getEvidence() {
        return evidence;
    }

    private static List<Evidence> copyEvidence(
        List<Evidence> evidence) {
        List<Evidence> copy =
            new ArrayList<>(Objects.requireNonNull(evidence, "evidence"));
        if (copy.isEmpty()) {
            throw new IllegalArgumentException(
                "Memory event evidence must not be empty");
        }
        for (Evidence item : copy) {
            Objects.requireNonNull(item, "evidence item");
        }
        return Collections.unmodifiableList(copy);
    }

    private static String requireText(
        String value,
        String fieldName) {
        Objects.requireNonNull(value, fieldName);
        if (value.trim().isEmpty()) {
            throw new IllegalArgumentException(
                "Memory event " + fieldName + " must not be blank");
        }
        return value;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof MemoryEvent)) {
            return false;
        }
        MemoryEvent that = (MemoryEvent) object;
        return id.equals(that.id)
            && operation == that.operation
            && factId.equals(that.factId)
            && Objects.equals(fact, that.fact)
            && validTime.equals(that.validTime)
            && transactionTime.equals(that.transactionTime)
            && evidence.equals(that.evidence);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            id,
            operation,
            factId,
            fact,
            validTime,
            transactionTime,
            evidence);
    }
}
