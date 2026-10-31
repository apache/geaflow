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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * An immutable bitemporal version of a memory fact.
 */
public final class MemoryFactVersion {

    private final String id;
    private final MemoryFact fact;
    private final MemoryFactVersionStatus status;
    private final TimeInterval validTime;
    private final TimeInterval transactionTime;
    private final List<Evidence> evidence;

    public MemoryFactVersion(
        String id,
        MemoryFact fact,
        TimeInterval validTime,
        TimeInterval transactionTime,
        List<Evidence> evidence) {
        this(
            id,
            fact,
            MemoryFactVersionStatus.ACTIVE,
            validTime,
            transactionTime,
            evidence);
    }

    public MemoryFactVersion(
        String id,
        MemoryFact fact,
        MemoryFactVersionStatus status,
        TimeInterval validTime,
        TimeInterval transactionTime,
        List<Evidence> evidence) {
        this.id = requireText(id);
        this.fact = Objects.requireNonNull(fact, "fact");
        this.status = Objects.requireNonNull(status, "status");
        this.validTime = Objects.requireNonNull(validTime, "validTime");
        this.transactionTime =
            Objects.requireNonNull(transactionTime, "transactionTime");

        List<Evidence> evidenceCopy =
            new ArrayList<>(Objects.requireNonNull(evidence, "evidence"));
        if (evidenceCopy.isEmpty()) {
            throw new IllegalArgumentException(
                "Memory fact version evidence must not be empty");
        }
        for (Evidence item : evidenceCopy) {
            Objects.requireNonNull(item, "evidence item");
        }
        this.evidence = Collections.unmodifiableList(evidenceCopy);
    }

    public String getId() {
        return id;
    }

    public MemoryFact getFact() {
        return fact;
    }

    public MemoryFactVersionStatus getStatus() {
        return status;
    }

    public TimeInterval getValidTime() {
        return validTime;
    }

    public TimeInterval getTransactionTime() {
        return transactionTime;
    }

    public List<Evidence> getEvidence() {
        return evidence;
    }

    private static String requireText(String value) {
        Objects.requireNonNull(value, "id");
        if (value.trim().isEmpty()) {
            throw new IllegalArgumentException(
                "Memory fact version id must not be blank");
        }
        return value;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof MemoryFactVersion)) {
            return false;
        }
        MemoryFactVersion that = (MemoryFactVersion) object;
        return id.equals(that.id)
            && fact.equals(that.fact)
            && status == that.status
            && validTime.equals(that.validTime)
            && transactionTime.equals(that.transactionTime)
            && evidence.equals(that.evidence);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            id,
            fact,
            status,
            validTime,
            transactionTime,
            evidence);
    }
}
