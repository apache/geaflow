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

import java.util.Objects;
import java.util.Optional;
import org.apache.geaflow.ai.temporal.model.FactKey;

/**
 * The first difference found between two canonical snapshots.
 */
public final class DiffReport {

    private final boolean equivalent;
    private final String fieldPath;
    private final String expectedValue;
    private final String actualValue;
    private final FactKey factKey;
    private final String eventId;

    private DiffReport(boolean equivalent, String fieldPath, String expectedValue,
                       String actualValue, FactKey factKey, String eventId) {
        this.equivalent = equivalent;
        this.fieldPath = fieldPath;
        this.expectedValue = expectedValue;
        this.actualValue = actualValue;
        this.factKey = factKey;
        this.eventId = eventId;
    }

    public static DiffReport equivalent() {
        return new DiffReport(true, null, null, null, null, null);
    }

    public static DiffReport difference(String fieldPath, String expectedValue,
                                        String actualValue, FactKey factKey,
                                        String eventId) {
        Objects.requireNonNull(fieldPath, "fieldPath");
        if (fieldPath.trim().isEmpty()) {
            throw new IllegalArgumentException("Difference field path must not be blank");
        }
        return new DiffReport(false, fieldPath, expectedValue, actualValue, factKey, eventId);
    }

    public boolean isEquivalent() {
        return equivalent;
    }

    public Optional<String> getFieldPath() {
        return Optional.ofNullable(fieldPath);
    }

    public Optional<String> getExpectedValue() {
        return Optional.ofNullable(expectedValue);
    }

    public Optional<String> getActualValue() {
        return Optional.ofNullable(actualValue);
    }

    public Optional<FactKey> getFactKey() {
        return Optional.ofNullable(factKey);
    }

    public Optional<String> getEventId() {
        return Optional.ofNullable(eventId);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof DiffReport)) {
            return false;
        }
        DiffReport that = (DiffReport) object;
        return equivalent == that.equivalent
            && Objects.equals(fieldPath, that.fieldPath)
            && Objects.equals(expectedValue, that.expectedValue)
            && Objects.equals(actualValue, that.actualValue)
            && Objects.equals(factKey, that.factKey)
            && Objects.equals(eventId, that.eventId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(equivalent, fieldPath, expectedValue, actualValue, factKey, eventId);
    }
}
