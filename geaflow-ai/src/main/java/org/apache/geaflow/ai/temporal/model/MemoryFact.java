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

import java.util.Objects;
import java.util.Optional;

/**
 * An immutable structured assertion about a memory entity.
 */
public final class MemoryFact {

    private final String id;
    private final MemoryEntity subject;
    private final String predicate;
    private final String literalValue;
    private final MemoryEntity target;

    private MemoryFact(
        String id,
        MemoryEntity subject,
        String predicate,
        String literalValue,
        MemoryEntity target) {
        this.id = requireText(id, "id");
        this.subject = Objects.requireNonNull(subject, "subject");
        this.predicate = requireText(predicate, "predicate");
        this.literalValue = literalValue;
        this.target = target;
    }

    public static MemoryFact attribute(
        String id,
        MemoryEntity subject,
        String predicate,
        String literalValue) {
        return new MemoryFact(
            id,
            subject,
            predicate,
            requireText(literalValue, "literal value"),
            null);
    }

    public static MemoryFact relationship(
        String id,
        MemoryEntity subject,
        String predicate,
        MemoryEntity target) {
        return new MemoryFact(
            id,
            subject,
            predicate,
            null,
            Objects.requireNonNull(target, "target"));
    }

    public String getId() {
        return id;
    }

    public MemoryEntity getSubject() {
        return subject;
    }

    public String getPredicate() {
        return predicate;
    }

    public Optional<String> getLiteralValue() {
        return Optional.ofNullable(literalValue);
    }

    public Optional<MemoryEntity> getTarget() {
        return Optional.ofNullable(target);
    }

    public boolean isRelationship() {
        return target != null;
    }

    private static String requireText(String value, String fieldName) {
        Objects.requireNonNull(value, fieldName);
        if (value.trim().isEmpty()) {
            throw new IllegalArgumentException(
                "Memory fact " + fieldName + " must not be blank");
        }
        return value;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof MemoryFact)) {
            return false;
        }
        MemoryFact that = (MemoryFact) object;
        return id.equals(that.id)
            && subject.equals(that.subject)
            && predicate.equals(that.predicate)
            && Objects.equals(literalValue, that.literalValue)
            && Objects.equals(target, that.target);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            id,
            subject,
            predicate,
            literalValue,
            target);
    }
}
