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
 * An immutable literal or entity-reference fact value.
 */
public final class FactValue implements Comparable<FactValue> {

    public enum Kind {
        LITERAL,
        ENTITY_REF
    }

    private final Kind kind;
    private final String value;

    private FactValue(Kind kind, String value) {
        this.kind = Objects.requireNonNull(kind, "kind");
        this.value = requireText(value);
    }

    public static FactValue literal(String value) {
        return new FactValue(Kind.LITERAL, value);
    }

    public static FactValue entityReference(String entityId) {
        return new FactValue(Kind.ENTITY_REF, entityId);
    }

    public Kind getKind() {
        return kind;
    }

    public String getValue() {
        return value;
    }

    public Optional<String> getLiteralValue() {
        return kind == Kind.LITERAL
            ? Optional.of(value) : Optional.empty();
    }

    public Optional<String> getEntityId() {
        return kind == Kind.ENTITY_REF
            ? Optional.of(value) : Optional.empty();
    }

    @Override
    public int compareTo(FactValue that) {
        int comparison = kind.compareTo(that.kind);
        return comparison != 0
            ? comparison : value.compareTo(that.value);
    }

    private static String requireText(String value) {
        Objects.requireNonNull(value, "value");
        if (value.trim().isEmpty()) {
            throw new IllegalArgumentException(
                "Fact value must not be blank");
        }
        return value;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof FactValue)) {
            return false;
        }
        FactValue that = (FactValue) object;
        return kind == that.kind && value.equals(that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, value);
    }
}
