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

/**
 * An immutable identity for a fact independent of its value.
 */
public final class FactKey implements Comparable<FactKey> {

    private final String subjectId;
    private final String predicate;
    private final String scope;

    public FactKey(
        String subjectId,
        String predicate,
        String scope) {
        this.subjectId = requireText(subjectId, "subjectId");
        this.predicate = requireText(predicate, "predicate");
        this.scope = requireText(scope, "scope");
    }

    public String getSubjectId() {
        return subjectId;
    }

    public String getPredicate() {
        return predicate;
    }

    public String getScope() {
        return scope;
    }

    @Override
    public int compareTo(FactKey that) {
        int comparison = subjectId.compareTo(that.subjectId);
        if (comparison != 0) {
            return comparison;
        }
        comparison = predicate.compareTo(that.predicate);
        if (comparison != 0) {
            return comparison;
        }
        return scope.compareTo(that.scope);
    }

    private static String requireText(
        String value,
        String fieldName) {
        Objects.requireNonNull(value, fieldName);
        if (value.trim().isEmpty()) {
            throw new IllegalArgumentException(
                "Fact key " + fieldName + " must not be blank");
        }
        return value;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof FactKey)) {
            return false;
        }
        FactKey that = (FactKey) object;
        return subjectId.equals(that.subjectId)
            && predicate.equals(that.predicate)
            && scope.equals(that.scope);
    }

    @Override
    public int hashCode() {
        return Objects.hash(subjectId, predicate, scope);
    }
}
