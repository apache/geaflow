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
 * An immutable directed relation between two materialized versions.
 */
public final class VersionRelation
    implements Comparable<VersionRelation> {

    private final VersionRelationType type;
    private final String fromVersionId;
    private final String toVersionId;

    public VersionRelation(
        VersionRelationType type,
        String fromVersionId,
        String toVersionId) {
        this.type = Objects.requireNonNull(type, "type");
        String from = requireText(
            fromVersionId,
            "fromVersionId");
        String to = requireText(toVersionId, "toVersionId");
        if (from.equals(to)) {
            throw new IllegalArgumentException(
                "Version relation must not be a self relation");
        }
        if (type == VersionRelationType.CONFLICTS_WITH
            && from.compareTo(to) > 0) {
            this.fromVersionId = to;
            this.toVersionId = from;
        } else {
            this.fromVersionId = from;
            this.toVersionId = to;
        }
    }

    public VersionRelationType getType() {
        return type;
    }

    public String getFromVersionId() {
        return fromVersionId;
    }

    public String getToVersionId() {
        return toVersionId;
    }

    @Override
    public int compareTo(VersionRelation that) {
        int comparison = type.compareTo(that.type);
        if (comparison != 0) {
            return comparison;
        }
        comparison = fromVersionId.compareTo(that.fromVersionId);
        if (comparison != 0) {
            return comparison;
        }
        return toVersionId.compareTo(that.toVersionId);
    }

    private static String requireText(
        String value,
        String fieldName) {
        Objects.requireNonNull(value, fieldName);
        if (value.trim().isEmpty()) {
            throw new IllegalArgumentException(
                "Version relation " + fieldName
                    + " must not be blank");
        }
        return value;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof VersionRelation)) {
            return false;
        }
        VersionRelation that = (VersionRelation) object;
        return type == that.type
            && fromVersionId.equals(that.fromVersionId)
            && toVersionId.equals(that.toVersionId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, fromVersionId, toVersionId);
    }
}
