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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import org.apache.geaflow.ai.temporal.model.FactKey;
import org.apache.geaflow.ai.temporal.model.MemoryFactVersion;
import org.apache.geaflow.ai.temporal.model.VersionRelation;

/**
 * An immutable, deterministically ordered temporal state snapshot.
 */
public final class TemporalState {

    private static final Comparator<MemoryFactVersion> VERSION_ORDER =
        Comparator.comparing(
            (MemoryFactVersion version) ->
                version.getTransactionTime().getStart())
            .thenComparing(
                version -> version.getValidTime().getStart())
            .thenComparing(MemoryFactVersion::getId);

    private final Map<FactKey, List<MemoryFactVersion>>
        versionsByFactKey;
    private final List<MemoryFactVersion> versions;
    private final List<VersionRelation> relations;

    public TemporalState(
        Map<FactKey, List<MemoryFactVersion>> versionsByFactKey,
        List<VersionRelation> relations) {
        Objects.requireNonNull(versionsByFactKey, "versionsByFactKey");

        Map<FactKey, List<MemoryFactVersion>> ordered =
            new TreeMap<>();
        for (Map.Entry<FactKey, List<MemoryFactVersion>> entry
            : versionsByFactKey.entrySet()) {
            FactKey key = Objects.requireNonNull(
                entry.getKey(),
                "factKey");
            List<MemoryFactVersion> versionsForKey =
                new ArrayList<>(Objects.requireNonNull(
                    entry.getValue(),
                    "versionsForKey"));
            for (MemoryFactVersion version : versionsForKey) {
                Objects.requireNonNull(version, "version");
            }
            Collections.sort(versionsForKey, VERSION_ORDER);
            List<MemoryFactVersion> immutableVersions =
                Collections.unmodifiableList(versionsForKey);
            ordered.put(key, immutableVersions);
        }
        List<MemoryFactVersion> flattened = new ArrayList<>();
        for (List<MemoryFactVersion> versionsForKey
            : ordered.values()) {
            flattened.addAll(versionsForKey);
        }
        this.versionsByFactKey = Collections.unmodifiableMap(ordered);
        this.versions = Collections.unmodifiableList(flattened);

        List<VersionRelation> orderedRelations =
            new ArrayList<>(Objects.requireNonNull(
                relations,
                "relations"));
        for (VersionRelation relation : orderedRelations) {
            Objects.requireNonNull(relation, "relation");
        }
        Collections.sort(orderedRelations);
        this.relations = Collections.unmodifiableList(orderedRelations);
    }

    public Map<FactKey, List<MemoryFactVersion>>
        getVersionsByFactKey() {
        return versionsByFactKey;
    }

    public List<MemoryFactVersion> getVersions() {
        return versions;
    }

    public List<VersionRelation> getRelations() {
        return relations;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof TemporalState)) {
            return false;
        }
        TemporalState that = (TemporalState) object;
        return versionsByFactKey.equals(that.versionsByFactKey)
            && relations.equals(that.relations);
    }

    @Override
    public int hashCode() {
        return Objects.hash(versionsByFactKey, relations);
    }
}
