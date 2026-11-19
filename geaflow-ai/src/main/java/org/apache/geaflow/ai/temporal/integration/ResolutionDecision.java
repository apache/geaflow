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

package org.apache.geaflow.ai.temporal.integration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.geaflow.ai.temporal.model.MemoryFactVersion;
import org.apache.geaflow.ai.temporal.model.TimeInterval;

/** The rule inputs and selected target used by one normalized transition. */
public final class ResolutionDecision {

    private final TimeInterval effectiveValidTime;
    private final List<String> mandatoryVersionIds;
    private final List<String> readVersionIds;
    private final List<String> rules;
    private final List<MemoryFactVersion> duplicateVersions;
    private final List<MemoryFactVersion> conflictVersions;
    private final MemoryFactVersion targetVersion;

    ResolutionDecision(TimeInterval effectiveValidTime, List<MemoryFactVersion> mandatoryVersions,
                       List<MemoryFactVersion> readVersions, List<String> rules,
                       List<MemoryFactVersion> duplicateVersions,
                       List<MemoryFactVersion> conflictVersions, MemoryFactVersion targetVersion) {
        this.effectiveValidTime = Objects.requireNonNull(effectiveValidTime, "effectiveValidTime");
        this.mandatoryVersionIds = ids(mandatoryVersions);
        this.readVersionIds = ids(readVersions);
        this.rules = Collections.unmodifiableList(new ArrayList<>(rules));
        this.duplicateVersions = Collections.unmodifiableList(new ArrayList<>(duplicateVersions));
        this.conflictVersions = Collections.unmodifiableList(new ArrayList<>(conflictVersions));
        this.targetVersion = targetVersion;
    }

    public TimeInterval getEffectiveValidTime() {
        return effectiveValidTime;
    }

    public List<String> getMandatoryVersionIds() {
        return mandatoryVersionIds;
    }

    public List<String> getReadVersionIds() {
        return readVersionIds;
    }

    public List<String> getRules() {
        return rules;
    }

    public String getTargetVersionId() {
        return targetVersion == null ? null : targetVersion.getId();
    }

    List<MemoryFactVersion> getDuplicateVersions() {
        return duplicateVersions;
    }

    List<MemoryFactVersion> getConflictVersions() {
        return conflictVersions;
    }

    MemoryFactVersion getTargetVersion() {
        return targetVersion;
    }

    private static List<String> ids(List<MemoryFactVersion> versions) {
        List<String> result = new ArrayList<>();
        for (MemoryFactVersion version : versions) {
            result.add(version.getId());
        }
        Collections.sort(result);
        return Collections.unmodifiableList(result);
    }
}
